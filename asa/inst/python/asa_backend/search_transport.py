"""DuckDuckGo transport, browser tiers, Tor routing, and ranking helpers."""

# custom_duckduckgo.py
#
# LangChain‑compatible DuckDuckGo search wrapper with:
#   • optional real Chrome/Chromium via Selenium
#   • proxy + arbitrary headers on every tier
#   • automatic proxy‑bypass for Chromedriver handshake
#   • smart retries and a final pure‑Requests HTML fallback
#
import contextlib
import copy
import json
import logging
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import traceback
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, parse_qsl, quote, unquote, urlencode, urlparse
import unicodedata

from bs4 import BeautifulSoup
from ddgs import ddgs  # ddgs.DDGS is a class, ddgs.ddgs is a module
from ddgs import DDGS  # ddgs.DDGS is a class, ddgs.ddgs is a module
from ddgs.exceptions import DDGSException
from langchain_community.tools.ddg_search.tool import DuckDuckGoSearchRun
from langchain_community.utilities.duckduckgo_search import DuckDuckGoSearchAPIWrapper
from pydantic import Field
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests
import random as random
import primp

try:
    from message_utils import (
        message_content_from_message as _shared_message_content_from_message,
        message_content_to_text as _shared_message_content_to_text,
    )
except ImportError:
    _module_dir = pathlib.Path(__file__).resolve().parent
    _parent_dir = _module_dir.parent
    for _candidate in (str(_parent_dir), str(_module_dir)):
        if _candidate not in sys.path:
            sys.path.insert(0, _candidate)
    from message_utils import (
        message_content_from_message as _shared_message_content_from_message,
        message_content_to_text as _shared_message_content_to_text,
    )

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
# Configuration class for search parameters
# ────────────────────────────────────────────────────────────────────────
@dataclass
class SearchConfig:
    """Centralized configuration for search parameters.

    All timing, retry, and limit values that were previously hardcoded
    can now be customized by creating a SearchConfig instance.

    Attributes:
        max_results: Maximum number of search results to return (default: 10)
        timeout: HTTP request timeout in seconds (default: 30.0)
        max_retries: Maximum retry attempts on failure (default: 3)
        retry_delay: Initial delay between retries in seconds (default: 2.0)
        backoff_multiplier: Multiplier for exponential backoff (default: 1.5)
        captcha_backoff_base: Base multiplier for CAPTCHA backoff (default: 3.0)
        page_load_wait: Wait time after page load in seconds (default: 2.0)
        inter_search_delay: Delay between consecutive searches in seconds (default: 1.5)
        humanize_timing: Add random jitter to delays for human-like behavior (default: True)
        jitter_factor: Jitter range as fraction of base delay (default: 0.5 = ±50%)
        allow_direct_fallback: Allow fallback to direct IP (no proxy) on final retry (default: False)
            WARNING: Enabling this defeats anonymity - only use if you don't need Tor protection
    """
    max_results: int = 10
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 2.0
    backoff_multiplier: float = 1.5
    captcha_backoff_base: float = 3.0
    page_load_wait: float = 2.0
    inter_search_delay: float = 1.5  # Increased from 0.5 to reduce CAPTCHA rate
    humanize_timing: bool = True
    jitter_factor: float = 0.5
    allow_direct_fallback: bool = False  # CRITICAL: Default OFF to preserve anonymity


# ────────────────────────────────────────────────────────────────────────
# Human Behavioral Entropy - The nervous pulse of a tired hand
# ────────────────────────────────────────────────────────────────────────
# The problem with clean randomness: it's too clean. Uniform distributions
# smell like bleach. Real humans have texture - hesitation, fatigue,
# distraction, the micro-stutter of uncertainty.

_SESSION_START = time.time()
_REQUEST_COUNT = 0
# HIGH FIX: Lock to protect session state from race conditions in parallel execution
_SESSION_STATE_LOCK = threading.Lock()


def _human_delay(base_delay: float, cfg: SearchConfig = None) -> float:
    """Generate a delay that feels human - messy, uncertain, tired.

    Not uniform jitter. This models:
    - Log-normal base: most actions quick, occasional long pauses (thinking)
    - Micro-stutters: tiny random additions (the tremor of a hand)
    - Fatigue curve: delays drift longer as session ages
    - Occasional spikes: the pause of a mind changing

    Thread-safe: Uses lock to protect global session state.

    Args:
        base_delay: The nominal delay in seconds
        cfg: SearchConfig instance

    Returns:
        A delay that breathes like a human
    """
    global _REQUEST_COUNT
    # HIGH FIX: Protect global state update with lock
    with _SESSION_STATE_LOCK:
        _REQUEST_COUNT += 1
        local_request_count = _REQUEST_COUNT
        local_session_start = _SESSION_START

    if cfg is None:
        cfg = _default_config

    if not cfg.humanize_timing or base_delay <= 0:
        return base_delay

    # 1. Log-normal base: right-skewed, mostly quick but occasional long pauses
    # sigma controls spread: 0.3 = tight, 0.5 = moderate, 0.8 = wild
    import math
    sigma = 0.4
    log_normal_factor = random.lognormvariate(0, sigma)
    # Clamp to reasonable range (0.5x to 3x base)
    log_normal_factor = max(0.5, min(3.0, log_normal_factor))

    # 2. Micro-stutter: tiny random addition (50-200ms) - the tremor
    micro_stutter = random.uniform(0.05, 0.2)

    # 3. Fatigue curve: delays drift longer over session
    # After 100 requests, delays are ~20% longer
    # HIGH FIX: Use local copies to avoid race conditions
    session_minutes = (time.time() - local_session_start) / 60.0
    fatigue_factor = 1.0 + (session_minutes * 0.01) + (local_request_count * 0.001)
    fatigue_factor = min(fatigue_factor, 1.5)  # Cap at 50% increase

    # 4. Occasional thinking pause: 5% chance of a longer hesitation
    thinking_pause = 0
    if random.random() < 0.05:
        thinking_pause = random.uniform(0.5, 2.0)

    # 5. The hesitation before commit: slight pause before action
    pre_commit_hesitation = random.uniform(0.02, 0.08)

    # Combine: base * log_normal * fatigue + stutter + thinking + hesitation
    delay = (base_delay * log_normal_factor * fatigue_factor +
             micro_stutter + thinking_pause + pre_commit_hesitation)

    return max(0.1, delay)


def _humanize_delay(base_delay: float, cfg: SearchConfig = None) -> float:
    """Add human-like random jitter to a delay value.

    DEPRECATED: Use _human_delay() for more realistic behavioral patterns.
    This wrapper maintains backward compatibility but now uses the
    human behavioral model internally.

    Args:
        base_delay: The base delay in seconds
        cfg: SearchConfig instance (uses default if None)

    Returns:
        Delay with human behavioral patterns applied
    """
    return _human_delay(base_delay, cfg)


# Default configuration instance (thread-safe via copy-on-read pattern)
_default_config = SearchConfig()
_last_search_time: float = 0.0  # Track last search timestamp for inter-search delay
_search_lock = threading.Lock()  # Lock for thread-safe access to _last_search_time
_config_lock = threading.Lock()  # Lock for thread-safe config modifications


def _is_captcha_page(page_text: str, has_results: bool = False) -> bool:
    """MEDIUM FIX: More specific CAPTCHA detection to reduce false positives.

    Checks for DuckDuckGo-specific CAPTCHA indicators rather than just
    substring matching on generic words like 'robot'.

    Args:
        page_text: Lowercased page source text
        has_results: Whether valid search results were found

    Returns:
        True if page appears to be a CAPTCHA challenge, False otherwise
    """
    # If we have valid results, it's not a CAPTCHA page
    if has_results:
        return False

    # DuckDuckGo-specific CAPTCHA indicators
    ddg_captcha_indicators = [
        "please click to continue",  # DDG CAPTCHA prompt
        "human verification",
        "security check",
        "prove you're not a robot",
        "i'm not a robot",
        "verify you are human",
        "too many requests",
        "rate limit exceeded",
    ]

    # Generic but more specific indicators (require no results + indicator)
    generic_indicators = [
        "captcha",
        "unusual traffic",
        "automated queries",
        "suspected bot",
    ]

    # Check for DDG-specific indicators first (high confidence)
    for indicator in ddg_captcha_indicators:
        if indicator in page_text:
            return True

    # Check generic indicators only if combined with other signals
    for indicator in generic_indicators:
        if indicator in page_text:
            # Additional confirmation: check for form elements or challenge
            if "form" in page_text or "challenge" in page_text or "submit" in page_text:
                return True
            # Or if page is suspiciously short (CAPTCHA pages are usually small)
            if len(page_text) < 5000:
                return True

    # "robot" alone is too generic - require more context
    if "robot" in page_text:
        if ("not a robot" in page_text or
            "prove you" in page_text or
            "verify" in page_text):
            return True

    return False


def configure_search(
    max_results: int = None,
    timeout: float = None,
    max_retries: int = None,
    retry_delay: float = None,
    backoff_multiplier: float = None,
    captcha_backoff_base: float = None,
    page_load_wait: float = None,
    inter_search_delay: float = None,
    humanize_timing: bool = None,
    jitter_factor: float = None,
) -> SearchConfig:
    """Configure global search defaults. Call from R via reticulate.

    Only non-None values will update the defaults. Returns the updated config.
    Thread-safe: uses lock to prevent race conditions during configuration.

    Args:
        humanize_timing: Enable/disable human-like random jitter on delays
        jitter_factor: Jitter range as fraction of base delay (0.5 = ±50%)
    """
    global _default_config
    with _config_lock:
        if max_results is not None:
            _default_config.max_results = max_results
        if timeout is not None:
            _default_config.timeout = timeout
        if max_retries is not None:
            _default_config.max_retries = max_retries
        if retry_delay is not None:
            _default_config.retry_delay = retry_delay
        if backoff_multiplier is not None:
            _default_config.backoff_multiplier = backoff_multiplier
        if captcha_backoff_base is not None:
            _default_config.captcha_backoff_base = captcha_backoff_base
        if page_load_wait is not None:
            _default_config.page_load_wait = page_load_wait
        if inter_search_delay is not None:
            _default_config.inter_search_delay = inter_search_delay
        if humanize_timing is not None:
            _default_config.humanize_timing = humanize_timing
        if jitter_factor is not None:
            _default_config.jitter_factor = jitter_factor
        return _default_config


def configure_logging(level: str = "WARNING") -> None:
    """Configure search module logging level. Call from R via reticulate.

    Args:
        level: Log level - one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    """
    logger.setLevel(getattr(logging, level.upper(), logging.WARNING))
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)


_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0 Safari/537.36"
)

__all__ = [
    "SearchConfig",
    "configure_search",
    "configure_logging",
    "configure_tor",
    "configure_tor_registry",
    "configure_anti_detection",
    "PatchedDuckDuckGoSearchAPIWrapper",
    "PatchedDuckDuckGoSearchRun",
    "BrowserDuckDuckGoSearchAPIWrapper",
    "BrowserDuckDuckGoSearchRun",
]

# ────────────────────────────────────────────────────────────────────────
# Helper tier 2 – ddgs HTTP API (with retry and polite UA)
# ────────────────────────────────────────────────────────────────────────
def _with_ddgs(
    proxy: str | None,
    headers: Dict[str, str] | None,
    fn: Callable[[DDGS], Any],
    *,
    retries: int = 3,
    backoff: float = 1.5,
) -> Any:
    proxy = proxy or os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
    headers = dict(headers or {})
    headers.setdefault("User-Agent", _DEFAULT_UA)

    sleep = 0.01
    for attempt in range(1, retries + 1):
        try:
            # Use DDGS class (not ddgs module) for context manager
            # Note: newer versions of ddgs don't accept headers parameter
            with DDGS(proxy=proxy, timeout=20) as client:
                return fn(client)
        except DDGSException as exc:
            logger.warning("ddgs raised %s (try %d/%d)", exc, attempt, retries)
            if attempt == retries:
                raise
            time.sleep(sleep)
            sleep *= backoff


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
    except Exception:
        return None
    if not text:
        return None
    if text.lower() in {"none", "null", "na", "nan"}:
        return None
    return text


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _build_ddgs_kwargs(
    *,
    max_results: int,
    region: Any = None,
    safesearch: Any = None,
    timelimit: Any = None,
) -> Dict[str, Any]:
    """Build ddgs kwargs without overriding its defaults with None.

    Newer ddgs versions accept arbitrary kwargs, but several engines assume
    region/safesearch are strings and will call `.lower()`/`.split()` on them.
    Passing `None` overrides ddgs defaults and can trigger AttributeError.
    """
    kwargs: Dict[str, Any] = {"max_results": max_results}
    region_norm = _normalize_optional_str(region)
    if region_norm is not None:
        kwargs["region"] = region_norm
    safesearch_norm = _normalize_optional_str(safesearch)
    if safesearch_norm is not None:
        kwargs["safesearch"] = safesearch_norm
    timelimit_norm = _normalize_optional_str(timelimit)
    if timelimit_norm is not None:
        kwargs["timelimit"] = timelimit_norm
    return kwargs


# ────────────────────────────────────────────────────────────────────────
# Helper tier 1 – real browser (Selenium)
# ────────────────────────────────────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────────
# Stealth Configuration - Randomized fingerprints to reduce detection
# ────────────────────────────────────────────────────────────────────────
_STEALTH_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]

_STEALTH_VIEWPORTS = [
    (1920, 1080),  # Full HD - most common
    (1366, 768),   # HD - laptops
    (1536, 864),   # Scaled HD
    (1440, 900),   # MacBook
    (1280, 720),   # HD
    (2560, 1440),  # QHD
]

_STEALTH_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
]


def _simulate_human_behavior(driver, cfg: SearchConfig = None) -> None:
    """Inject the entropy of a tired hand into browser behavior.

    The algorithm at the gate looks for scripts. Scripts are efficient.
    Humans are not. This function makes the connection sweat:
    - Random scrolling (scanning the page)
    - Mouse drift (the hand that overshoots)
    - Micro-pauses (the mind that wanders)
    - Variable focus (looking at nothing in particular)

    Args:
        driver: Selenium WebDriver instance
        cfg: SearchConfig for timing parameters
    """
    cfg = cfg or _default_config
    if not getattr(cfg, "humanize_timing", True):
        return

    try:
        from selenium.webdriver.common.action_chains import ActionChains

        actions = ActionChains(driver)

        # 1. Initial hesitation - the moment of arrival, looking around
        time.sleep(random.uniform(0.3, 0.8))

        # 2. Scroll behavior - humans scan before they act
        # Sometimes scroll down, sometimes up, sometimes just a little
        scroll_patterns = [
            lambda: driver.execute_script("window.scrollBy(0, %d)" % random.randint(100, 300)),
            lambda: driver.execute_script("window.scrollBy(0, %d)" % random.randint(-50, 150)),
            lambda: driver.execute_script("window.scrollBy(0, %d)" % random.randint(50, 100)),
            lambda: None,  # Sometimes don't scroll at all
        ]
        scroll_action = random.choice(scroll_patterns)
        scroll_action()
        time.sleep(random.uniform(0.1, 0.4))

        # 3. Mouse movement - the drift of an uncertain hand
        # Move to a random point, overshoot, correct
        try:
            viewport_width = driver.execute_script("return window.innerWidth")
            viewport_height = driver.execute_script("return window.innerHeight")

            # Target somewhere vaguely in the content area
            target_x = random.randint(int(viewport_width * 0.2), int(viewport_width * 0.8))
            target_y = random.randint(int(viewport_height * 0.3), int(viewport_height * 0.7))

            # Overshoot by a bit
            overshoot_x = target_x + random.randint(-30, 30)
            overshoot_y = target_y + random.randint(-20, 20)

            # Move with slight curve (not a straight line)
            actions.move_by_offset(
                overshoot_x // 2 + random.randint(-10, 10),
                overshoot_y // 2 + random.randint(-5, 5)
            ).pause(random.uniform(0.05, 0.15))

            actions.move_by_offset(
                overshoot_x // 2 + random.randint(-15, 15),
                overshoot_y // 2 + random.randint(-10, 10)
            ).pause(random.uniform(0.02, 0.08))

            actions.perform()
            actions = ActionChains(driver)  # Reset chain

        except Exception:
            pass  # Mouse movement is best-effort

        # 4. Another small scroll - reconsidering the page
        if random.random() < 0.4:
            driver.execute_script("window.scrollBy(0, %d)" % random.randint(-30, 80))
            time.sleep(random.uniform(0.1, 0.3))

        # 5. The final pause - gathering thoughts before extraction
        time.sleep(random.uniform(0.2, 0.5))

    except Exception as e:
        logger.debug("Human behavior simulation failed (non-fatal): %s", e)


def _simulate_reading(driver, num_results: int, cfg: SearchConfig = None) -> None:
    """Simulate a human scanning search results.

    Humans don't just grab results. They look. They consider.
    They scroll back up because they missed something.

    Args:
        driver: Selenium WebDriver instance
        num_results: Number of results found
        cfg: SearchConfig for timing parameters
    """
    cfg = cfg or _default_config
    if not getattr(cfg, "humanize_timing", True):
        return

    try:
        # Time spent "reading" scales with results but has variance
        base_read_time = min(num_results * 0.15, 1.5)
        read_time = _human_delay(base_read_time, cfg)

        # Break into micro-intervals (scanning behavior)
        intervals = random.randint(2, 4)
        for i in range(intervals):
            time.sleep(read_time / intervals)

            # Occasionally scroll slightly (following content)
            if random.random() < 0.3:
                scroll_amount = random.randint(-20, 40)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount})")

    except Exception as e:
        logger.debug("Reading simulation failed (non-fatal): %s", e)


# ────────────────────────────────────────────────────────────────────────
# Tor Circuit Rotation - Request new identity on CAPTCHA detection
# ────────────────────────────────────────────────────────────────────────
# NOTE: Control port is read dynamically in _get_tor_control_port() to support
# per-worker port assignment via TOR_CONTROL_PORT environment variable
_TOR_CONTROL_PORT = 9051  # Fallback when env var is absent/invalid
_TOR_CONTROL_PASSWORD = os.environ.get("TOR_CONTROL_PASSWORD", "")
_TOR_LAST_ROTATION = 0.0
_TOR_MIN_ROTATION_INTERVAL = 5.0  # Minimum seconds between rotations (reduced from 10s)
_TOR_CONTROL_PORT_WARNED = False  # "warn once" for ControlPort connection refused


def _get_tor_control_port() -> int:
    """Get Tor control port dynamically from environment.

    This is read at call time (not module load time) to support per-worker
    port assignment in parallel execution environments.

    Returns:
        The Tor control port number (default: 9051)
    """
    port_str = os.environ.get("TOR_CONTROL_PORT")
    if port_str is not None:
        try:
            port = int(port_str)
            logger.debug("Using Tor control port: %d (from env: %s)", port, port_str)
            return port
        except ValueError:
            logger.warning("Invalid TOR_CONTROL_PORT '%s', using configured default %d", port_str, _TOR_CONTROL_PORT)
    return _TOR_CONTROL_PORT


def _tor_cookie_candidate_paths(control_port: int) -> List[str]:
    """Build prioritized Tor control cookie paths from env and defaults."""
    configured: List[str] = []
    direct_cookie = _normalize_optional_str(os.environ.get("ASA_TOR_CONTROL_COOKIE"))
    if direct_cookie:
        configured.append(os.path.expanduser(direct_cookie))

    try:
        socks_port = max(1, int(control_port) - 100)
    except Exception:
        socks_port = 9050

    template = _normalize_optional_str(os.environ.get("ASA_TOR_COOKIE_TEMPLATE"))
    if template:
        try:
            rendered = template.format(control_port=control_port, socks_port=socks_port)
        except Exception:
            rendered = template
        rendered_norm = _normalize_optional_str(rendered)
        if rendered_norm:
            configured.append(os.path.expanduser(rendered_norm))

    extra_paths_raw = _normalize_optional_str(os.environ.get("ASA_TOR_COOKIE_PATHS"))
    if extra_paths_raw:
        for segment in extra_paths_raw.split(os.pathsep):
            segment_norm = _normalize_optional_str(segment)
            if segment_norm:
                configured.append(os.path.expanduser(segment_norm))

    defaults = [
        f"/tmp/asa_tor_instance_{socks_port}/control_auth_cookie",
        f"/tmp/tor_{socks_port}/control_auth_cookie",
        "/var/lib/tor/control_auth_cookie",
        os.path.expanduser("~/.tor/control_auth_cookie"),
        "/opt/homebrew/var/lib/tor/control_auth_cookie",
        "/usr/local/var/lib/tor/control_auth_cookie",
    ]

    deduped: List[str] = []
    seen = set()
    for path in configured + defaults:
        normalized = _normalize_optional_str(path)
        if not normalized:
            continue
        expanded = os.path.expanduser(normalized)
        if expanded in seen:
            continue
        seen.add(expanded)
        deduped.append(expanded)
    return deduped

# ────────────────────────────────────────────────────────────────────────
# Shared Tor Exit Registry (SQLite, cross-process)
# ────────────────────────────────────────────────────────────────────────
_TOR_REGISTRY_ENABLED = True
_TOR_REGISTRY_PATH: str | None = os.environ.get("ASA_TOR_EXIT_DB") or os.environ.get("TOR_EXIT_DB")
_TOR_BAD_TTL = 3600.0
_TOR_GOOD_TTL = 1800.0
_TOR_OVERUSE_THRESHOLD = 8
_TOR_OVERUSE_DECAY = 900.0
_TOR_MAX_ROTATION_ATTEMPTS = 4
_TOR_IP_CACHE_TTL = 300.0
_TOR_EXIT_IP_CACHE: Dict[str, tuple[str, float]] = {}
_TOR_REGISTRY_LOCK = threading.Lock()

# CRITICAL FIX: Dedicated session for exit IP lookups with proper timeouts and retry
# This prevents blocking the main pipeline when Tor check endpoint is slow
_TOR_IP_CHECK_SESSION: requests.Session | None = None
_TOR_IP_CHECK_LOCK = threading.Lock()

# MEDIUM FIX: Lock for environment variable mutations (thread-safe proxy bypass)
_ENV_VAR_LOCK = threading.Lock()


def _get_tor_ip_session() -> requests.Session:
    """Get or create a dedicated session for exit IP lookups.

    Uses a separate session with tight timeouts and retry logic to avoid
    blocking the main search pipeline when check.torproject.org is slow.
    """
    global _TOR_IP_CHECK_SESSION
    with _TOR_IP_CHECK_LOCK:
        if _TOR_IP_CHECK_SESSION is None:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            session = requests.Session()
            # Retry strategy: 2 retries with short backoff
            retry_strategy = Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            _TOR_IP_CHECK_SESSION = session
        return _TOR_IP_CHECK_SESSION


def _resolve_registry_path() -> str | None:
    """Resolve or initialize the registry path."""
    global _TOR_REGISTRY_PATH

    if _TOR_REGISTRY_PATH:
        return _TOR_REGISTRY_PATH

    env_path = os.environ.get("ASA_TOR_EXIT_DB") or os.environ.get("TOR_EXIT_DB")
    if env_path:
        _TOR_REGISTRY_PATH = env_path
        return _TOR_REGISTRY_PATH

    base_dir = os.path.join(tempfile.gettempdir(), "asa_tor")
    os.makedirs(base_dir, exist_ok=True)
    _TOR_REGISTRY_PATH = os.path.join(base_dir, "tor_exit_registry.sqlite")
    return _TOR_REGISTRY_PATH


# HIGH FIX: Increased SQLite timeout for parallel workloads
_SQLITE_TIMEOUT = 5.0  # Was 1.0s, now 5.0s for parallel execution


def _sqlite_retry_transaction(path: str, callback, label: str = "sqlite op",
                              max_attempts: int = 3) -> None:
    """Run a SQLite write transaction with retry-on-contention.

    The callback receives an open connection with BEGIN IMMEDIATE already
    issued.  It must NOT call conn.commit() — the wrapper does that on
    success.

    Args:
        path: Path to the SQLite database file.
        callback: ``callable(conn)`` that executes SQL statements.
        label: Human-readable label for debug log messages.
        max_attempts: Number of retries on OperationalError (lock contention).
    """
    with _TOR_REGISTRY_LOCK:
        for attempt in range(max_attempts):
            try:
                with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    callback(conn)
                    conn.commit()
                    return
            except sqlite3.OperationalError as exc:
                logger.debug("%s contention (attempt %d): %s", label, attempt + 1, exc)
                time.sleep(0.05 * (attempt + 1))
            except Exception as exc:
                logger.debug("%s failed: %s", label, exc)
                return


def _ensure_registry() -> str | None:
    """Create registry if enabled; return path or None."""
    if not _TOR_REGISTRY_ENABLED:
        return None

    path = _resolve_registry_path()
    if not path:
        return None

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # HIGH FIX: Increased timeout from 1.0s to 5.0s for parallel workloads
        with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            # HIGH FIX: NORMAL sync is faster but still safe with WAL
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exit_health (
                    exit_ip TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_seen REAL NOT NULL,
                    failures INTEGER DEFAULT 0,
                    successes INTEGER DEFAULT 0,
                    last_reason TEXT,
                    cooldown_until REAL DEFAULT 0,
                    recent_uses INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS proxy_blocks (
                    proxy TEXT PRIMARY KEY,
                    last_hit REAL NOT NULL,
                    captcha_count INTEGER DEFAULT 1,
                    expires_at REAL NOT NULL
                )
                """
            )
        return path
    except Exception as exc:
        logger.debug("Tor registry setup failed: %s", exc)
        return None


def _get_exit_status(exit_ip: str) -> Dict[str, Any] | None:
    """Fetch exit status with TTL/overuse handling."""
    path = _ensure_registry()
    if not path:
        return None

    with _TOR_REGISTRY_LOCK:
        try:
            with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                row = conn.execute(
                    """
                    SELECT status, last_seen, failures, successes, last_reason, cooldown_until, recent_uses
                    FROM exit_health WHERE exit_ip=?
                    """,
                    (exit_ip,),
                ).fetchone()

                if not row:
                    return None

                status, last_seen, failures, successes, last_reason, cooldown_until, recent_uses = row
                now = time.time()

                expired = False
                if status == "bad":
                    if (cooldown_until and now >= cooldown_until) or (now - last_seen) >= _TOR_BAD_TTL:
                        expired = True
                elif status == "ok" and (now - last_seen) >= _TOR_GOOD_TTL:
                    expired = True

                if expired:
                    conn.execute("DELETE FROM exit_health WHERE exit_ip=?", (exit_ip,))
                    conn.commit()
                    return None

                # Decay overuse counts if stale
                if now - last_seen > _TOR_OVERUSE_DECAY:
                    recent_uses = 0
                    conn.execute(
                        "UPDATE exit_health SET recent_uses=?, last_seen=? WHERE exit_ip=?",
                        (0, now, exit_ip),
                    )
                    conn.commit()

                return {
                    "status": status,
                    "last_seen": last_seen,
                    "failures": failures,
                    "successes": successes,
                    "last_reason": last_reason,
                    "cooldown_until": cooldown_until,
                    "recent_uses": recent_uses or 0,
                    "overused": (recent_uses or 0) >= _TOR_OVERUSE_THRESHOLD
                    and (now - last_seen) <= _TOR_OVERUSE_DECAY,
                }
        except Exception as exc:
            logger.debug("Tor registry read failed for %s: %s", exit_ip, exc)
            return None


def _update_exit_record(
    exit_ip: str,
    status: str,
    reason: str | None = None,
    increment_failure: bool = False,
    increment_success: bool = False,
    bump_use: bool = False,
    cooldown_until: float | None = None,
) -> None:
    """Upsert exit health entry."""
    path = _ensure_registry()
    if not path or not exit_ip:
        return

    now = time.time()

    def _do_upsert(conn):
        row = conn.execute(
            """
            SELECT status, last_seen, failures, successes, last_reason, cooldown_until, recent_uses
            FROM exit_health WHERE exit_ip=?
            """,
            (exit_ip,),
        ).fetchone()

        nonlocal status
        if row:
            _, last_seen, failures, successes, last_reason, existing_cooldown, recent_uses = row
            if bump_use:
                if now - (last_seen or now) > _TOR_OVERUSE_DECAY:
                    recent_uses = 0
                recent_uses = (recent_uses or 0) + 1
            if increment_failure:
                failures = (failures or 0) + 1
            if increment_success:
                successes = (successes or 0) + 1
            effective_reason = reason or last_reason
            effective_cooldown = cooldown_until if cooldown_until is not None else (existing_cooldown or 0)

            if (recent_uses or 0) >= _TOR_OVERUSE_THRESHOLD:
                status = "bad"
                effective_reason = reason or "overused"
                effective_cooldown = max(effective_cooldown or 0, now + _TOR_OVERUSE_DECAY)

            conn.execute(
                """
                UPDATE exit_health
                SET status=?, last_seen=?, failures=?, successes=?, last_reason=?, cooldown_until=?, recent_uses=?
                WHERE exit_ip=?
                """,
                (
                    status,
                    now,
                    failures or 0,
                    successes or 0,
                    effective_reason,
                    effective_cooldown,
                    recent_uses or 0,
                    exit_ip,
                ),
            )
        else:
            recent_uses = 1 if bump_use else 0
            insert_status = status
            insert_reason = reason
            insert_cooldown = cooldown_until or 0

            if recent_uses >= _TOR_OVERUSE_THRESHOLD:
                insert_status = "bad"
                insert_reason = reason or "overused"
                insert_cooldown = max(insert_cooldown, now + _TOR_OVERUSE_DECAY)

            conn.execute(
                """
                INSERT INTO exit_health
                (exit_ip, status, last_seen, failures, successes, last_reason, cooldown_until, recent_uses)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exit_ip,
                    insert_status,
                    now,
                    1 if increment_failure else 0,
                    1 if increment_success else 0,
                    insert_reason,
                    insert_cooldown,
                    recent_uses,
                ),
            )

    _sqlite_retry_transaction(path, _do_upsert, label=f"Tor registry update for {exit_ip}")


def _clear_exit_cache(proxy: str) -> None:
    """Drop cached exit IP for a proxy."""
    with _TOR_REGISTRY_LOCK:
        if proxy in _TOR_EXIT_IP_CACHE:
            _TOR_EXIT_IP_CACHE.pop(proxy, None)


def _get_exit_ip(proxy: str | None, force_refresh: bool = False) -> str | None:
    """Resolve current Tor exit IP for a proxy.

    CRITICAL FIX: Uses dedicated session with tight timeouts to avoid blocking
    the main search pipeline when check.torproject.org is slow or unreachable.
    """
    if not proxy or "socks" not in proxy.lower():
        return None

    now = time.time()
    with _TOR_REGISTRY_LOCK:
        cached = _TOR_EXIT_IP_CACHE.get(proxy)
        if cached and not force_refresh:
            ip, ts = cached
            if now - ts <= _TOR_IP_CACHE_TTL:
                return ip

    proxies = {"http": proxy, "https": proxy}

    # Try multiple endpoints for resilience
    endpoints = [
        "https://check.torproject.org/api/ip",
        "https://api.ipify.org?format=json",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]

    session = _get_tor_ip_session()

    for endpoint in endpoints:
        try:
            # CRITICAL FIX: Tight timeout (5s) to avoid blocking pipeline
            resp = session.get(endpoint, proxies=proxies, timeout=5.0)
            if resp.status_code >= 400:
                logger.debug("Exit IP lookup failed for %s at %s: status %s",
                           proxy, endpoint, resp.status_code)
                continue

            # Parse response based on endpoint format
            if "json" in endpoint or endpoint.endswith("/ip"):
                try:
                    data = resp.json()
                    ip = data.get("IP") or data.get("ip")
                except (ValueError, KeyError):
                    # Not JSON, try plain text
                    ip = resp.text.strip()
            else:
                ip = resp.text.strip()

            # Validate IP format (basic check)
            if ip and "." in ip and len(ip) <= 45:  # Max IPv6 length
                with _TOR_REGISTRY_LOCK:
                    _TOR_EXIT_IP_CACHE[proxy] = (ip, now)
                logger.debug("Exit IP resolved for %s via %s: %s", proxy, endpoint, ip)
                return ip

        except requests.exceptions.Timeout:
            logger.debug("Exit IP lookup timed out for %s at %s", proxy, endpoint)
            continue
        except requests.exceptions.ConnectionError as exc:
            logger.debug("Exit IP lookup connection error for %s at %s: %s", proxy, endpoint, exc)
            continue
        except Exception as exc:
            logger.debug("Exit IP lookup failed for %s at %s: %s", proxy, endpoint, exc)
            continue

    logger.debug("Exit IP lookup failed for %s: all endpoints exhausted", proxy)
    return None


def _mark_exit_bad(proxy: str | None, reason: str = "tainted") -> None:
    """Mark current exit as bad and rotate circuit.

    HIGH FIX: When exit IP cannot be determined, still persist proxy-level
    block and rotate circuit, but skip exit_health updates to avoid registry
    pollution with unknown/None entries.
    """
    if not _TOR_REGISTRY_ENABLED or not proxy or "socks" not in proxy.lower():
        return

    exit_ip = _get_exit_ip(proxy, force_refresh=True)

    # HIGH FIX: Only update exit_health if we have a valid exit IP
    # This prevents registry pollution with unknown exits
    if exit_ip:
        _update_exit_record(
            exit_ip,
            status="bad",
            reason=reason,
            increment_failure=True,
            bump_use=True,
            cooldown_until=time.time() + _TOR_BAD_TTL,
        )
        logger.debug("Marked exit %s as bad: %s", exit_ip, reason)
    else:
        # Can't identify exit - still persist proxy-level block
        logger.warning("Cannot identify exit IP for proxy %s - persisting proxy-level block only", proxy)
        _persist_proxy_block(proxy, 1, time.time())

    _clear_exit_cache(proxy)
    _rotate_tor_circuit(force=True, proxy=proxy)


def _mark_exit_good(proxy: str | None) -> None:
    """Record successful usage for the current exit."""
    if not _TOR_REGISTRY_ENABLED or not proxy or "socks" not in proxy.lower():
        return

    exit_ip = _get_exit_ip(proxy, force_refresh=False)
    if not exit_ip:
        return

    _update_exit_record(
        exit_ip,
        status="ok",
        reason="success",
        increment_success=True,
        bump_use=True,
        cooldown_until=None,
    )


def _ensure_clean_exit(proxy: str | None) -> str | None:
    """Ensure we are not using a known bad/overused exit."""
    if not _TOR_REGISTRY_ENABLED or not proxy or "socks" not in proxy.lower():
        return None

    for attempt in range(_TOR_MAX_ROTATION_ATTEMPTS):
        exit_ip = _get_exit_ip(proxy, force_refresh=attempt > 0)
        if not exit_ip:
            logger.debug("Could not resolve exit IP on attempt %d/%d; rotating to try a fresh circuit",
                         attempt + 1, _TOR_MAX_ROTATION_ATTEMPTS)
            _rotate_tor_circuit(force=True, proxy=proxy)
            _clear_exit_cache(proxy)
            continue

        status = _get_exit_status(exit_ip)

        if status and status.get("status") == "bad":
            logger.info("Exit %s marked bad (%s); rotating", exit_ip, status.get("last_reason") or "tainted")
            _rotate_tor_circuit(force=True, proxy=proxy)
            _clear_exit_cache(proxy)
            continue

        if status and status.get("overused"):
            logger.info(
                "Exit %s overused (%d recent uses) - marking bad and rotating",
                exit_ip,
                status.get("recent_uses", 0),
            )
            _update_exit_record(
                exit_ip,
                status="bad",
                reason="overused",
                bump_use=True,
                cooldown_until=time.time() + _TOR_OVERUSE_DECAY,
            )
            _clear_exit_cache(proxy)
            _rotate_tor_circuit(force=True, proxy=proxy)
            continue

        _update_exit_record(exit_ip, status=status["status"] if status else "ok", reason="in_use", bump_use=True)

        # If bump_use crossed threshold inside _update_exit_record, status may now be bad
        refreshed_status = _get_exit_status(exit_ip)
        if refreshed_status and refreshed_status.get("status") == "bad" and refreshed_status.get("last_reason") == "overused":
            logger.info("Exit %s crossed overuse threshold during selection; rotating", exit_ip)
            _rotate_tor_circuit(force=True, proxy=proxy)
            _clear_exit_cache(proxy)
            continue

        return exit_ip

    return None

# ────────────────────────────────────────────────────────────────────────
# Proactive Anti-Detection State
# ────────────────────────────────────────────────────────────────────────
_REQUESTS_SINCE_ROTATION = 0
_PROACTIVE_ROTATION_ENABLED = True
_PROACTIVE_ROTATION_BASE = int(os.environ.get("ASA_PROACTIVE_ROTATION_INTERVAL", "15"))
_NEXT_PROACTIVE_ROTATION_TARGET = None

_REQUESTS_SINCE_SESSION_RESET = 0
_SESSION_RESET_ENABLED = True
_SESSION_RESET_BASE = int(os.environ.get("ASA_SESSION_RESET_INTERVAL", "50"))
_NEXT_SESSION_RESET_TARGET = None

def _sample_request_threshold(mean: int, spread: int, minimum: int, maximum: int) -> int:
    """Generate a stochastic interval to avoid detectable periodicity."""
    try:
        val = int(round(random.gauss(mean, spread)))
    except Exception:
        val = mean
    return max(minimum, min(maximum, val))


def _reset_rotation_targets() -> None:
    """Resample rotation/session reset thresholds."""
    global _NEXT_PROACTIVE_ROTATION_TARGET, _NEXT_SESSION_RESET_TARGET
    _NEXT_PROACTIVE_ROTATION_TARGET = _sample_request_threshold(
        _PROACTIVE_ROTATION_BASE, max(2, _PROACTIVE_ROTATION_BASE // 3), 5, 60
    )
    _NEXT_SESSION_RESET_TARGET = _sample_request_threshold(
        _SESSION_RESET_BASE, max(5, _SESSION_RESET_BASE // 3), 15, 120
    )

# Initialize stochastic targets on import
_reset_rotation_targets()

# Session CAPTCHA tracking - pause or fail if too many CAPTCHAs
_SESSION_CAPTCHA_COUNT = 0
_SESSION_CAPTCHA_THRESHOLD = 50  # After this many CAPTCHAs, take extended pause
_SESSION_CAPTCHA_CRITICAL = 100  # After this many, raise exception
_SESSION_CAPTCHA_PAUSE_DURATION = 300  # 5 minute pause after threshold

# ────────────────────────────────────────────────────────────────────────
# IP/Proxy Blocklist Tracking
# ────────────────────────────────────────────────────────────────────────
# Track proxies that have recently hit CAPTCHA to avoid reusing them
_PROXY_BLOCKLIST: Dict[str, float] = {}  # proxy -> timestamp of last CAPTCHA
_PROXY_BLOCKLIST_LOCK = threading.Lock()
_PROXY_BLOCK_DURATION = 300.0  # Block proxy for 5 minutes after CAPTCHA
_PROXY_CAPTCHA_COUNTS: Dict[str, int] = {}  # Track repeat offenders
_PROXY_MAX_CAPTCHA_COUNT = 3  # After this many CAPTCHAs, extend block duration


def _persist_proxy_block(proxy: str, captcha_count: int, now: float) -> None:
    """Persist proxy block to shared registry for cross-process awareness."""
    path = _ensure_registry()
    if not path or not proxy:
        return

    expires_at = now + (_PROXY_BLOCK_DURATION * min(captcha_count, 5))

    def _do_upsert(conn):
        conn.execute(
            """
            INSERT INTO proxy_blocks(proxy, last_hit, captcha_count, expires_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(proxy) DO UPDATE SET
                last_hit=excluded.last_hit,
                captcha_count=excluded.captcha_count,
                expires_at=excluded.expires_at
            """,
            (proxy, now, captcha_count, expires_at),
        )

    _sqlite_retry_transaction(path, _do_upsert, label=f"Proxy block persist for {proxy}")


def _get_proxy_block(proxy: str) -> Dict[str, float] | None:
    """Fetch proxy block entry from registry."""
    path = _ensure_registry()
    if not path or not proxy:
        return None

    with _TOR_REGISTRY_LOCK:
        try:
            with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                row = conn.execute(
                    "SELECT last_hit, captcha_count, expires_at FROM proxy_blocks WHERE proxy=?",
                    (proxy,),
                ).fetchone()
                if not row:
                    return None
                last_hit, captcha_count, expires_at = row
                return {
                    "last_hit": last_hit,
                    "captcha_count": captcha_count or 0,
                    "expires_at": expires_at,
                }
        except Exception as exc:
            logger.debug("Proxy block fetch failed for %s: %s", proxy, exc)
            return None


def _delete_proxy_block(proxy: str) -> None:
    """Remove proxy block entry from registry."""
    path = _ensure_registry()
    if not path or not proxy:
        return

    with _TOR_REGISTRY_LOCK:
        try:
            with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                conn.execute("DELETE FROM proxy_blocks WHERE proxy=?", (proxy,))
                conn.commit()
        except Exception as exc:
            logger.debug("Proxy block delete failed for %s: %s", proxy, exc)


def _cleanup_expired_proxy_blocks_db() -> int:
    """Purge expired proxy blocks from registry."""
    path = _ensure_registry()
    if not path:
        return 0

    with _TOR_REGISTRY_LOCK:
        try:
            now = time.time()
            with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM proxy_blocks WHERE expires_at <= ?",
                    (now,),
                ).fetchone()[0]
                if count:
                    conn.execute("DELETE FROM proxy_blocks WHERE expires_at <= ?", (now,))
                    conn.commit()
                return int(count or 0)
        except Exception as exc:
            logger.debug("Proxy block cleanup failed: %s", exc)
            return 0


def _record_captcha_hit(proxy: Optional[str]) -> None:
    """Record that a proxy has hit a CAPTCHA.

    Also tracks session-level CAPTCHA count and takes action if thresholds
    are exceeded (extended pause or exception).

    Args:
        proxy: The proxy URL that hit CAPTCHA, or None for direct connections
    """
    global _SESSION_CAPTCHA_COUNT

    # Track session-level CAPTCHAs (even for direct connections) - thread-safe
    with _SESSION_STATE_LOCK:
        _SESSION_CAPTCHA_COUNT += 1
        captcha_count = _SESSION_CAPTCHA_COUNT

    # Check session-level thresholds
    if captcha_count >= _SESSION_CAPTCHA_CRITICAL:
        logger.error("CRITICAL: Session has hit %d CAPTCHAs - search infrastructure may be blocked",
                    captcha_count)
        raise RuntimeError(f"Session CAPTCHA limit exceeded ({captcha_count} CAPTCHAs). "
                          "All exit nodes may be blocked. Consider: (1) waiting 10+ minutes, "
                          "(2) using different Tor circuits, (3) reducing request rate.")

    if captcha_count >= _SESSION_CAPTCHA_THRESHOLD and \
       captcha_count % _SESSION_CAPTCHA_THRESHOLD == 0:  # Every 50 CAPTCHAs
        logger.warning("Session has hit %d CAPTCHAs - taking extended pause (%.0fs)",
                      captcha_count, _SESSION_CAPTCHA_PAUSE_DURATION)
        time.sleep(_SESSION_CAPTCHA_PAUSE_DURATION)

    if not proxy:
        return  # Don't track per-proxy stats for direct connections

    now = time.time()
    persisted = _get_proxy_block(proxy)

    with _PROXY_BLOCKLIST_LOCK:
        baseline_count = _PROXY_CAPTCHA_COUNTS.get(proxy, 0)
        if persisted:
            baseline_count = max(baseline_count, persisted.get("captcha_count", 0))

        count = baseline_count + 1
        _PROXY_BLOCKLIST[proxy] = now
        _PROXY_CAPTCHA_COUNTS[proxy] = count

        if count >= _PROXY_MAX_CAPTCHA_COUNT:
            logger.warning("Proxy %s has hit CAPTCHA %d times - forcing circuit rotation",
                          proxy, count)
            # Force rotation to escape blocked exit node
            if proxy and "socks" in proxy.lower():
                rotation_success = _rotate_tor_circuit(force=True, proxy=proxy)
                if rotation_success:
                    logger.info("Forced circuit rotation succeeded after %d CAPTCHAs", count)
                else:
                    logger.warning("Forced circuit rotation FAILED after %d CAPTCHAs - check Tor control port", count)

    _persist_proxy_block(proxy, count, now)


def _is_proxy_blocked(proxy: Optional[str]) -> bool:
    """Check if a proxy is currently blocked due to recent CAPTCHA.

    Args:
        proxy: The proxy URL to check, or None for direct connections

    Returns:
        True if the proxy is blocked, False otherwise
    """
    if not proxy:
        return False  # Direct connections aren't tracked

    now = time.time()

    with _PROXY_BLOCKLIST_LOCK:
        if proxy not in _PROXY_BLOCKLIST:
            blocked_time = None
        else:
            blocked_time = _PROXY_BLOCKLIST[proxy]

        if blocked_time is not None:
            elapsed = now - blocked_time

            # Calculate block duration based on repeat offender status
            count = _PROXY_CAPTCHA_COUNTS.get(proxy, 1)
            effective_duration = _PROXY_BLOCK_DURATION * min(count, 5)  # Cap at 5x

            if elapsed < effective_duration:
                remaining = effective_duration - elapsed
                logger.debug("Proxy %s blocked for %.1f more seconds (CAPTCHA count: %d)",
                            proxy, remaining, count)
                return True

        # Block expired - clean it up (local cache)
        if blocked_time is not None:
            del _PROXY_BLOCKLIST[proxy]
            if proxy in _PROXY_CAPTCHA_COUNTS:
                del _PROXY_CAPTCHA_COUNTS[proxy]

    # Cross-process: check persisted registry
    entry = _get_proxy_block(proxy)
    if entry:
        remaining = entry.get("expires_at", 0) - now
        if remaining > 0:
            with _PROXY_BLOCKLIST_LOCK:
                _PROXY_BLOCKLIST[proxy] = entry.get("last_hit", now)
                _PROXY_CAPTCHA_COUNTS[proxy] = entry.get("captcha_count", 1)
            logger.debug("Proxy %s blocked via registry for %.1f more seconds (CAPTCHA count: %d)",
                        proxy, remaining, entry.get("captcha_count", 1))
            return True

        _delete_proxy_block(proxy)
        with _PROXY_BLOCKLIST_LOCK:
            _PROXY_BLOCKLIST.pop(proxy, None)
            _PROXY_CAPTCHA_COUNTS.pop(proxy, None)

    return False


def _cleanup_expired_blocks() -> int:
    """Remove expired entries from the blocklist.

    Returns:
        Number of entries removed
    """
    db_removed = _cleanup_expired_proxy_blocks_db()
    with _PROXY_BLOCKLIST_LOCK:
        now = time.time()
        expired = []

        for proxy, blocked_time in _PROXY_BLOCKLIST.items():
            count = _PROXY_CAPTCHA_COUNTS.get(proxy, 1)
            effective_duration = _PROXY_BLOCK_DURATION * min(count, 5)

            if now - blocked_time >= effective_duration:
                expired.append(proxy)

        for proxy in expired:
            del _PROXY_BLOCKLIST[proxy]
            # Also reset CAPTCHA count after block expires
            if proxy in _PROXY_CAPTCHA_COUNTS:
                del _PROXY_CAPTCHA_COUNTS[proxy]

        total_removed = len(expired) + db_removed

        if total_removed:
            logger.debug("Cleaned up %d expired proxy blocks (local=%d, db=%d)",
                        total_removed, len(expired), db_removed)

        return total_removed


def _get_blocklist_status() -> Dict[str, Any]:
    """Get current blocklist status for debugging.

    Returns:
        Dict with blocklist statistics
    """
    db_entries: Dict[str, Any] = {}
    path = _ensure_registry()
    if path:
        with _TOR_REGISTRY_LOCK:
            try:
                with sqlite3.connect(path, timeout=_SQLITE_TIMEOUT) as conn:
                    rows = conn.execute(
                        "SELECT proxy, last_hit, captcha_count, expires_at FROM proxy_blocks"
                    ).fetchall()
                    now = time.time()
                    db_entries = {
                        proxy: {
                            "blocked_since": now - last_hit,
                            "captcha_count": captcha_count,
                            "seconds_remaining": max(expires_at - now, 0),
                        }
                        for proxy, last_hit, captcha_count, expires_at in rows
                    }
            except Exception as exc:
                logger.debug("Proxy blocklist status read failed: %s", exc)

    with _PROXY_BLOCKLIST_LOCK:
        now = time.time()
        return {
            "blocked_proxies": len(_PROXY_BLOCKLIST),
            "total_captcha_counts": dict(_PROXY_CAPTCHA_COUNTS),
            "blocked_entries": {
                proxy: {
                    "blocked_since": now - ts,
                    "captcha_count": _PROXY_CAPTCHA_COUNTS.get(proxy, 0)
                }
                for proxy, ts in _PROXY_BLOCKLIST.items()
            },
            "registry_entries": db_entries,
        }


def _rotate_tor_circuit(force: bool = False, proxy: str | None = None, verify: bool = True) -> bool:
    """Request a new Tor circuit (new exit node) via the control port.

    This is the key to avoiding CAPTCHA when using Tor - each new circuit
    gets a different exit node IP address, making it harder to block.

    Requires Tor control port enabled. Configure in torrc:
        ControlPort 9051
        HashedControlPassword <your_hashed_password>
    Or use cookie authentication (default on many systems).

    Args:
        force: If True, rotate even if min interval hasn't passed
        proxy: Proxy URL to verify IP change (e.g., "socks5://127.0.0.1:9050")
        verify: If True, verify that exit IP actually changed after rotation

    Returns:
        True if rotation succeeded (and IP changed if verify=True), False otherwise
    """
    global _TOR_LAST_ROTATION, _TOR_CONTROL_PORT_WARNED

    # Get control port dynamically (supports per-worker assignment)
    control_port = _get_tor_control_port()

    # Rate limit rotations to avoid hammering Tor
    now = time.time()
    if not force and (now - _TOR_LAST_ROTATION) < _TOR_MIN_ROTATION_INTERVAL:
        logger.debug("Tor rotation skipped - too soon (%.1fs since last)",
                     now - _TOR_LAST_ROTATION)
        return False

    logger.info("Attempting Tor circuit rotation on port %d (force=%s)", control_port, force)

    # CRITICAL FIX: Get current exit IP before rotation for verification
    old_exit_ip = None
    if verify and proxy:
        old_exit_ip = _get_exit_ip(proxy, force_refresh=True)
        if old_exit_ip:
            logger.debug("Pre-rotation exit IP: %s", old_exit_ip)

    rotation_success = False

    # Try using stem library first (cleaner API)
    try:
        from stem import Signal
        from stem.control import Controller

        with Controller.from_port(port=control_port) as controller:
            if _TOR_CONTROL_PASSWORD:
                controller.authenticate(password=_TOR_CONTROL_PASSWORD)
            else:
                controller.authenticate()  # Try cookie auth
            controller.signal(Signal.NEWNYM)
            _TOR_LAST_ROTATION = time.time()
            rotation_success = True
            logger.info("Tor NEWNYM signal sent successfully via stem (port %d)", control_port)

    except ImportError:
        logger.debug("stem library not available, trying raw socket")
    except Exception as e:
        is_conn_refused = "Connection refused" in str(e) or "Errno 61" in str(e) or "Errno 111" in str(e)
        if is_conn_refused and _TOR_CONTROL_PORT_WARNED:
            logger.debug("stem rotation failed on port %d: %s (suppressed repeated warning)", control_port, e)
        else:
            logger.warning("stem rotation failed on port %d: %s", control_port, e)
            if is_conn_refused:
                _TOR_CONTROL_PORT_WARNED = True

    # Fallback: raw socket communication (only if stem didn't succeed)
    if not rotation_success:
        try:
            import socket

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5.0)
                sock.connect(("127.0.0.1", control_port))

                # Authenticate - try multiple methods
                auth_success = False

                if _TOR_CONTROL_PASSWORD:
                    # Method 1: Password authentication
                    sock.send(f'AUTHENTICATE "{_TOR_CONTROL_PASSWORD}"\r\n'.encode())
                    response = sock.recv(1024).decode()
                    auth_success = "250" in response

                if not auth_success:
                    # Method 2: Cookie authentication - try known paths
                    cookie_paths = _tor_cookie_candidate_paths(control_port)
                    for cookie_path in cookie_paths:
                        if os.path.exists(cookie_path):
                            try:
                                with open(cookie_path, "rb") as f:
                                    cookie = f.read().hex()
                                sock.send(f'AUTHENTICATE {cookie}\r\n'.encode())
                                response = sock.recv(1024).decode()
                                if "250" in response:
                                    auth_success = True
                                    logger.debug("Cookie auth succeeded from %s", cookie_path)
                                    break
                            except (IOError, PermissionError) as e:
                                logger.debug("Could not read cookie from %s: %s", cookie_path, e)

                if not auth_success:
                    # Method 3: Try empty auth (some Tor configs allow it)
                    if os.environ.get("ASA_ALLOW_TOR_EMPTY_AUTH") == "1":
                        sock.send(b'AUTHENTICATE\r\n')
                        response = sock.recv(1024).decode()
                        auth_success = "250" in response

                if not auth_success:
                    logger.warning("Tor authentication failed on port %d (tried password, cookie, and empty auth)", control_port)
                    return False

                # Request new identity
                sock.send(b'SIGNAL NEWNYM\r\n')
                response = sock.recv(1024).decode()

                if "250" in response:
                    _TOR_LAST_ROTATION = time.time()
                    rotation_success = True
                    logger.info("Tor NEWNYM signal sent successfully via raw socket (port %d)", control_port)
                else:
                    logger.warning("Tor NEWNYM failed on port %d: %s", control_port, response.strip())
                    return False

        except ConnectionRefusedError:
            if _TOR_CONTROL_PORT_WARNED:
                logger.debug("Tor control port %d: connection refused (suppressed repeated warning)", control_port)
            else:
                logger.warning("Tor control port %d: connection refused (is Tor running with ControlPort enabled?)", control_port)
                _TOR_CONTROL_PORT_WARNED = True
            return False
        except Exception as e:
            logger.warning("Raw socket Tor rotation failed on port %d: %s", control_port, e)
            return False

    # If rotation signal was sent but we couldn't verify, return success optimistically
    if not rotation_success:
        return False

    # CRITICAL FIX: Verify that exit IP actually changed after rotation
    if not verify or not proxy:
        return True

    for attempt in range(3):
        # Wait a moment for circuit to settle (progressively longer)
        time.sleep(1.5 + attempt)
        _clear_exit_cache(proxy)
        new_exit_ip = _get_exit_ip(proxy, force_refresh=True)

        if new_exit_ip:
            if not old_exit_ip or new_exit_ip != old_exit_ip:
                logger.info(
                    "Tor rotation VERIFIED: exit IP changed%s %s",
                    f" from {old_exit_ip}" if old_exit_ip else "",
                    new_exit_ip,
                )
                return True
            # Same IP — retry; new circuit may not be used by next connection yet
            logger.debug(
                "Tor rotation attempt %d/3: exit IP still %s, retrying...",
                attempt + 1, new_exit_ip,
            )

    # All retries exhausted — IP never changed
    if old_exit_ip:
        logger.info(
            "Tor rotation: exit IP unchanged after 3 attempts (still %s). "
            "This is normal when few exit relays are available.",
            old_exit_ip,
        )
    else:
        logger.warning("Tor rotation could not verify exit change (all lookups failed)")
    return False


def configure_tor(
    control_port: int = None,
    control_password: str = None,
    min_rotation_interval: float = None,
) -> dict:
    """Configure Tor circuit rotation settings.

    Call from R via reticulate to set up Tor control connection.

    Args:
        control_port: Tor control port (default: 9051)
        control_password: Control port password (or use cookie auth if None)
        min_rotation_interval: Minimum seconds between rotations (default: 10)

    Returns:
        Dict with current configuration
    """
    global _TOR_CONTROL_PORT, _TOR_CONTROL_PASSWORD, _TOR_MIN_ROTATION_INTERVAL, _TOR_CONTROL_PORT_WARNED

    _TOR_CONTROL_PORT_WARNED = False  # Reset so reconfiguration re-tests ControlPort

    if control_port is not None:
        _TOR_CONTROL_PORT = control_port
        # Keep env var in sync so worker processes that only read env still pick it up
        os.environ["TOR_CONTROL_PORT"] = str(control_port)
    if control_password is not None:
        _TOR_CONTROL_PASSWORD = control_password
    if min_rotation_interval is not None:
        _TOR_MIN_ROTATION_INTERVAL = min_rotation_interval

    return {
        "control_port": _TOR_CONTROL_PORT,
        "has_password": bool(_TOR_CONTROL_PASSWORD),
        "min_rotation_interval": _TOR_MIN_ROTATION_INTERVAL,
    }


def configure_tor_registry(
    registry_path: str | None = None,
    enable: bool | None = None,
    bad_ttl: float | None = None,
    good_ttl: float | None = None,
    overuse_threshold: int | None = None,
    overuse_decay: float | None = None,
    max_rotation_attempts: int | None = None,
    ip_cache_ttl: float | None = None,
) -> dict:
    """Configure shared Tor exit registry settings."""
    global _TOR_REGISTRY_PATH, _TOR_REGISTRY_ENABLED, _TOR_BAD_TTL, _TOR_GOOD_TTL
    global _TOR_OVERUSE_THRESHOLD, _TOR_OVERUSE_DECAY, _TOR_MAX_ROTATION_ATTEMPTS, _TOR_IP_CACHE_TTL

    if enable is not None:
        _TOR_REGISTRY_ENABLED = bool(enable)

    if registry_path:
        _TOR_REGISTRY_PATH = registry_path
        os.environ["ASA_TOR_EXIT_DB"] = registry_path
        os.environ["TOR_EXIT_DB"] = registry_path

    if bad_ttl is not None:
        _TOR_BAD_TTL = float(bad_ttl)
    if good_ttl is not None:
        _TOR_GOOD_TTL = float(good_ttl)
    if overuse_threshold is not None:
        _TOR_OVERUSE_THRESHOLD = int(overuse_threshold)
    if overuse_decay is not None:
        _TOR_OVERUSE_DECAY = float(overuse_decay)
    if max_rotation_attempts is not None:
        _TOR_MAX_ROTATION_ATTEMPTS = int(max_rotation_attempts)
    if ip_cache_ttl is not None:
        _TOR_IP_CACHE_TTL = float(ip_cache_ttl)

    if not _TOR_REGISTRY_ENABLED:
        _TOR_EXIT_IP_CACHE.clear()

    resolved_path = _ensure_registry()

    if resolved_path:
        logger.info("Tor registry configured (enabled=%s): %s", _TOR_REGISTRY_ENABLED, resolved_path)
    else:
        logger.info("Tor registry disabled")

    return {
        "registry_path": _resolve_registry_path(),
        "enabled": _TOR_REGISTRY_ENABLED,
        "bad_ttl": _TOR_BAD_TTL,
        "good_ttl": _TOR_GOOD_TTL,
        "overuse_threshold": _TOR_OVERUSE_THRESHOLD,
        "overuse_decay": _TOR_OVERUSE_DECAY,
        "max_rotation_attempts": _TOR_MAX_ROTATION_ATTEMPTS,
        "ip_cache_ttl": _TOR_IP_CACHE_TTL,
    }


# ────────────────────────────────────────────────────────────────────────
# Proactive Anti-Detection Functions
# ────────────────────────────────────────────────────────────────────────

def _maybe_proactive_rotation(proxy: str = None) -> bool:
    """Rotate Tor circuit proactively every N requests (not just on error).

    This significantly reduces CAPTCHA encounters by getting a fresh exit node
    IP before detection thresholds are reached.

    Args:
        proxy: Current proxy setting (only rotates if Tor proxy is in use)

    Returns:
        True if rotation was performed, False otherwise
    """
    global _REQUESTS_SINCE_ROTATION

    if not _PROACTIVE_ROTATION_ENABLED:
        return False

    # Only rotate if using Tor proxy
    if not proxy or "socks" not in proxy.lower():
        return False

    _REQUESTS_SINCE_ROTATION += 1

    if _REQUESTS_SINCE_ROTATION >= (_NEXT_PROACTIVE_ROTATION_TARGET or _PROACTIVE_ROTATION_BASE):
        logger.info(
            "Proactive circuit rotation (after %d requests; target=%d)",
            _REQUESTS_SINCE_ROTATION,
            _NEXT_PROACTIVE_ROTATION_TARGET or _PROACTIVE_ROTATION_BASE,
        )
        success = _rotate_tor_circuit(force=False, proxy=proxy)
        _REQUESTS_SINCE_ROTATION = 0
        _reset_rotation_targets()
        return success

    return False


def _maybe_session_reset() -> bool:
    """Reset session identity periodically to avoid fingerprint tracking.

    Clears timing state, shuffles user-agent pool, and resets request counters
    to make each session batch appear as a different user.

    Returns:
        True if session was reset, False otherwise
    """
    global _REQUESTS_SINCE_SESSION_RESET, _REQUEST_COUNT, _last_search_time
    global _SESSION_START

    if not _SESSION_RESET_ENABLED:
        return False

    _REQUESTS_SINCE_SESSION_RESET += 1

    if _REQUESTS_SINCE_SESSION_RESET >= (_NEXT_SESSION_RESET_TARGET or _SESSION_RESET_BASE):
        logger.info(
            "Session reset (after %d requests; target=%d)",
            _REQUESTS_SINCE_SESSION_RESET,
            _NEXT_SESSION_RESET_TARGET or _SESSION_RESET_BASE,
        )

        # Reset timing state to appear as new session
        _last_search_time = 0.0
        _REQUEST_COUNT = 0
        _SESSION_START = time.time()

        # Shuffle user-agent pool for variety
        random.shuffle(_STEALTH_USER_AGENTS)

        # Reset counter
        _REQUESTS_SINCE_SESSION_RESET = 0
        _reset_rotation_targets()

        return True

    return False


def configure_anti_detection(
    proactive_rotation_enabled: bool = None,
    proactive_rotation_interval: int = None,
    session_reset_enabled: bool = None,
    session_reset_interval: int = None,
) -> dict:
    """Configure proactive anti-detection settings.

    Call from R via reticulate to customize anti-detection behavior.

    Args:
        proactive_rotation_enabled: Enable/disable proactive Tor rotation
        proactive_rotation_interval: Requests between proactive rotations
        session_reset_enabled: Enable/disable periodic session reset
        session_reset_interval: Requests between session resets

    Returns:
        Dict with current configuration
    """
    global _PROACTIVE_ROTATION_ENABLED, _PROACTIVE_ROTATION_BASE
    global _SESSION_RESET_ENABLED, _SESSION_RESET_BASE

    if proactive_rotation_enabled is not None:
        _PROACTIVE_ROTATION_ENABLED = proactive_rotation_enabled
    if proactive_rotation_interval is not None:
        _PROACTIVE_ROTATION_BASE = proactive_rotation_interval
    if session_reset_enabled is not None:
        _SESSION_RESET_ENABLED = session_reset_enabled
    if session_reset_interval is not None:
        _SESSION_RESET_BASE = session_reset_interval

    _reset_rotation_targets()

    return {
        "proactive_rotation_enabled": _PROACTIVE_ROTATION_ENABLED,
        "proactive_rotation_interval": _PROACTIVE_ROTATION_BASE,
        "session_reset_enabled": _SESSION_RESET_ENABLED,
        "session_reset_interval": _SESSION_RESET_BASE,
    }


def _strip_chromedriver_from_path(path_value: str) -> str:
    if not path_value:
        return path_value
    sep = os.pathsep
    entries = [p for p in path_value.split(sep) if p]
    keep = []
    exe_names = ("chromedriver", "chromedriver.exe")
    for entry in entries:
        try:
            has_driver = any(os.path.exists(os.path.join(entry, exe)) for exe in exe_names)
        except Exception:
            has_driver = False
        if not has_driver:
            keep.append(entry)
    return sep.join(keep)


def _parse_major_version(version_str: str | None) -> int | None:
    if not version_str:
        return None
    m = re.search(r"(\\d+)\\.", version_str)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _detect_chrome_version() -> str | None:
    env_version = os.environ.get("ASA_CHROME_VERSION") or os.environ.get("CHROME_VERSION")
    if env_version:
        return env_version

    candidates = []
    if sys.platform == "darwin":
        candidates.extend([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ])
    for name in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"):
        path = shutil.which(name)
        if path:
            candidates.append(path)

    for path in candidates:
        if not path or not os.path.exists(path):
            continue
        try:
            out = subprocess.check_output([path, "--version"], stderr=subprocess.STDOUT)
            version = out.decode("utf-8", errors="ignore").strip()
            if version:
                return version
        except Exception:
            continue

    return None


def _detect_chrome_major() -> int | None:
    env_major = os.environ.get("ASA_CHROME_MAJOR") or os.environ.get("CHROME_MAJOR")
    if env_major:
        try:
            return int(env_major)
        except Exception:
            pass
    return _parse_major_version(_detect_chrome_version())


def _new_driver(
    *,
    proxy: str | None,
    headers: Dict[str, str] | None,
    headless: bool,
    bypass_proxy_for_driver: bool,
    use_proxy_for_browser: bool = True,
) -> Any:  # Returns UC Chrome, Firefox, or standard Chrome
    """Create a WebDriver instance with maximum stealth measures.

    Priority order (best stealth first):
    1. undetected-chromedriver (UC) - best anti-detection, handles most bot checks
    2. Firefox with stealth options - good fallback, different fingerprint
    3. Standard Chrome with manual stealth - last resort

    Args:
        proxy: SOCKS or HTTP proxy URL
        headers: Custom headers to inject
        headless: Run in headless mode
        bypass_proxy_for_driver: Temporarily clear proxy for driver download
        use_proxy_for_browser: Whether to use proxy for actual browsing

    Returns:
        WebDriver instance (UC Chrome, Firefox, or standard Chrome)
    """
    # Selenium Manager writes metadata/drivers into a cache directory.
    # In locked-down environments (containers, sandboxes, some CI runners),
    # the default cache location can be non-writable and driver creation fails
    # with messages like:
    #   "Metadata cannot be written in cache (.../.cache/selenium): Operation not permitted"
    #
    # If SE_CACHE_PATH isn't explicitly set, validate the default and fall back
    # to a temp dir cache when needed.
    if not os.environ.get("SE_CACHE_PATH"):
        default_cache = os.path.join(os.path.expanduser("~"), ".cache", "selenium")
        try:
            os.makedirs(default_cache, exist_ok=True)
            if not os.access(default_cache, os.W_OK):
                raise OSError(f"Selenium cache not writable: {default_cache}")
        except OSError:
            tmp_cache = os.path.join(tempfile.gettempdir(), "asa_selenium_cache")
            os.makedirs(tmp_cache, exist_ok=True)
            os.environ["SE_CACHE_PATH"] = tmp_cache

    # Temporarily clear proxies so Selenium-Manager/UC can download drivers
    # MEDIUM FIX: Use lock to make env var mutations thread-safe
    saved_env: Dict[str, str] = {}
    if bypass_proxy_for_driver:
        with _ENV_VAR_LOCK:
            for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                if var in os.environ:
                    saved_env[var] = os.environ.pop(var)

    # Randomize fingerprint components
    random_ua = random.choice(_STEALTH_USER_AGENTS)
    random_viewport = random.choice(_STEALTH_VIEWPORTS)
    random_lang = random.choice(_STEALTH_LANGUAGES)

    driver = None
    ignore_path = str(os.environ.get("ASA_IGNORE_PATH_CHROMEDRIVER", "")).lower() in ("1", "true", "yes")
    disable_uc = str(os.environ.get("ASA_DISABLE_UC", "")).lower() in ("1", "true", "yes") or ignore_path
    chrome_major = _detect_chrome_major()
    try:
        # ════════════════════════════════════════════════════════════════════
        # TIER 1: undetected-chromedriver (UC) - BEST STEALTH
        # ════════════════════════════════════════════════════════════════════
        # UC patches ChromeDriver to evade detection by anti-bot services.
        # It handles: webdriver flag, CDP detection, headless detection, etc.
        try:
            if disable_uc:
                raise ImportError("UC disabled via ASA_DISABLE_UC/ASA_IGNORE_PATH_CHROMEDRIVER")

            import undetected_chromedriver as uc

            logger.info("Attempting undetected-chromedriver (UC) for maximum stealth")

            # UC options - it handles most stealth automatically
            uc_options = uc.ChromeOptions()

            # Headless mode - UC supports headless but it's more detectable
            if headless:
                uc_options.add_argument("--headless=new")

            # Window size
            uc_options.add_argument(f"--window-size={random_viewport[0]},{random_viewport[1]}")

            # Disable GPU (reduces fingerprinting surface)
            uc_options.add_argument("--disable-gpu")
            uc_options.add_argument("--disable-dev-shm-usage")
            uc_options.add_argument("--no-sandbox")

            # Language
            uc_options.add_argument(f"--lang={random_lang.split(',')[0]}")

            # Proxy handling for UC
            if proxy and use_proxy_for_browser:
                # UC can handle SOCKS proxies but needs special handling
                if "socks" in (proxy or "").lower():
                    # For SOCKS proxy, we need to use a proxy extension or bypass
                    # UC doesn't natively support SOCKS well in headless
                    logger.debug("UC: SOCKS proxy detected, using proxy-server arg")
                    uc_options.add_argument(f"--proxy-server={proxy}")
                else:
                    uc_options.add_argument(f"--proxy-server={proxy}")

            # Create UC driver
            # use_subprocess=True helps with cleanup in parallel environments
            driver = uc.Chrome(
                options=uc_options,
                use_subprocess=True,
                version_main=chrome_major,  # Prefer installed Chrome major when available
            )

            logger.info("Using undetected-chromedriver (UC) - maximum stealth mode")

            # UC handles most stealth automatically, but we can add extras
            try:
                # Set custom user agent if needed
                platform_override = "Win32"
                if "mac" in random_ua.lower() or "safari" in random_ua.lower():
                    platform_override = "MacIntel"
                driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                    "userAgent": random_ua,
                    "acceptLanguage": random_lang,
                    "platform": platform_override
                })
            except Exception as e:
                logger.debug("UC user agent override failed (non-fatal): %s", e)

            # Inject custom headers if provided
            if headers:
                try:
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": headers})
                except Exception as e:
                    logger.debug("UC header injection failed (non-fatal): %s", e)

            return driver

        except ImportError as ie:
            if disable_uc:
                logger.info("Skipping undetected-chromedriver (%s), trying Firefox", ie)
            else:
                logger.debug("undetected-chromedriver not installed, trying Firefox")
        except Exception as uc_err:
            logger.warning("UC Chrome failed (%s), trying Firefox fallback", uc_err)
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

        # ════════════════════════════════════════════════════════════════════
        # TIER 2: Firefox with stealth options
        # ════════════════════════════════════════════════════════════════════
        firefox_bin = None
        try:
            firefox_bin = shutil.which("firefox") or shutil.which("firefox-bin")
            if not firefox_bin and os.path.exists("/Applications/Firefox.app/Contents/MacOS/firefox"):
                firefox_bin = "/Applications/Firefox.app/Contents/MacOS/firefox"
        except Exception:
            firefox_bin = None

        if firefox_bin:
            try:
                firefox_opts = FirefoxOptions()
                if headless:
                    firefox_opts.add_argument("--headless")
                firefox_opts.add_argument("--disable-gpu")
                firefox_opts.add_argument("--no-sandbox")
                firefox_opts.add_argument(f"--width={random_viewport[0]}")
                firefox_opts.add_argument(f"--height={random_viewport[1]}")
                try:
                    firefox_opts.binary_location = firefox_bin
                except Exception:
                    pass

                # Firefox stealth preferences
                firefox_opts.set_preference("general.useragent.override", random_ua)
                firefox_opts.set_preference("intl.accept_languages", random_lang)
                firefox_opts.set_preference("dom.webdriver.enabled", False)
                firefox_opts.set_preference("useAutomationExtension", False)
                firefox_opts.set_preference("privacy.trackingprotection.enabled", False)
                firefox_opts.set_preference("network.http.sendRefererHeader", 2)
                firefox_opts.set_preference("general.platform.override", "Win32")

                # Disable telemetry and crash reporting
                firefox_opts.set_preference("toolkit.telemetry.enabled", False)
                firefox_opts.set_preference("datareporting.healthreport.uploadEnabled", False)
                firefox_opts.set_preference("browser.crashReports.unsubmittedCheck.autoSubmit2", False)

                if proxy and use_proxy_for_browser:
                    firefox_opts.set_preference("network.proxy.type", 1)
                    if "socks" in (proxy or "").lower():
                        import re
                        match = re.match(r"socks\d*h?://([^:]+):(\d+)", proxy)
                        if match:
                            firefox_opts.set_preference("network.proxy.socks", match.group(1))
                            firefox_opts.set_preference("network.proxy.socks_port", int(match.group(2)))
                            firefox_opts.set_preference("network.proxy.socks_remote_dns", True)

                driver = webdriver.Firefox(options=firefox_opts)
                logger.info("Using Firefox WebDriver (stealth mode)")

                # Additional stealth via JavaScript
                try:
                    driver.execute_script("""
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    """)
                except Exception as e:
                    logger.debug("Firefox stealth script injection failed: %s", e)

                return driver

            except Exception as firefox_err:
                logger.debug("Firefox unavailable (%s), trying standard Chrome", firefox_err)
        else:
            logger.debug("Firefox binary not found, skipping Firefox tier")

        # ════════════════════════════════════════════════════════════════════
        # TIER 3: Standard Chrome with manual stealth (last resort)
        # ════════════════════════════════════════════════════════════════════
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
        chrome_opts = ChromeOptions()
        if headless:
            chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument(f"--window-size={random_viewport[0]},{random_viewport[1]}")
        chrome_opts.add_argument(f"--user-agent={random_ua}")
        chrome_opts.add_argument(f"--lang={random_lang.split(',')[0]}")

        # Manual stealth arguments
        chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
        chrome_opts.add_argument("--disable-infobars")
        chrome_opts.add_argument("--disable-extensions")
        chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_opts.add_experimental_option("useAutomationExtension", False)

        if proxy and use_proxy_for_browser:
            chrome_opts.add_argument(f"--proxy-server={proxy}")

        driver_path = os.environ.get("ASA_CHROMEDRIVER_BIN") or os.environ.get("CHROMEDRIVER_BIN")
        ignore_path = str(os.environ.get("ASA_IGNORE_PATH_CHROMEDRIVER", "")).lower() in ("1", "true", "yes")
        restored_path = None
        if ignore_path and not driver_path:
            restored_path = os.environ.get("PATH", "")
            os.environ["PATH"] = _strip_chromedriver_from_path(restored_path)
            logger.info("Ignoring PATH chromedriver entries (ASA_IGNORE_PATH_CHROMEDRIVER=1)")
        try:
            if driver_path:
                driver = webdriver.Chrome(service=ChromeService(executable_path=driver_path), options=chrome_opts)
            else:
                driver = webdriver.Chrome(options=chrome_opts)
        finally:
            if restored_path is not None:
                os.environ["PATH"] = restored_path
        logger.info("Using standard Chrome WebDriver (manual stealth mode)")

        # Manual stealth via CDP
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    window.chrome = {runtime: {}};
                """
            })
        except Exception as e:
            logger.debug("Chrome stealth CDP injection failed: %s", e)

        if headers:
            try:
                driver.execute_cdp_cmd("Network.enable", {})
                driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": headers})
            except Exception as e:
                logger.debug("CDP header injection failed: %s", e)

        return driver

    finally:
        # MEDIUM FIX: Use lock to make env var restoration thread-safe
        if saved_env:
            with _ENV_VAR_LOCK:
                os.environ.update(saved_env)


def _browser_search(
    query: str,
    *,
    max_results: int = None,
    proxy: str | None,
    headers: Dict[str, str] | None,
    headless: bool,
    bypass_proxy_for_driver: bool,
    timeout: float = None,
    max_retries: int = None,
    url_override: str | None = None,
    config: SearchConfig = None,
) -> List[Dict[str, str]]:
    cfg = config or _default_config
    max_results = max_results if max_results is not None else cfg.max_results
    timeout = timeout if timeout is not None else cfg.timeout
    max_retries = max_retries if max_retries is not None else cfg.max_retries

    logger.debug("Starting browser search for: %s", query[:60])

    result_link_selector = "a.result__a, a.result-link, a[data-testid='result-title-a']"
    result_snippet_selector = "div.result__snippet, a.result__snippet"

    # Strategy: Always use proxy if configured (preserve anonymity by default)
    # Only fall back to direct IP if explicitly allowed via config
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        # CRITICAL FIX: Direct IP fallback is now opt-in to preserve anonymity
        use_proxy = bool(proxy)  # Always use proxy if configured
        if attempt >= 2 and proxy and cfg.allow_direct_fallback:
            use_proxy = False
            logger.warning("ANONYMITY WARNING: Switching to DIRECT IP (no proxy) for final attempt - "
                          "your real IP will be exposed. Set allow_direct_fallback=False to disable.")

        driver = None  # Initialize to None for safe cleanup
        proxy_label = proxy if (proxy and use_proxy) else "direct"
        try:
            driver = _new_driver(
                proxy=proxy,
                headers=headers,
                headless=headless,
                bypass_proxy_for_driver=bypass_proxy_for_driver,
                use_proxy_for_browser=use_proxy,
            )
            # Use html.duckduckgo.com which is the lite/HTML version
            # url_override exists for deterministic testing (e.g., file:// fixtures)
            start_url = url_override or f"https://html.duckduckgo.com/html/?q={quote(query)}"
            driver.get(start_url)

            # Wait a moment for page to load, then check for CAPTCHA
            # Use humanized timing for less predictable patterns
            humanized_page_wait = _humanize_delay(cfg.page_load_wait, cfg)
            time.sleep(humanized_page_wait)
            page_source = ""
            try:
                page_source = driver.page_source.lower()
            except Exception:
                page_source = ""

            # MEDIUM FIX: Use more specific CAPTCHA detection
            if _is_captcha_page(page_source, has_results=False):
                logger.warning("Browser CAPTCHA detected on attempt %d/%d (proxy=%s, query=%s...)",
                             attempt + 1, max_retries, proxy_label, query[:30])
                # Record this proxy hit for blocklist tracking
                if proxy and use_proxy:
                    _record_captcha_hit(proxy)
                    _mark_exit_bad(proxy, reason="captcha_browser")
                if attempt < max_retries - 1:
                    # Rotate Tor circuit to get new exit node IP before retry (force=True to bypass rate limit)
                    if proxy and use_proxy and "socks" in proxy.lower():
                        if _rotate_tor_circuit(force=True, proxy=proxy):
                            logger.info("Tor circuit rotated (forced) - new exit node for browser retry")
                        else:
                            logger.warning("Tor rotation FAILED - check control port auth")
                    if proxy and use_proxy:
                        _ensure_clean_exit(proxy)
                    base_wait = cfg.captcha_backoff_base * (attempt + 1)  # Exponential backoff
                    wait_time = _humanize_delay(base_wait, cfg)
                    logger.info("Waiting %.1fs before retry (humanized)...", wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    raise WebDriverException("CAPTCHA detected after all retries")

            # Now wait for results (with shorter timeout since we already loaded)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, result_link_selector))
            )

            # Inject human behavior - the nervous pulse, the wandering eye
            _simulate_human_behavior(driver, cfg)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            result_links = soup.select(result_link_selector)[:max_results]
            result_snippets = soup.select(result_snippet_selector)[:max_results]
            if not result_links:
                raise WebDriverException("DuckDuckGo results container loaded but no result links were found")

            # Simulate reading/scanning the results
            _simulate_reading(driver, len(result_links), cfg)

            # return block
            from urllib.parse import urlparse, parse_qs, unquote
            return_content = []
            for idx, link in enumerate(result_links):
                raw_href = link["href"]
                # extract & decode uddg (or fall back to the raw href)
                real_url = unquote(
                    parse_qs(urlparse(raw_href).query)
                    .get("uddg", [raw_href])[0]
                    )
                real_url = _canonicalize_url(real_url) or real_url
                if _is_noise_source_url(real_url):
                    continue
                snippet = (
                    result_snippets[idx].get_text(strip=True)
                    if idx < len(result_snippets)
                    else ""
                    )
                return_content.append({
                    "id": idx + 1,
                    "title": link.get_text(strip=True),
                    "href": raw_href,
                    "body": f"__START_OF_SOURCE {idx + 1}__ <CONTENT> {snippet} </CONTENT> <URL> {real_url} </URL> __END_OF_SOURCE  {idx + 1}__",
                    "_tier": "selenium",
                })

            if proxy and use_proxy:
                _mark_exit_good(proxy)
            logger.info("Browser search SUCCESS: returning %d results", len(return_content))
            return return_content
        except TimeoutException as exc:
            last_exc = exc
            current_url = ""
            title = ""
            page_text = ""
            try:
                current_url = driver.current_url if driver else ""
            except Exception:
                current_url = ""
            try:
                title = driver.title if driver else ""
            except Exception:
                title = ""
            try:
                page_text = driver.page_source.lower() if driver else ""
            except Exception:
                page_text = ""

            if page_text and _is_captcha_page(page_text, has_results=False):
                logger.warning("Browser CAPTCHA detected after timeout on attempt %d/%d (proxy=%s, query=%s...)",
                               attempt + 1, max_retries, proxy_label, query[:30])
                if proxy and use_proxy:
                    _record_captcha_hit(proxy)
                    _mark_exit_bad(proxy, reason="captcha_browser")
                if attempt < max_retries - 1:
                    if proxy and use_proxy and "socks" in proxy.lower():
                        if _rotate_tor_circuit(force=True, proxy=proxy):
                            logger.info("Tor circuit rotated (forced) - new exit node for browser retry")
                        else:
                            logger.warning("Tor rotation FAILED - check control port auth")
                    if proxy and use_proxy:
                        _ensure_clean_exit(proxy)
                    base_wait = cfg.captcha_backoff_base * (attempt + 1)
                    wait_time = _humanize_delay(base_wait, cfg)
                    logger.info("Waiting %.1fs before retry (humanized)...", wait_time)
                    time.sleep(wait_time)
                    continue
                raise WebDriverException("CAPTCHA detected after all retries") from exc

            if current_url.startswith("about:") or current_url.startswith("chrome-error://"):
                if proxy and use_proxy:
                    _mark_exit_bad(proxy, reason="browser_neterror")
                logger.warning("Browser load failed on attempt %d/%d (proxy=%s, url=%s, title=%s)",
                               attempt + 1, max_retries, proxy_label, current_url or "<unknown>", title or "<none>")
            else:
                if proxy and use_proxy:
                    _mark_exit_bad(proxy, reason="browser_timeout")
                logger.warning("Browser timed out waiting for results on attempt %d/%d (proxy=%s, url=%s, title=%s)",
                               attempt + 1, max_retries, proxy_label, current_url or "<unknown>", title or "<none>")

            if attempt < max_retries - 1:
                if proxy and use_proxy and "socks" in proxy.lower():
                    _rotate_tor_circuit(force=True, proxy=proxy)
                if proxy and use_proxy:
                    _ensure_clean_exit(proxy)
                base_wait = cfg.retry_delay * (cfg.backoff_multiplier ** attempt)
                wait_time = _humanize_delay(base_wait, cfg)
                time.sleep(wait_time)
                continue

            raise
        except WebDriverException as exc:
            last_exc = exc
            current_url = ""
            title = ""
            page_text = ""
            try:
                current_url = driver.current_url if driver else ""
            except Exception:
                current_url = ""
            try:
                title = driver.title if driver else ""
            except Exception:
                title = ""
            try:
                page_text = driver.page_source.lower() if driver else ""
            except Exception:
                page_text = ""

            if page_text and _is_captcha_page(page_text, has_results=False):
                logger.warning("Browser CAPTCHA detected on attempt %d/%d (proxy=%s, query=%s...)",
                               attempt + 1, max_retries, proxy_label, query[:30])
                if proxy and use_proxy:
                    _record_captcha_hit(proxy)
                    _mark_exit_bad(proxy, reason="captcha_browser")
                if attempt < max_retries - 1:
                    if proxy and use_proxy and "socks" in proxy.lower():
                        if _rotate_tor_circuit(force=True, proxy=proxy):
                            logger.info("Tor circuit rotated (forced) - new exit node for browser retry")
                        else:
                            logger.warning("Tor rotation FAILED - check control port auth")
                    if proxy and use_proxy:
                        _ensure_clean_exit(proxy)
                    base_wait = cfg.captcha_backoff_base * (attempt + 1)
                    wait_time = _humanize_delay(base_wait, cfg)
                    logger.info("Waiting %.1fs before retry (humanized)...", wait_time)
                    time.sleep(wait_time)
                    continue
                raise WebDriverException("CAPTCHA detected after all retries") from exc

            if proxy and use_proxy:
                if current_url.startswith("about:") or current_url.startswith("chrome-error://"):
                    _mark_exit_bad(proxy, reason="browser_neterror")
                else:
                    _mark_exit_bad(proxy, reason=f"browser_{type(exc).__name__}")

            logger.warning(
                "Browser search failed on attempt %d/%d (proxy=%s, error=%s, url=%s, title=%s)",
                attempt + 1,
                max_retries,
                proxy_label,
                type(exc).__name__,
                current_url or "<unknown>",
                title or "<none>",
            )

            if attempt < max_retries - 1:
                if proxy and use_proxy and "socks" in proxy.lower():
                    _rotate_tor_circuit(force=True, proxy=proxy)
                if proxy and use_proxy:
                    _ensure_clean_exit(proxy)
                base_wait = cfg.retry_delay * (cfg.backoff_multiplier ** attempt)
                wait_time = _humanize_delay(base_wait, cfg)
                time.sleep(wait_time)
                continue

            raise
        finally:
            # Safe driver cleanup with null check and specific exception handling
            if driver is not None:
                time.sleep(random.uniform(0, 0.01))
                try:
                    driver.quit()
                except (WebDriverException, OSError) as e:
                    logger.debug("Driver cleanup failed: %s", e)
                except Exception as e:
                    logger.warning("Unexpected error during driver cleanup: %s: %s",
                                   type(e).__name__, e)

    # If we get here, all retries failed
    if last_exc is not None:
        raise WebDriverException("Browser search failed after all retries") from last_exc
    raise WebDriverException("Browser search failed after all retries")


# ────────────────────────────────────────────────────────────────────────
# Helper tier 3 – tiny Requests + BeautifulSoup fallback
# ────────────────────────────────────────────────────────────────────────
def _requests_scrape(
    query: str,
    *,
    max_results: int,
    proxy: str | None,
    headers: Dict[str, str] | None,
    timeout: int = 10,
) -> List[Dict[str, str]]:
    """
    Very small "Plan C" that fetches the DuckDuckGo Lite HTML endpoint
    and scrapes results.  No Javascript, so it's low‑rate and robust.

    HIGH FIX: Now includes preflight checks for consistency with other tiers.
    """
    # HIGH FIX: Apply same preflight checks as other tiers
    _maybe_session_reset()
    _maybe_proactive_rotation(proxy)

    # Ensure clean exit before making request
    if proxy and "socks" in proxy.lower():
        exit_ip = _ensure_clean_exit(proxy)
        if exit_ip:
            logger.debug("Plan C using exit: %s", exit_ip)

    # Check if proxy is blocklisted
    if _is_proxy_blocked(proxy):
        logger.info("Plan C: proxy %s is blocklisted - rotating before request", proxy or "direct")
        if proxy and "socks" in proxy.lower():
            _rotate_tor_circuit(force=True, proxy=proxy)

    url = "https://html.duckduckgo.com/html"
    headers = dict(headers or {})
    headers.setdefault("User-Agent", _DEFAULT_UA)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        resp = requests.post(
            url,
            data=urlencode({"q": query}),
            headers=headers,
            proxies=proxies,
            timeout=timeout,
        )
        if resp.status_code in (403, 429):
            _mark_exit_bad(proxy, reason=f"http_{resp.status_code}")
        resp.raise_for_status()
    except Exception:
        _mark_exit_bad(proxy, reason="requests_scrape_error")
        raise

    page_text = resp.text.lower()
    # MEDIUM FIX: Use specific CAPTCHA detection to reduce false positives
    if _is_captcha_page(page_text, has_results=False):
        _record_captcha_hit(proxy)
        _mark_exit_bad(proxy, reason="captcha_requests")

    soup = BeautifulSoup(resp.text, "html.parser")
    results: List[Dict[str, str]] = []
    for i, a in enumerate(soup.select("a.result__a")[:max_results], 1):
        href = _canonicalize_url(a.get("href", "")) or a.get("href", "")
        if _is_noise_source_url(href):
            continue
        # Extract snippet body from the result container
        body = ""
        result_container = a.find_parent(class_="result")
        if result_container:
            snippet_el = result_container.select_one(".result__snippet")
            if snippet_el:
                body = snippet_el.get_text(strip=True)
        if not body:
            # Fallback: try sibling or parent text
            parent = a.find_parent()
            if parent:
                body = parent.get_text(strip=True)[:300]
        results.append({"id": i, "title": a.get_text(strip=True), "href": href, "body": body, "_tier": "requests"})
    results = _rerank_search_results(query, results, max_results=max_results)
    normalized_results = _normalize_tool_result_rows(results, max_results=max_results)
    if normalized_results:
        results = normalized_results
    if results:
        _mark_exit_good(proxy)
    return results


# ────────────────────────────────────────────────────────────────────────
# Public LangChain‑compatible wrappers
# ────────────────────────────────────────────────────────────────────────
class PatchedDuckDuckGoSearchAPIWrapper(DuckDuckGoSearchAPIWrapper):
    """
    A robust DuckDuckGo wrapper with three search tiers.
    """

    # Upstream fields
    k: int = Field(default=10, description="Number of results to return")
    max_results: int = Field(default=10, description="Number of results to return")
    region: str | None = None
    safesearch: str | None = None
    time: str | None = None

    # Extensions
    proxy: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    use_browser: bool = False
    headless: bool = True
    bypass_proxy_for_driver: bool = True

    # ── override endpoints ───────────────────────────────────────────────
    def _search_text(self, query: str, max_results: int) -> List[Dict[str, str]]:
        """
        Unified dispatcher for text search with multi‑level fallback.
        Thread-safe: uses lock for inter-search delay coordination.
        """
        # Validate query early to prevent cryptic errors downstream
        if not query or not query.strip():
            logger.warning("Empty query received in _search_text - returning empty results")
            return []

        global _last_search_time
        cfg = _default_config

        # Apply inter-search delay to avoid rate limiting (thread-safe)
        # Uses humanized timing for less predictable patterns
        # HIGH FIX: Compute wait time under lock, then release before sleeping
        # This prevents synchronized "burst then stall" patterns across threads
        wait_time = 0
        with _search_lock:
            if cfg.inter_search_delay > 0:
                elapsed = time.time() - _last_search_time
                base_delay = cfg.inter_search_delay
                humanized_delay = _humanize_delay(base_delay, cfg)
                if elapsed < humanized_delay:
                    wait_time = humanized_delay - elapsed

        # Sleep OUTSIDE the lock to allow other threads to proceed
        if wait_time > 0:
            logger.debug("Inter-search delay: waiting %.2fs (humanized from %.2fs)", wait_time, base_delay)
            time.sleep(wait_time)

        # Update last search time under lock
        with _search_lock:
            _last_search_time = time.time()

        # ────────────────────────────────────────────────────────────────────────
        # Proactive Anti-Detection: Session reset and circuit rotation
        # ────────────────────────────────────────────────────────────────────────
        _maybe_session_reset()
        _maybe_proactive_rotation(self.proxy)
        exit_ip = _ensure_clean_exit(self.proxy)
        if exit_ip:
            logger.debug("Exit selected for search: %s", exit_ip)

        def _reselect_exit(reason: str) -> None:
            """Force selection of a new exit before retrying."""
            nonlocal exit_ip
            exit_ip = _ensure_clean_exit(self.proxy)
            if exit_ip:
                logger.info("Exit %s selected after %s", exit_ip, reason)
            else:
                logger.debug("No exit selected after %s", reason)

        # Check if current proxy is blocklisted due to recent CAPTCHA
        if _is_proxy_blocked(self.proxy):
            logger.info("Proxy %s is blocklisted - forcing circuit rotation before search",
                       self.proxy or "direct")
            if self.proxy and "socks" in self.proxy.lower():
                if _rotate_tor_circuit(force=True, proxy=self.proxy):
                    logger.info("Circuit rotated successfully to clear blocklist")
                    # Periodic cleanup of expired blocks
                    _cleanup_expired_blocks()
                _reselect_exit("blocklist")

        # tier 0 - curl_cffi
        # ────────────────────────────────────────────────────────────────────────
        # Tier 0 — curl_cffi (libcurl + BoringSSL with browser impersonation)
        # Requires: `pip install curl_cffi` (https://github.com/lexiforest/curl_cffi)
        # Notes:
        #   • Uses libcurl under the hood — different TLS stack from PRIMP (Rust/reqwest).
        #   • Stateless: no session reuse across calls for anonymity.
        #   • Supports browser impersonation via `impersonate=` parameter.
        #   • Includes retry logic for CAPTCHA with delay and impersonation rotation.
        logger.debug("curl_cffi starting search for: %s", query[:60])

        _curl_impersonations = [
            "chrome131",
            "chrome130",
            "firefox133",
            "safari18_0",
            "edge131",
        ]
        _curl_max_retries = cfg.max_retries
        _curl_retry_delay = cfg.retry_delay

        for _curl_attempt in range(_curl_max_retries):
            try:
                from curl_cffi import requests as curl_requests

                # Map DuckDuckGo params
                _params = {"q": query}
                if self.safesearch:
                    _ss = str(self.safesearch).lower()
                    _params["kp"] = {"off": "-1", "moderate": "0", "safe": "1", "strict": "1"}.get(_ss, "0")
                if self.time:
                    _params["df"] = self.time
                if self.region:
                    _params["kl"] = self.region

                # Rotate impersonation on retries
                _curl_imp = _curl_impersonations[_curl_attempt % len(_curl_impersonations)]

                # Proxy handling — always use proxy if configured
                _curl_proxy = self.proxy
                if _curl_attempt >= 2 and self.proxy and cfg.allow_direct_fallback:
                    _curl_proxy = None
                    logger.warning("ANONYMITY WARNING: curl_cffi falling back to DIRECT IP (no proxy) - "
                                  "your real IP will be exposed. Set allow_direct_fallback=False to disable.")
                if _curl_attempt > 0:
                    proxy_status = "with proxy" if _curl_proxy else "DIRECT IP (no proxy)"
                    logger.info("curl_cffi retry %d/%d with impersonation: %s, %s",
                               _curl_attempt, _curl_max_retries - 1, _curl_imp, proxy_status)

                _curl_proxies = {"https": _curl_proxy, "http": _curl_proxy} if _curl_proxy else None

                _curl_resp = curl_requests.get(
                    "https://html.duckduckgo.com/html",
                    params=_params,
                    impersonate=_curl_imp,
                    proxies=_curl_proxies,
                    timeout=cfg.timeout,
                )
                logger.debug("curl_cffi response status: %d, length: %d",
                            _curl_resp.status_code, len(_curl_resp.text) if _curl_resp.text else 0)

                if 200 <= _curl_resp.status_code < 300 and _curl_resp.text:
                    from bs4 import BeautifulSoup
                    from urllib.parse import urlparse, parse_qs, unquote

                    _soup = BeautifulSoup(_curl_resp.text, "html.parser")
                    _links = _soup.select("a.result__a")
                    _snips = _soup.select("div.result__snippet, a.result__snippet")
                    logger.debug("curl_cffi found %d links, %d snippets", len(_links), len(_snips))

                    # Check for CAPTCHA or rate limiting
                    if len(_links) == 0:
                        _page_text = _curl_resp.text.lower()
                        if _is_captcha_page(_page_text, has_results=False):
                            logger.warning("curl_cffi CAPTCHA detected on attempt %d (proxy=%s, query=%s...)",
                                         _curl_attempt + 1, self.proxy or "direct", query[:30])
                            _record_captcha_hit(self.proxy)
                            _mark_exit_bad(self.proxy, reason="captcha_curl_cffi")
                            if _curl_attempt < _curl_max_retries - 1:
                                if self.proxy and "socks" in self.proxy.lower():
                                    if _rotate_tor_circuit(force=True, proxy=self.proxy):
                                        logger.info("Tor circuit rotated (forced) - new exit node for curl_cffi retry")
                                    else:
                                        logger.warning("Tor rotation FAILED - check control port auth")
                                _reselect_exit("curl_cffi CAPTCHA")
                                _humanized_retry = _humanize_delay(_curl_retry_delay, cfg)
                                logger.info("curl_cffi waiting %.1fs before retry (humanized)...", _humanized_retry)
                                time.sleep(_humanized_retry)
                            _curl_retry_delay *= cfg.backoff_multiplier
                            continue
                        elif "rate" in _page_text and "limit" in _page_text:
                            logger.warning("curl_cffi rate limiting detected")
                            _mark_exit_bad(self.proxy, reason="rate_limit_curl_cffi")
                            if self.proxy and "socks" in self.proxy.lower():
                                if _rotate_tor_circuit(force=True, proxy=self.proxy):
                                    logger.info("Tor circuit rotated (forced) after rate limit")
                                else:
                                    logger.warning("Tor rotation FAILED after rate limit")
                            _reselect_exit("curl_cffi rate-limit")
                        else:
                            logger.debug("curl_cffi no results - page title: %s",
                                       _soup.title.string if _soup.title else 'N/A')

                    _out = []
                    _limit = int(max_results or cfg.max_results)
                    for i, a in enumerate(_links[:_limit], 1):
                        _raw = a.get("href", "")
                        _parsed = urlparse(_raw)
                        _real = unquote(parse_qs(_parsed.query).get("uddg", [_raw])[0])
                        _real = _canonicalize_url(_real) or _real
                        if _is_noise_source_url(_real):
                            continue

                        _snip = ""
                        if i - 1 < len(_snips):
                            try:
                                _snip = _snips[i - 1].get_text(strip=True)
                            except (AttributeError, IndexError) as e:
                                logger.debug("Snippet extraction failed for result %d: %s", i, e)
                        _out.append(
                            {
                                "id": i,
                                "title": a.get_text(strip=True),
                                "href": _raw,
                                "body": f"__START_OF_SOURCE {i}__ <CONTENT> {_snip} </CONTENT> <URL> {_real} </URL> __END_OF_SOURCE {i}__",
                                "_tier": "curl_cffi",
                            }
                        )
                    if _out:
                        _out = _rerank_search_results(query, _out, max_results=_limit)
                        normalized_out = _normalize_tool_result_rows(_out, max_results=_limit)
                        if normalized_out:
                            _out = normalized_out
                        logger.info("curl_cffi SUCCESS: returning %d results", len(_out))
                        _mark_exit_good(self.proxy)
                        return _out
                    else:
                        logger.debug("curl_cffi no output generated, falling through to next tier")
                        break  # Don't retry if we got a valid response with no results

            except Exception as _curl_exc:
                logger.warning("curl_cffi exception on attempt %d: %s: %s",
                              _curl_attempt + 1, type(_curl_exc).__name__, _curl_exc)
                if _curl_attempt < _curl_max_retries - 1:
                    _humanized_retry = _humanize_delay(_curl_retry_delay, cfg)
                    time.sleep(_humanized_retry)
                    _curl_retry_delay *= cfg.backoff_multiplier
                else:
                    logger.debug("curl_cffi tier failed after %d attempts: %s", _curl_max_retries, _curl_exc)
        # End tier 0 (curl_cffi)

        # Inter-tier rotation: get fresh exit before trying PRIMP
        if self.proxy and "socks" in self.proxy.lower():
            logger.info("curl_cffi tier exhausted - rotating circuit before PRIMP fallback")
            if _rotate_tor_circuit(force=True, proxy=self.proxy):
                logger.info("Circuit rotated successfully before PRIMP tier")
            _reselect_exit("tier_fallback_primp")

        # tier 1 - primp
        # ────────────────────────────────────────────────────────────────────────
        # Tier 1 — PRIMP (fast HTTP client with browser impersonation, fallback)
        # Requires: `pip install -U primp` (https://github.com/deedy5/primp)
        # Notes for massive parallel runs:
        #   • We create a fresh client per call (cookie_store=False) and close it immediately.
        #   • No global env mutation; proxy is passed directly from `self.proxy`.
        #   • Optional overrides via env: PRIMP_IMPERSONATE, PRIMP_IMPERSONATE_OS.
        #   • Includes retry logic for CAPTCHA with delay and impersonation rotation.
        logger.debug("PRIMP starting search for: %s", query[:60])

        # Impersonation options to rotate through on CAPTCHA
        _impersonations = [
            ("chrome_131", "windows"),
            ("chrome_130", "macos"),
            ("firefox_133", "windows"),
            ("safari_18", "macos"),
            ("edge_131", "windows"),
        ]
        _max_retries = cfg.max_retries
        _retry_delay = cfg.retry_delay

        for _attempt in range(_max_retries):
            try:
                import primp  # lightweight, precompiled wheels available

                # Map DuckDuckGo params if provided (best-effort parity with ddgs)
                _params = {"q": query}
                if self.safesearch:
                    _ss = str(self.safesearch).lower()
                    _params["kp"] = {"off": "-1", "moderate": "0", "safe": "1", "strict": "1"}.get(_ss, "0")
                if self.time:
                    _params["df"] = self.time
                if self.region:
                    _params["kl"] = self.region

                # Rotate impersonation on retries
                _imp, _imp_os = _impersonations[_attempt % len(_impersonations)]

                # CRITICAL FIX: Direct IP fallback is now opt-in to preserve anonymity
                # Always use proxy if configured, unless allow_direct_fallback is True
                _use_proxy = self.proxy  # Default: always use proxy
                if _attempt >= 2 and self.proxy and cfg.allow_direct_fallback:
                    _use_proxy = None
                    logger.warning("ANONYMITY WARNING: PRIMP falling back to DIRECT IP (no proxy) - "
                                  "your real IP will be exposed. Set allow_direct_fallback=False to disable.")
                if _attempt > 0:
                    proxy_status = "with proxy" if _use_proxy else "DIRECT IP (no proxy)"
                    logger.info("PRIMP retry %d/%d with impersonation: %s/%s, %s",
                               _attempt, _max_retries - 1, _imp, _imp_os, proxy_status)

                _client = primp.Client(
                    impersonate=_imp,
                    impersonate_os=_imp_os,
                    proxy=_use_proxy,
                    timeout=cfg.timeout,
                    cookie_store=False,
                    follow_redirects=True,
                )
                try:
                    if self.headers:
                        try:
                            _client.headers_update(self.headers)
                        except AttributeError:
                            logger.debug("Client does not support headers_update")
                        except Exception as e:
                            logger.debug("Header update failed: %s: %s", type(e).__name__, e)

                    _resp = _client.get("https://html.duckduckgo.com/html", params=_params, timeout=cfg.timeout)
                    logger.debug("PRIMP response status: %d, length: %d",
                                _resp.status_code, len(_resp.text) if _resp.text else 0)

                    if 200 <= _resp.status_code < 300 and _resp.text:
                        from bs4 import BeautifulSoup
                        from urllib.parse import urlparse, parse_qs, unquote

                        _soup = BeautifulSoup(_resp.text, "html.parser")
                        _links = _soup.select("a.result__a")
                        _snips = _soup.select("div.result__snippet, a.result__snippet")
                        logger.debug("PRIMP found %d links, %d snippets", len(_links), len(_snips))

                        # Check for CAPTCHA or rate limiting
                        if len(_links) == 0:
                            _page_text = _resp.text.lower()
                            # MEDIUM FIX: Use more specific CAPTCHA detection
                            if _is_captcha_page(_page_text, has_results=False):
                                logger.warning("PRIMP CAPTCHA detected on attempt %d (proxy=%s, query=%s...)",
                                             _attempt + 1, self.proxy or "direct", query[:30])
                                # Record this proxy hit for blocklist tracking
                                _record_captcha_hit(self.proxy)
                                _mark_exit_bad(self.proxy, reason="captcha_primp")
                                if _attempt < _max_retries - 1:
                                    # Rotate Tor circuit to get new exit node IP (force=True to bypass rate limit)
                                    if self.proxy and "socks" in self.proxy.lower():
                                        if _rotate_tor_circuit(force=True, proxy=self.proxy):
                                            logger.info("Tor circuit rotated (forced) - new exit node for retry")
                                        else:
                                            logger.warning("Tor rotation FAILED - check control port auth")
                                    _reselect_exit("PRIMP CAPTCHA")
                                    _humanized_retry = _humanize_delay(_retry_delay, cfg)
                                    logger.info("PRIMP waiting %.1fs before retry (humanized)...", _humanized_retry)
                                    time.sleep(_humanized_retry)
                                _retry_delay *= cfg.backoff_multiplier
                                continue  # retry with different impersonation + new circuit
                            elif "rate" in _page_text and "limit" in _page_text:
                                logger.warning("PRIMP rate limiting detected")
                                _mark_exit_bad(self.proxy, reason="rate_limit_primp")
                                # Force rotation on rate limit to escape blocked exit
                                if self.proxy and "socks" in self.proxy.lower():
                                    if _rotate_tor_circuit(force=True, proxy=self.proxy):
                                        logger.info("Tor circuit rotated (forced) after rate limit")
                                    else:
                                        logger.warning("Tor rotation FAILED after rate limit")
                                _reselect_exit("PRIMP rate-limit")
                            else:
                                logger.debug("PRIMP no results - page title: %s",
                                           _soup.title.string if _soup.title else 'N/A')

                        _out = []
                        _limit = int(max_results or cfg.max_results)
                        for i, a in enumerate(_links[:_limit], 1):
                            _raw = a.get("href", "")
                            _parsed = urlparse(_raw)
                            _real = unquote(parse_qs(_parsed.query).get("uddg", [_raw])[0])
                            _real = _canonicalize_url(_real) or _real
                            if _is_noise_source_url(_real):
                                continue

                            _snip = ""
                            if i - 1 < len(_snips):
                                try:
                                    _snip = _snips[i - 1].get_text(strip=True)
                                except (AttributeError, IndexError) as e:
                                    logger.debug("Snippet extraction failed for result %d: %s", i, e)
                            _out.append(
                                {
                                    "id": i,
                                    "title": a.get_text(strip=True),
                                    "href": _raw,
                                    "body": f"__START_OF_SOURCE {i}__ <CONTENT> {_snip} </CONTENT> <URL> {_real} </URL> __END_OF_SOURCE {i}__",
                                    "_tier": "primp",
                                }
                            )
                        if _out:
                            _out = _rerank_search_results(query, _out, max_results=_limit)
                            normalized_out = _normalize_tool_result_rows(_out, max_results=_limit)
                            if normalized_out:
                                _out = normalized_out
                            logger.info("PRIMP SUCCESS: returning %d results", len(_out))
                            _mark_exit_good(self.proxy)
                            return _out
                        else:
                            logger.debug("PRIMP no output generated, falling through to next tier")
                            break  # Don't retry if we got a valid response with no results
                finally:
                    try:
                        close_fn = getattr(_client, "close", None)
                        if callable(close_fn):
                            close_fn()
                    except (OSError, ConnectionError) as e:
                        logger.debug("PRIMP client cleanup failed: %s", e)
                    except Exception as e:
                        logger.warning("Unexpected error closing PRIMP client: %s: %s",
                                       type(e).__name__, e)
                    finally:
                        del _client

            except Exception as _primp_exc:
                logger.warning("PRIMP exception on attempt %d: %s: %s",
                              _attempt + 1, type(_primp_exc).__name__, _primp_exc)
                if _attempt < _max_retries - 1:
                    _humanized_retry = _humanize_delay(_retry_delay, cfg)
                    time.sleep(_humanized_retry)
                    _retry_delay *= cfg.backoff_multiplier
                else:
                    logger.debug("PRIMP tier failed after %d attempts: %s", _max_retries, _primp_exc)
        # End tier 1 (primp)

        # Inter-tier rotation: get fresh exit before trying Selenium
        if self.proxy and "socks" in self.proxy.lower():
            logger.info("PRIMP tier exhausted - rotating circuit before Selenium fallback")
            if _rotate_tor_circuit(force=True, proxy=self.proxy):
                logger.info("Circuit rotated successfully before Selenium tier")
            _reselect_exit("tier_fallback")

        # Tier 2 – Selenium
        if self.use_browser:
            try:
                return _browser_search(
                    query,
                    max_results=max_results,
                    proxy=self.proxy,
                    headers=self.headers,
                    headless=self.headless,
                    bypass_proxy_for_driver=self.bypass_proxy_for_driver,
                )
            except WebDriverException as exc:
                exc_name = type(exc).__name__
                exc_msg = getattr(exc, "msg", None) or str(exc) or ""
                first_line = exc_msg.strip().splitlines()[0] if exc_msg else ""
                logger.warning("Browser tier failed (%s): %s", exc_name, first_line or "no details")
                logger.debug("Browser tier exception: %s", exc_msg)
                logger.debug("Browser tier traceback: %s", traceback.format_exc())

        logger.debug("Selenium tier complete, trying next tier")

        # Inter-tier rotation: get fresh exit before DDGS
        if self.proxy and "socks" in self.proxy.lower():
            logger.info("Selenium tier exhausted - rotating circuit before DDGS fallback")
            if _rotate_tor_circuit(force=True, proxy=self.proxy):
                logger.info("Circuit rotated successfully before DDGS tier")
            _reselect_exit("tier_fallback_ddgs")

        # Tier 3 – ddgs HTTP API
        logger.debug("Trying DDGS tier...")
        try:
            ddgs_kwargs = _build_ddgs_kwargs(
                max_results=max_results,
                region=self.region,
                safesearch=self.safesearch,
                timelimit=self.time,
            )
            ddgs_results = _with_ddgs(
                self.proxy,
                self.headers,
                lambda d: list(
                    d.text(
                        query,
                        **ddgs_kwargs,
                    )
                ),
            )
            # Add tier info to each result
            for item in ddgs_results:
                item["_tier"] = "ddgs"
            ddgs_results = _rerank_search_results(query, ddgs_results, max_results=max_results)
            normalized_ddgs = _normalize_tool_result_rows(ddgs_results, max_results=max_results)
            if normalized_ddgs:
                ddgs_results = normalized_ddgs
            _mark_exit_good(self.proxy)
            return ddgs_results
        except DDGSException as exc:
            logger.warning("DDGS tier failed (%s); falling back to raw scrape.", exc)
            if self.proxy and "socks" in self.proxy.lower():
                logger.info("DDGS tier exhausted - rotating circuit before requests fallback")
                _rotate_tor_circuit(force=True, proxy=self.proxy)
                _reselect_exit("tier_fallback_requests")

        # Tier 4 – raw Requests scrape
        logger.debug("Trying requests scrape tier...")
        fallback_results = _requests_scrape(
            query,
            max_results=max_results,
            proxy=self.proxy,
            headers=self.headers,
        )
        if fallback_results:
            return fallback_results

        relaxed_query = _relaxed_entity_query(query)
        if relaxed_query and relaxed_query != query:
            logger.info(
                "No usable results for query; retrying with relaxed entity query: %s",
                relaxed_query,
            )
            relaxed_results = _requests_scrape(
                relaxed_query,
                max_results=max_results,
                proxy=self.proxy,
                headers=self.headers,
            )
            if relaxed_results:
                return relaxed_results

        return fallback_results

    # LangChain calls the four "_ddgs_*" methods – just delegate.
    def _ddgs_text(self, query: str, **kw):
        return self._search_text(query, kw.get("max_results", self.k))

    def _ddgs_images(self, query: str, **kw):
        ddgs_kwargs = _build_ddgs_kwargs(
            max_results=kw.get("max_results", self.k),
            region=self.region,
            safesearch=self.safesearch,
            timelimit=self.time,
        )
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.images(
                    query,
                    **ddgs_kwargs,
                )
            ),
        )

    def _ddgs_videos(self, query: str, **kw):
        ddgs_kwargs = _build_ddgs_kwargs(
            max_results=kw.get("max_results", self.k),
            region=self.region,
            safesearch=self.safesearch,
            timelimit=self.time,
        )
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.videos(
                    query,
                    **ddgs_kwargs,
                )
            ),
        )

    def _ddgs_news(self, query: str, **kw):
        ddgs_kwargs = _build_ddgs_kwargs(
            max_results=kw.get("max_results", self.k),
            region=self.region,
            safesearch=self.safesearch,
            timelimit=self.time,
        )
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.news(
                    query,
                    **ddgs_kwargs,
                )
            ),
        )

class PatchedDuckDuckGoSearchRun(DuckDuckGoSearchRun):
    """LangChain *Tool* wired to the safe API wrapper above."""
    api_wrapper: PatchedDuckDuckGoSearchAPIWrapper = Field(
        default_factory=PatchedDuckDuckGoSearchAPIWrapper
    )


# Semantic aliases for code that always picks the browser path
BrowserDuckDuckGoSearchAPIWrapper = PatchedDuckDuckGoSearchAPIWrapper
BrowserDuckDuckGoSearchRun = PatchedDuckDuckGoSearchRun


# Autonomous Memory Folding Agent (DeepAgent-style)

# ---------------------------------------------------------------------------
# Search ranking and canonicalization helpers (moved from mixed graph module)
# ---------------------------------------------------------------------------
_RETRIEVE_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "to", "of", "in", "on", "for", "with",
    "is", "are", "was", "were", "be", "been", "being", "it", "this", "that",
    "i", "you", "we", "they", "he", "she", "them", "his", "her", "our", "your",
    "as", "at", "by", "from", "about", "into", "over", "after", "before", "than",
}


_TASK_HINT_LABEL_RE = re.compile(
    r"(?im)^"
    r"\s*(?:[\-\*\u2022]\s*)?"
    r"([a-z][a-z0-9 _/\-]{1,64})\s*:"
    r"\s*(.+)$"
)
_TASK_HINT_NAME_KEY_RE = re.compile(
    r"\b(name|target|subject|entity|person|individual|candidate)\b",
    re.I,
)
_TASK_HINT_QUOTED_RE = re.compile(r'"([^"]+)"')


def _extract_profile_hints(text: str) -> Dict[str, Any]:
    """Extract lightweight name/context hints from task text."""
    out: Dict[str, Any] = {
        "name_tokens": set(),
        "context_tokens": set(),
        "raw_hint_lines": [],
    }
    if not text:
        return out

    for quoted in _TASK_HINT_QUOTED_RE.findall(text):
        normalized = _normalize_match_text(quoted)
        if not normalized:
            continue
        quote_tokens = [t for t in normalized.split() if len(t) >= 2]
        if len(quote_tokens) >= 2:
            out["name_tokens"].update(quote_tokens)
        else:
            out["context_tokens"].update(quote_tokens)

    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = _TASK_HINT_LABEL_RE.match(line)
        if not m:
            continue
        label = str(m.group(1) or "").strip().lower()
        value = str(m.group(2) or "").strip()
        if not value:
            continue
        bucket = "name" if _TASK_HINT_NAME_KEY_RE.search(label) else "context"
        tokens = _tokenize_for_retrieval(value)
        if not tokens:
            continue
        if bucket == "name":
            out["name_tokens"].update(tokens)
        else:
            out["context_tokens"].update(tokens)
        out["raw_hint_lines"].append(f"{label}:{value}")

    if out["name_tokens"]:
        return out

    fallback = None
    for phrase in re.findall(
        r"\b([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑ]+(?:\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑ]+){1,3})",
        text,
    ):
        normalized = _normalize_match_text(phrase)
        words = normalized.split()
        if 2 <= len(words) <= 4 and not any(w in _QUERY_CONTEXT_NOISE_TOKENS for w in words):
            fallback = set(words)
            break
    if fallback:
        out["name_tokens"].update(fallback)

    return out


_RETRIEVE_TRIGGERS = (
    "earlier", "previous", "before", "we discussed", "you said", "remind", "recall",
    "what did", "from our conversation", "from the conversation", "as mentioned",
)

_QUERY_CONTEXT_NOISE_TOKENS = {
    "bio", "biography", "profile", "information", "details", "source", "sources",
    "official", "record", "records", "field", "fields", "query",
    "pdf", "cv", "resume", "curriculum",
    # Instruction/formatting words should never drive entity matching.
    "answer", "format", "json", "object", "only", "output", "respond",
    "response", "return", "schema", "strict", "valid",
}

_LOW_SIGNAL_HOST_FRAGMENTS = (
    "brainly.",
    "pinterest.",
    "facebook.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "sites.google.com",
)


def _tokenize_for_retrieval(text: str) -> set:
    if not text:
        return set()
    t = re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()
    if not t:
        return set()
    tokens = [w for w in t.split() if len(w) >= 3 and w not in _RETRIEVE_STOPWORDS]
    return set(tokens)


def _query_entity_tokens(query: Any, *, max_tokens: int = 8) -> List[str]:
    """Extract likely entity-bearing tokens from a free-form search query."""
    hints = _extract_profile_hints(str(query or ""))
    hint_name_tokens = [tok for tok in (hints.get("name_tokens") or set())]
    hint_context_tokens = [tok for tok in (hints.get("context_tokens") or set())]
    seen: set = set()
    out: List[str] = []

    for token in list(hint_name_tokens) + list(hint_context_tokens):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= max_tokens:
            return out[: max(1, int(max_tokens))]

    text = str(query or "")
    if not text.strip():
        return []

    candidates: List[str] = []

    # Prefer explicitly quoted entities when available.
    for quoted in re.findall(r'"([^"]+)"', text):
        normalized = _normalize_match_text(quoted)
        if not normalized:
            continue
        for token in normalized.split():
            if len(token) < 3 or token in _QUERY_CONTEXT_NOISE_TOKENS:
                continue
            candidates.append(token)

    # Fallback: title-cased words usually capture person names and places.
    if not candidates:
        for raw_token in re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'-]{2,}", text):
            if not raw_token[:1].isupper():
                continue
            token = _normalize_match_text(raw_token)
            if len(token) < 3 or token in _QUERY_CONTEXT_NOISE_TOKENS:
                continue
            candidates.append(token)

    # Last fallback: non-stopword normalized query tokens.
    if not candidates:
        for token in _tokenize_for_retrieval(_normalize_match_text(text)):
            if token in _QUERY_CONTEXT_NOISE_TOKENS:
                continue
            candidates.append(token)

    deduped: List[str] = []
    for token in candidates:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
        if len(deduped) >= max(1, int(max_tokens)):
            break
    return deduped


def _query_focus_tokens(query: Any, *, max_tokens: int = 6) -> List[str]:
    """Extract high-signal entity tokens, preferring explicit name-like spans."""
    text = str(query or "").strip()
    if not text:
        return []

    focus_tokens: List[str] = []

    for quoted in re.findall(r'"([^"]+)"', text):
        normalized = _normalize_match_text(quoted)
        words = [
            token
            for token in normalized.split()
            if len(token) >= 3 and token not in _QUERY_CONTEXT_NOISE_TOKENS
        ]
        if len(words) >= 2:
            focus_tokens = words
            break

    if not focus_tokens:
        name_like_phrases = re.findall(
            r"\b([A-ZÀ-ÿ][A-Za-zÀ-ÿ'’-]+(?:\s+[A-ZÀ-ÿ][A-Za-zÀ-ÿ'’-]+){1,4})\b",
            text,
        )
        for phrase in name_like_phrases:
            normalized = _normalize_match_text(phrase)
            words = [
                token
                for token in normalized.split()
                if len(token) >= 3 and token not in _QUERY_CONTEXT_NOISE_TOKENS
            ]
            if len(words) >= 2:
                focus_tokens = words
                break

    if not focus_tokens:
        focus_tokens = _query_entity_tokens(text, max_tokens=max_tokens)

    deduped = []
    seen = set()
    for token in focus_tokens:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
        if len(deduped) >= max(1, int(max_tokens)):
            break
    return deduped


def _relaxed_entity_query(query: Any) -> str:
    """Build a fallback query centered on the main quoted/name entity."""
    text = str(query or "").strip()
    if not text:
        return ""

    quoted_candidates = [q.strip() for q in re.findall(r'"([^"]+)"', text) if q and q.strip()]
    entity = ""
    for cand in quoted_candidates:
        if len(cand.split()) >= 2:
            entity = cand
            break
    if not entity:
        tokens = _query_entity_tokens(text, max_tokens=4)
        if tokens:
            entity = " ".join(tokens)
    if not entity:
        return ""

    return f"\"{entity}\""


def _normalize_tool_result_rows(
    rows: Any,
    *,
    max_results: Any = None,
) -> List[Dict[str, Any]]:
    """Normalize search rows so every item carries structured source+URL text."""
    if not isinstance(rows, list) or not rows:
        return []

    limit = len(rows)
    try:
        if max_results is not None:
            limit = max(1, int(max_results))
    except Exception:
        limit = len(rows)

    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        item = dict(row)
        raw_href = item.get("href") or item.get("link") or item.get("url")
        href = _canonicalize_url(raw_href) or str(raw_href or "").strip()
        if not href or _is_noise_source_url(href):
            continue

        title = str(item.get("title") or "").strip()
        raw_body = str(item.get("body") or item.get("snippet") or item.get("content") or "").strip()
        if not raw_body:
            raw_body = title or href

        if "__START_OF_SOURCE" in raw_body and "<URL>" in raw_body:
            body = raw_body
        else:
            snippet = re.sub(r"\s+", " ", raw_body)
            snippet = re.sub(r"[<>]", " ", snippet).strip()
            if title:
                snippet = f"Title: {title}. {snippet}"
            source_id = len(out) + 1
            body = (
                f"__START_OF_SOURCE {source_id}__ "
                f"<CONTENT> {snippet} </CONTENT> "
                f"<URL> {href} </URL> "
                f"__END_OF_SOURCE {source_id}__"
            )

        item["href"] = href
        item["body"] = body
        item["id"] = len(out) + 1
        out.append(item)
        if len(out) >= limit:
            break

    return out


def _rerank_search_results(
    query: str,
    results: Any,
    *,
    max_results: Any = None,
    min_results: int = 3,
) -> List[Dict[str, Any]]:
    """Prefer search snippets that overlap with the query entity/context tokens."""
    if not isinstance(results, list) or not results:
        return []

    limit = len(results)
    try:
        if max_results is not None:
            limit = max(1, int(max_results))
    except Exception:
        limit = len(results)

    query_tokens = _tokenize_for_retrieval(query)
    entity_tokens = _query_entity_tokens(query)
    if not query_tokens:
        # Keep stable IDs when no lexical signal is available.
        out = []
        for idx, row in enumerate(results[:limit], 1):
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["id"] = idx
            out.append(item)
        return out

    normalized_query = _normalize_match_text(query)
    query_words = [
        tok for tok in normalized_query.split()
        if len(tok) >= 3 and tok not in _RETRIEVE_STOPWORDS
    ]
    query_phrases = set()
    for i in range(max(0, len(query_words) - 1)):
        query_phrases.add(f"{query_words[i]} {query_words[i + 1]}")
    if len(query_words) >= 3:
        query_phrases.add(" ".join(query_words[: min(4, len(query_words))]))

    scored: List[Any] = []
    for idx, row in enumerate(results):
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "")
        body = str(row.get("body") or row.get("snippet") or row.get("content") or "")
        href = str(row.get("href") or row.get("url") or row.get("link") or "")
        haystack = f"{title} {body}"
        doc_tokens = _tokenize_for_retrieval(haystack)
        overlap = len(query_tokens & doc_tokens)
        haystack_norm = _normalize_match_text(haystack)
        phrase_hits = sum(1 for phrase in query_phrases if phrase and phrase in haystack_norm)
        href_norm = _normalize_match_text(href)
        href_hits = sum(1 for token in list(query_tokens)[:8] if token in href_norm)
        entity_hits = sum(
            1 for token in entity_tokens
            if token and (token in haystack_norm or token in href_norm)
        )
        strong_entity_match = bool(entity_tokens) and entity_hits >= min(2, len(entity_tokens))
        trusted_domain_bonus = 0.0
        href_lower = href.lower()
        try:
            host = str(urlparse(href).hostname or "").lower()
        except Exception:
            host = ""
        is_public_institution_host = (
            bool(host)
            and (
                host.endswith(".gov")
                or ".gov." in host
                or host.endswith(".gob")
                or ".gob." in host
                or "parliament" in host
                or "senate" in host
                or "congress" in host
                or "assembly" in host
            )
        )
        if "wikipedia.org" in href_lower or is_public_institution_host:
            trusted_domain_bonus = 1.25
        low_signal_penalty = 0.0
        if any(fragment in host for fragment in _LOW_SIGNAL_HOST_FRAGMENTS):
            low_signal_penalty = 1.75
        score = (
            float(overlap)
            + (1.5 * float(phrase_hits))
            + (0.25 * float(href_hits))
            + (1.1 * float(entity_hits))
            + (2.0 if strong_entity_match else 0.0)
            + trusted_domain_bonus
            - low_signal_penalty
        )
        scored.append((score, overlap, entity_hits, trusted_domain_bonus, idx, row))

    if not scored:
        return []

    scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[4]))

    chosen: List[Any] = []
    seen_idx: set = set()
    min_keep = max(1, min(int(min_results), limit))
    strict_entity_mode = bool(entity_tokens)
    if strict_entity_mode:
        # When user query carries explicit entity tokens (e.g., quoted person name),
        # keep the result set tight to avoid flooding the prompt with off-topic pages.
        min_keep = 1

    def _append_candidate(candidate: Any) -> None:
        orig_idx = int(candidate[4])
        if orig_idx in seen_idx:
            return
        chosen.append(candidate)
        seen_idx.add(orig_idx)

    def _candidate_is_low_signal(candidate: Any) -> bool:
        row = candidate[5] if len(candidate) >= 6 else None
        if not isinstance(row, dict):
            return False
        href = str(row.get("href") or row.get("url") or row.get("link") or "")
        try:
            host = str(urlparse(href).hostname or "").lower()
        except Exception:
            host = ""
        return any(fragment in host for fragment in _LOW_SIGNAL_HOST_FRAGMENTS)

    has_entity_signal = any(candidate[2] > 0 for candidate in scored)
    has_trusted_signal = any(candidate[3] > 0 for candidate in scored)

    if strict_entity_mode and not has_entity_signal and not has_trusted_signal:
        # No lexical match to the queried entity anywhere in returned rows.
        # Return empty so the caller can retry with a relaxed fallback query.
        return []
    else:
        if strict_entity_mode:
            # In strict entity mode, prioritize trusted/government sources first.
            for candidate in scored:
                _, _, _, trusted_bonus, _, _ = candidate
                if trusted_bonus > 0:
                    _append_candidate(candidate)
                    if len(chosen) >= limit:
                        break
            # Then add non-social pages that still match entity tokens.
            if len(chosen) < limit:
                for candidate in scored:
                    _, _, entity_hits, _, _, _ = candidate
                    if entity_hits > 0 and not _candidate_is_low_signal(candidate):
                        _append_candidate(candidate)
                        if len(chosen) >= limit:
                            break
            # Final strict backfill (at most one result required).
            if len(chosen) < min_keep:
                for candidate in scored:
                    _append_candidate(candidate)
                    if len(chosen) >= min_keep:
                        break
        else:
            # Prefer candidates with explicit entity support or trusted domains.
            for candidate in scored:
                _, _, entity_hits, trusted_bonus, _, _ = candidate
                if entity_hits > 0 or trusted_bonus > 0:
                    _append_candidate(candidate)
                    if len(chosen) >= limit:
                        break

            # Backfill if focused filtering got too strict.
            if len(chosen) < min_keep:
                for candidate in scored:
                    _append_candidate(candidate)
                    if len(chosen) >= min_keep:
                        break

            if len(chosen) < limit:
                for candidate in scored:
                    _append_candidate(candidate)
                    if len(chosen) >= limit:
                        break

    reranked: List[Dict[str, Any]] = []
    for new_id, (_, _, _, _, _, row) in enumerate(chosen, 1):
        item = dict(row)
        body = item.get("body")
        if isinstance(body, str) and "__START_OF_SOURCE" in body:
            body = re.sub(
                r"__START_OF_SOURCE\s+\d+__",
                f"__START_OF_SOURCE {new_id}__",
                body,
            )
            body = re.sub(
                r"__END_OF_SOURCE\s+\d+__",
                f"__END_OF_SOURCE {new_id}__",
                body,
            )
            item["body"] = body
        item["id"] = new_id
        reranked.append(item)
    return reranked


_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "source",
    "spm",
    "utm_campaign",
    "utm_content",
    "utm_id",
    "utm_medium",
    "utm_name",
    "utm_source",
    "utm_term",
}
_PREFER_HTTPS_URLS = _env_flag("ASA_PREFER_HTTPS_URLS", default=True)
_AUTO_OPENWEBPAGE_PRIMARY_SOURCE = _env_flag(
    "ASA_AUTO_OPENWEBPAGE_PRIMARY_SOURCE",
    default=True,
)
_AUTO_OPENWEBPAGE_SELECTOR_MODE = (
    str(os.getenv("ASA_AUTO_OPENWEBPAGE_SELECTOR_MODE", "heuristic") or "heuristic")
    .strip()
    .lower()
)
if _AUTO_OPENWEBPAGE_SELECTOR_MODE in {"model", "ai"}:
    _AUTO_OPENWEBPAGE_SELECTOR_MODE = "llm"
if _AUTO_OPENWEBPAGE_SELECTOR_MODE not in {"llm", "heuristic", "hybrid"}:
    _AUTO_OPENWEBPAGE_SELECTOR_MODE = "heuristic"
try:
    _AUTO_OPENWEBPAGE_SELECTOR_MAX_CANDIDATES = int(
        str(os.getenv("ASA_AUTO_OPENWEBPAGE_SELECTOR_MAX_CANDIDATES", "12") or "12")
    )
except Exception:
    _AUTO_OPENWEBPAGE_SELECTOR_MAX_CANDIDATES = 12
_AUTO_OPENWEBPAGE_SELECTOR_MAX_CANDIDATES = max(
    5,
    min(24, int(_AUTO_OPENWEBPAGE_SELECTOR_MAX_CANDIDATES)),
)
try:
    _AUTO_OPENWEBPAGE_SELECTOR_TIMEOUT_S = float(
        str(os.getenv("ASA_AUTO_OPENWEBPAGE_SELECTOR_TIMEOUT_S", "6") or "6")
    )
except Exception:
    _AUTO_OPENWEBPAGE_SELECTOR_TIMEOUT_S = 6.0
_AUTO_OPENWEBPAGE_SELECTOR_TIMEOUT_S = max(0.0, min(20.0, float(_AUTO_OPENWEBPAGE_SELECTOR_TIMEOUT_S)))
_PRIMARY_SOURCE_PATH_HINTS = (
    "about",
    "bio",
    "biography",
    "candidate",
    "directory",
    "member",
    "official",
    "person",
    "profile",
    "staff",
)
_NON_HTML_SUFFIXES = (
    ".csv",
    ".doc",
    ".docx",
    ".jpg",
    ".jpeg",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".zip",
)
_NON_SPECIFIC_PATH_SEGMENTS = {
    "all",
    "archive",
    "archives",
    "author",
    "authors",
    "blog",
    "category",
    "categories",
    "directory",
    "directories",
    "index",
    "listing",
    "listings",
    "news",
    "people",
    "results",
    "search",
    "tag",
    "tags",
    "topic",
    "topics",
}
_NON_SPECIFIC_PATH_TERMINALS = {
    "home",
    "index",
    "latest",
    "main",
    "news",
    "posts",
    "results",
    "search",
}
_NON_SPECIFIC_QUERY_KEYS = {
    "filter",
    "order",
    "page",
    "q",
    "query",
    "s",
    "search",
    "sort",
    "start",
    "view",
}


def _is_public_institution_host(host: str) -> bool:
    if not host:
        return False
    return (
        host.endswith(".gov")
        or ".gov." in host
        or host.endswith(".gob")
        or ".gob." in host
        or "parliament" in host
        or "senate" in host
        or "congress" in host
        or "assembly" in host
        or "legislature" in host
        or "government" in host
    )


def _url_path_segments(parsed: Any) -> List[str]:
    try:
        raw_segments = [seg for seg in str(parsed.path or "").split("/") if seg]
    except Exception:
        raw_segments = []
    out: List[str] = []
    for seg in raw_segments:
        clean = _normalize_match_text(unquote(seg))
        if clean:
            out.append(clean)
    return out


def _looks_like_listing_query(parsed: Any) -> bool:
    query = str(getattr(parsed, "query", "") or "")
    if not query:
        return False
    try:
        pairs = parse_qsl(query, keep_blank_values=False)
    except Exception:
        return False
    if not pairs:
        return False
    query_keys = {_normalize_match_text(k) for k, _ in pairs if _normalize_match_text(k)}
    if not query_keys:
        return False
    return query_keys.issubset(_NON_SPECIFIC_QUERY_KEYS)


def _source_specificity_score(url: Any) -> float:
    """Score URL specificity (higher is better) using generic path/query signals."""
    normalized = _canonicalize_url(url)
    if not normalized:
        return float("-inf")
    try:
        parsed = urlparse(normalized)
    except Exception:
        return float("-inf")

    score = 0.0
    segments = _url_path_segments(parsed)
    terminal = segments[-1] if segments else ""

    if not segments and not parsed.query:
        score -= 1.25
    else:
        score += min(1.5, 0.40 * len(segments))

    if parsed.query:
        score -= 0.25
        if _looks_like_listing_query(parsed):
            score -= 1.0

    generic_hits = sum(1 for seg in segments if seg in _NON_SPECIFIC_PATH_SEGMENTS)
    if generic_hits > 0 and len(segments) <= 2:
        score -= 0.8
    elif generic_hits > 1:
        score -= 0.5

    if terminal in _NON_SPECIFIC_PATH_TERMINALS:
        score -= 0.7

    slug_like_terminal = bool(re.search(r"[a-z0-9]+[-_][a-z0-9]+", terminal))
    dated_path = bool(re.search(r"/\d{4}/\d{1,2}/\d{1,2}(?:/|$)", str(parsed.path or "")))
    if slug_like_terminal or dated_path:
        score += 0.5

    return score


def _is_source_specific_url(url: Any, *, min_score: float = 0.25) -> bool:
    """Return True when URL looks specific enough to cite as provenance."""
    try:
        threshold = float(min_score)
    except Exception:
        threshold = 0.25
    return _source_specificity_score(url) >= threshold


def _score_primary_source_url(url: str) -> float:
    """Score likely primary-source URLs using task-agnostic host/path signals."""
    normalized = _normalize_url_match(url)
    if not normalized:
        return float("-inf")
    try:
        parsed = urlparse(normalized)
    except Exception:
        return float("-inf")
    host = str(parsed.hostname or "").lower()
    path = str(parsed.path or "").lower()
    score = 0.0

    if _is_public_institution_host(host):
        score += 3.0
    if "wikipedia.org" in host:
        score += 2.0
    if host.endswith(".edu") or ".edu." in host:
        score += 1.25
    if host.endswith(".org") or ".org." in host:
        score += 0.5
    if any(fragment in host for fragment in _LOW_SIGNAL_HOST_FRAGMENTS):
        score -= 2.5
    if any(hint in path for hint in _PRIMARY_SOURCE_PATH_HINTS):
        score += 0.75
    if path.endswith(_NON_HTML_SUFFIXES):
        score -= 0.75
    if path in {"", "/"} and not parsed.query:
        score -= 0.25
    specificity = _source_specificity_score(normalized)
    if specificity != float("-inf"):
        score += max(-1.5, min(1.5, specificity))
    if not _is_source_specific_url(normalized):
        score -= 0.75

    return score


def _entity_overlap_for_candidate(
    entity_tokens: List[str],
    *,
    candidate_url: str,
    candidate_text: str,
) -> tuple:
    """Return (hit_count, hit_ratio) for entity tokens against candidate URL/text."""
    if not entity_tokens:
        return 0, 0.0
    normalized_url = _normalize_url_match(candidate_url)
    normalized_text = _normalize_match_text(candidate_text)
    url_bits = ""
    if normalized_url:
        try:
            parsed = urlparse(normalized_url)
            url_bits = " ".join([str(parsed.hostname or ""), str(parsed.path or ""), str(parsed.query or "")])
        except Exception:
            url_bits = normalized_url
    combined = _normalize_match_text(f"{normalized_text} {url_bits}")
    token_set = _tokenize_for_retrieval(combined)

    hits = 0
    for token in entity_tokens:
        variants = _token_variants(token)
        if any(
            variant
            and (variant in token_set or variant in combined)
            for variant in variants
        ):
            hits += 1
    ratio = float(hits) / float(max(1, len(entity_tokens)))
    return hits, ratio


def _entity_match_thresholds(entity_token_count: int) -> tuple:
    """Return (min_hits, min_ratio) required to accept a candidate."""
    if entity_token_count <= 0:
        return 0, 0.0
    if entity_token_count == 1:
        return 1, 1.0
    if entity_token_count == 2:
        return 1, 0.50
    if entity_token_count <= 6:
        return 2, 0.45
    return 3, 0.40

def _canonicalize_url(raw_url: Any) -> Optional[str]:
    """Normalize and unwrap common redirect URLs into stable canonical links."""
    if not isinstance(raw_url, str):
        return None
    url = str(raw_url).strip().strip("<>").strip("\"'").rstrip(".,;)")
    if not url.startswith("http"):
        return None

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    scheme = str(parsed.scheme or "").lower()
    if _PREFER_HTTPS_URLS and scheme == "http":
        scheme = "https"
    host = str(parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return None

    query_map = parse_qs(parsed.query, keep_blank_values=False)
    # Unwrap common redirect wrappers first.
    if "duckduckgo.com" in host:
        redirected = query_map.get("uddg", [None])[0] or query_map.get("rut", [None])[0]
        if isinstance(redirected, str) and redirected.strip():
            unwrapped = unquote(redirected).strip()
            if unwrapped and unwrapped != url:
                return _canonicalize_url(unwrapped)
    if host.endswith("google.com") and parsed.path == "/url":
        redirected = query_map.get("url", [None])[0] or query_map.get("q", [None])[0]
        if isinstance(redirected, str) and redirected.strip():
            unwrapped = unquote(redirected).strip()
            if unwrapped and unwrapped != url:
                return _canonicalize_url(unwrapped)

    kept_query = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = str(key).lower()
        if key_lower.startswith("utm_") or key_lower in _TRACKING_QUERY_KEYS:
            continue
        kept_query.append((key, val))

    path = re.sub(r"/{2,}", "/", parsed.path or "")
    try:
        port = parsed.port
    except Exception:
        port = None
    netloc = host
    if port is not None:
        default_port = 80 if scheme == "http" else 443
        if int(port) != int(default_port):
            netloc = f"{host}:{int(port)}"

    normalized = parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path or "",
        query=urlencode(kept_query, doseq=True),
        fragment="",
    ).geturl()
    if normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    return normalized or None


def _is_noise_source_url(url: Any) -> bool:
    """Filter clearly non-evidentiary ad/redirect URLs."""
    normalized = _canonicalize_url(url)
    if not normalized:
        return True
    try:
        parsed = urlparse(normalized)
    except Exception:
        return True
    host = str(parsed.hostname or "").lower()
    path = str(parsed.path or "").lower()

    if not host:
        return True
    if host.endswith("duckduckgo.com") and (path.startswith("/y.js") or path.startswith("/l/")):
        return True
    if "googleadservices.com" in host or "doubleclick.net" in host:
        return True
    if host.startswith("ads.") and "amazon." in host:
        return True
    if "amazon." in host and "slredirect" in path:
        return True
    return False


def _normalize_url_match(url: Any) -> Optional[str]:
    normalized = _canonicalize_url(url)
    if not normalized:
        return None
    if _is_noise_source_url(normalized):
        return None
    return normalized


def _token_variants(token: str) -> set:
    token_norm = _normalize_match_text(token)
    return {token_norm} if token_norm else set()


def _normalize_match_text(text: Any) -> str:
    if text is None:
        return ""
    value = str(text).strip().lower()
    if not value:
        return ""
    folded = unicodedata.normalize("NFKD", value)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = re.sub(r"\s+", " ", folded)
    return folded.strip()
