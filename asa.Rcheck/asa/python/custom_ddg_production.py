# custom_duckduckgo.py
#
# LangChain‑compatible DuckDuckGo search wrapper with:
#   • optional real Chrome/Chromium via Selenium
#   • proxy + arbitrary headers on every tier
#   • automatic proxy‑bypass for Chromedriver handshake
#   • smart retries and a final pure‑Requests HTML fallback
#
import contextlib
import logging
import os
import threading
import traceback
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote, urlencode

from bs4 import BeautifulSoup
from ddgs import ddgs  # ddgs.DDGS is a class, ddgs.ddgs is a module
from ddgs import DDGS  # ddgs.DDGS is a class, ddgs.ddgs is a module
from ddgs.exceptions import DDGSException
from langchain_community.tools.ddg_search.tool import DuckDuckGoSearchRun
from langchain_community.utilities.duckduckgo_search import DuckDuckGoSearchAPIWrapper
from pydantic import Field
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests
import random as random
import primp

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
        timeout: HTTP request timeout in seconds (default: 15.0)
        max_retries: Maximum retry attempts on failure (default: 3)
        retry_delay: Initial delay between retries in seconds (default: 2.0)
        backoff_multiplier: Multiplier for exponential backoff (default: 1.5)
        captcha_backoff_base: Base multiplier for CAPTCHA backoff (default: 3.0)
        page_load_wait: Wait time after page load in seconds (default: 2.0)
        inter_search_delay: Delay between consecutive searches in seconds (default: 0.5)
        humanize_timing: Add random jitter to delays for human-like behavior (default: True)
        jitter_factor: Jitter range as fraction of base delay (default: 0.5 = ±50%)
    """
    max_results: int = 10
    timeout: float = 15.0
    max_retries: int = 3
    retry_delay: float = 2.0
    backoff_multiplier: float = 1.5
    captcha_backoff_base: float = 3.0
    page_load_wait: float = 2.0
    inter_search_delay: float = 0.5
    humanize_timing: bool = True
    jitter_factor: float = 0.5


# ────────────────────────────────────────────────────────────────────────
# Human Behavioral Entropy - The nervous pulse of a tired hand
# ────────────────────────────────────────────────────────────────────────
# The problem with clean randomness: it's too clean. Uniform distributions
# smell like bleach. Real humans have texture - hesitation, fatigue,
# distraction, the micro-stutter of uncertainty.

_SESSION_START = time.time()
_REQUEST_COUNT = 0


def _human_delay(base_delay: float, cfg: SearchConfig = None) -> float:
    """Generate a delay that feels human - messy, uncertain, tired.

    Not uniform jitter. This models:
    - Log-normal base: most actions quick, occasional long pauses (thinking)
    - Micro-stutters: tiny random additions (the tremor of a hand)
    - Fatigue curve: delays drift longer as session ages
    - Occasional spikes: the pause of a mind changing

    Args:
        base_delay: The nominal delay in seconds
        cfg: SearchConfig instance

    Returns:
        A delay that breathes like a human
    """
    global _REQUEST_COUNT
    _REQUEST_COUNT += 1

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
    session_minutes = (time.time() - _SESSION_START) / 60.0
    fatigue_factor = 1.0 + (session_minutes * 0.01) + (_REQUEST_COUNT * 0.001)
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
    "PatchedDuckDuckGoSearchAPIWrapper",
    "PatchedDuckDuckGoSearchRun",
    "BrowserDuckDuckGoSearchAPIWrapper",
    "BrowserDuckDuckGoSearchRun",
    "MemoryFoldingAgentState",
    "create_memory_folding_agent",
    "create_memory_folding_agent_with_checkpointer",
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
_TOR_CONTROL_PORT = int(os.environ.get("TOR_CONTROL_PORT", "9051"))
_TOR_CONTROL_PASSWORD = os.environ.get("TOR_CONTROL_PASSWORD", "")
_TOR_LAST_ROTATION = 0.0
_TOR_MIN_ROTATION_INTERVAL = 5.0  # Minimum seconds between rotations (reduced from 10s)

# ────────────────────────────────────────────────────────────────────────
# Proactive Anti-Detection State
# ────────────────────────────────────────────────────────────────────────
_REQUESTS_SINCE_ROTATION = 0
_PROACTIVE_ROTATION_ENABLED = True
_PROACTIVE_ROTATION_INTERVAL = 15  # Rotate every N requests (configurable from R)

_REQUESTS_SINCE_SESSION_RESET = 0
_SESSION_RESET_ENABLED = True
_SESSION_RESET_INTERVAL = 50  # Reset session every N requests


def _rotate_tor_circuit(force: bool = False) -> bool:
    """Request a new Tor circuit (new exit node) via the control port.

    This is the key to avoiding CAPTCHA when using Tor - each new circuit
    gets a different exit node IP address, making it harder to block.

    Requires Tor control port enabled. Configure in torrc:
        ControlPort 9051
        HashedControlPassword <your_hashed_password>
    Or use cookie authentication (default on many systems).

    Args:
        force: If True, rotate even if min interval hasn't passed

    Returns:
        True if rotation succeeded, False otherwise
    """
    global _TOR_LAST_ROTATION

    # Rate limit rotations to avoid hammering Tor
    now = time.time()
    if not force and (now - _TOR_LAST_ROTATION) < _TOR_MIN_ROTATION_INTERVAL:
        logger.debug("Tor rotation skipped - too soon (%.1fs since last)",
                     now - _TOR_LAST_ROTATION)
        return False

    # Try using stem library first (cleaner API)
    try:
        from stem import Signal
        from stem.control import Controller

        with Controller.from_port(port=_TOR_CONTROL_PORT) as controller:
            if _TOR_CONTROL_PASSWORD:
                controller.authenticate(password=_TOR_CONTROL_PASSWORD)
            else:
                controller.authenticate()  # Try cookie auth
            controller.signal(Signal.NEWNYM)
            _TOR_LAST_ROTATION = time.time()
            logger.info("Tor circuit rotated successfully (new exit node)")
            return True

    except ImportError:
        logger.debug("stem library not available, trying raw socket")
    except Exception as e:
        logger.debug("stem rotation failed: %s", e)

    # Fallback: raw socket communication
    try:
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", _TOR_CONTROL_PORT))

            # Authenticate
            if _TOR_CONTROL_PASSWORD:
                sock.send(f'AUTHENTICATE "{_TOR_CONTROL_PASSWORD}"\r\n'.encode())
            else:
                sock.send(b'AUTHENTICATE\r\n')

            response = sock.recv(1024).decode()
            if "250" not in response:
                logger.warning("Tor authentication failed: %s", response.strip())
                return False

            # Request new identity
            sock.send(b'SIGNAL NEWNYM\r\n')
            response = sock.recv(1024).decode()

            if "250" in response:
                _TOR_LAST_ROTATION = time.time()
                logger.info("Tor circuit rotated successfully via raw socket")
                return True
            else:
                logger.warning("Tor NEWNYM failed: %s", response.strip())
                return False

    except Exception as e:
        logger.debug("Raw socket Tor rotation failed: %s", e)
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
    global _TOR_CONTROL_PORT, _TOR_CONTROL_PASSWORD, _TOR_MIN_ROTATION_INTERVAL

    if control_port is not None:
        _TOR_CONTROL_PORT = control_port
    if control_password is not None:
        _TOR_CONTROL_PASSWORD = control_password
    if min_rotation_interval is not None:
        _TOR_MIN_ROTATION_INTERVAL = min_rotation_interval

    return {
        "control_port": _TOR_CONTROL_PORT,
        "has_password": bool(_TOR_CONTROL_PASSWORD),
        "min_rotation_interval": _TOR_MIN_ROTATION_INTERVAL,
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

    if _REQUESTS_SINCE_ROTATION >= _PROACTIVE_ROTATION_INTERVAL:
        logger.info("Proactive circuit rotation (after %d requests)",
                    _REQUESTS_SINCE_ROTATION)
        success = _rotate_tor_circuit(force=False)
        if success:
            _REQUESTS_SINCE_ROTATION = 0
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

    if _REQUESTS_SINCE_SESSION_RESET >= _SESSION_RESET_INTERVAL:
        logger.info("Session reset (after %d requests)", _REQUESTS_SINCE_SESSION_RESET)

        # Reset timing state to appear as new session
        _last_search_time = 0.0
        _REQUEST_COUNT = 0
        _SESSION_START = time.time()

        # Shuffle user-agent pool for variety
        random.shuffle(_STEALTH_USER_AGENTS)

        # Reset counter
        _REQUESTS_SINCE_SESSION_RESET = 0

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
    global _PROACTIVE_ROTATION_ENABLED, _PROACTIVE_ROTATION_INTERVAL
    global _SESSION_RESET_ENABLED, _SESSION_RESET_INTERVAL

    if proactive_rotation_enabled is not None:
        _PROACTIVE_ROTATION_ENABLED = proactive_rotation_enabled
    if proactive_rotation_interval is not None:
        _PROACTIVE_ROTATION_INTERVAL = proactive_rotation_interval
    if session_reset_enabled is not None:
        _SESSION_RESET_ENABLED = session_reset_enabled
    if session_reset_interval is not None:
        _SESSION_RESET_INTERVAL = session_reset_interval

    return {
        "proactive_rotation_enabled": _PROACTIVE_ROTATION_ENABLED,
        "proactive_rotation_interval": _PROACTIVE_ROTATION_INTERVAL,
        "session_reset_enabled": _SESSION_RESET_ENABLED,
        "session_reset_interval": _SESSION_RESET_INTERVAL,
    }


def _new_driver(
    *,
    proxy: str | None,
    headers: Dict[str, str] | None,
    headless: bool,
    bypass_proxy_for_driver: bool,
    use_proxy_for_browser: bool = True,
) -> webdriver.Firefox | webdriver.Chrome:
    """Create a Selenium WebDriver instance with stealth measures.

    Tries Firefox first (preferred), falls back to Chrome if unavailable.
    Implements anti-detection measures:
    - Randomized user agent
    - Randomized viewport size
    - Disabled automation indicators
    - Randomized language headers
    """
    # Temporarily clear proxies so Selenium-Manager can download drivers
    saved_env: Dict[str, str] = {}
    if bypass_proxy_for_driver:
        for var in ("HTTP_PROXY", "HTTPS_PROXY"):
            if var in os.environ:
                saved_env[var] = os.environ.pop(var)

    # Randomize fingerprint components
    random_ua = random.choice(_STEALTH_USER_AGENTS)
    random_viewport = random.choice(_STEALTH_VIEWPORTS)
    random_lang = random.choice(_STEALTH_LANGUAGES)

    driver = None
    try:
        # Try Firefox first (default) with stealth options
        firefox_opts = FirefoxOptions()
        firefox_opts.add_argument("--headless")
        firefox_opts.add_argument("--disable-gpu")
        firefox_opts.add_argument("--no-sandbox")
        firefox_opts.add_argument(f"--width={random_viewport[0]}")
        firefox_opts.add_argument(f"--height={random_viewport[1]}")

        # Firefox stealth preferences
        firefox_opts.set_preference("general.useragent.override", random_ua)
        firefox_opts.set_preference("intl.accept_languages", random_lang)
        firefox_opts.set_preference("dom.webdriver.enabled", False)
        firefox_opts.set_preference("useAutomationExtension", False)
        firefox_opts.set_preference("privacy.trackingprotection.enabled", False)
        firefox_opts.set_preference("network.http.sendRefererHeader", 2)
        firefox_opts.set_preference("general.platform.override", "Win32")

        # Disable telemetry and crash reporting (reduces detectability)
        firefox_opts.set_preference("toolkit.telemetry.enabled", False)
        firefox_opts.set_preference("datareporting.healthreport.uploadEnabled", False)
        firefox_opts.set_preference("browser.crashReports.unsubmittedCheck.autoSubmit2", False)

        if proxy and use_proxy_for_browser:
            firefox_opts.set_preference("network.proxy.type", 1)
            # Parse SOCKS proxy if provided
            if "socks" in (proxy or "").lower():
                # Format: socks5h://host:port
                import re
                match = re.match(r"socks\d*h?://([^:]+):(\d+)", proxy)
                if match:
                    firefox_opts.set_preference("network.proxy.socks", match.group(1))
                    firefox_opts.set_preference("network.proxy.socks_port", int(match.group(2)))
                    firefox_opts.set_preference("network.proxy.socks_remote_dns", True)

        try:
            driver = webdriver.Firefox(options=firefox_opts)
            logger.debug("Using Firefox WebDriver (stealth mode)")

            # Additional stealth: override navigator.webdriver via JavaScript
            try:
                driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                """)
            except Exception as e:
                logger.debug("Firefox stealth script injection failed: %s", e)

        except WebDriverException as firefox_err:
            logger.debug("Firefox unavailable (%s), trying Chrome", firefox_err)

            # Fallback to Chrome with stealth options
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            chrome_opts = ChromeOptions()
            chrome_opts.add_argument("--headless=new")
            chrome_opts.add_argument("--disable-gpu")
            chrome_opts.add_argument("--disable-dev-shm-usage")
            chrome_opts.add_argument("--no-sandbox")
            chrome_opts.add_argument(f"--window-size={random_viewport[0]},{random_viewport[1]}")
            chrome_opts.add_argument(f"--user-agent={random_ua}")
            chrome_opts.add_argument(f"--lang={random_lang.split(',')[0]}")

            # Chrome stealth arguments
            chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
            chrome_opts.add_argument("--disable-infobars")
            chrome_opts.add_argument("--disable-extensions")
            chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_opts.add_experimental_option("useAutomationExtension", False)

            if proxy and use_proxy_for_browser:
                chrome_opts.add_argument(f"--proxy-server={proxy}")

            driver = webdriver.Chrome(options=chrome_opts)
            logger.debug("Using Chrome WebDriver (stealth mode)")

            # Chrome stealth: override navigator.webdriver via CDP
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

            # Chrome-specific: inject headers via CDP
            if headers:
                try:
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": headers})
                except Exception as e:
                    logger.debug("CDP header injection failed: %s", e)

    finally:
        os.environ.update(saved_env)

    return driver


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
    config: SearchConfig = None,
) -> List[Dict[str, str]]:
    cfg = config or _default_config
    max_results = max_results if max_results is not None else cfg.max_results
    timeout = timeout if timeout is not None else cfg.timeout
    max_retries = max_retries if max_retries is not None else cfg.max_retries

    logger.debug("Starting browser search for: %s", query[:60])

    # Strategy: Try with proxy first (2 attempts), then without proxy (1 attempt)
    # This allows falling back to direct IP if Tor is blocked
    for attempt in range(max_retries):
        # After first 2 attempts with proxy fail, try without proxy
        use_proxy = (attempt < 2) if proxy else False
        if attempt == 2 and proxy:
            logger.info("Switching to DIRECT IP (no proxy) for final attempt")

        driver = None  # Initialize to None for safe cleanup
        try:
            driver = _new_driver(
                proxy=proxy,
                headers=headers,
                headless=headless,
                bypass_proxy_for_driver=bypass_proxy_for_driver,
                use_proxy_for_browser=use_proxy,
            )
            # Use html.duckduckgo.com which is the lite/HTML version
            driver.get(f"https://html.duckduckgo.com/html/?q={quote(query)}")

            # Wait a moment for page to load, then check for CAPTCHA
            # Use humanized timing for less predictable patterns
            humanized_page_wait = _humanize_delay(cfg.page_load_wait, cfg)
            time.sleep(humanized_page_wait)
            page_source = driver.page_source.lower()

            # Detect CAPTCHA before waiting for results
            if "captcha" in page_source or "robot" in page_source or "unusual traffic" in page_source:
                logger.warning("CAPTCHA detected on attempt %d/%d", attempt + 1, max_retries)
                # Safe driver cleanup with specific exception handling
                if driver is not None:
                    try:
                        driver.quit()
                    except (WebDriverException, OSError) as e:
                        logger.debug("Driver cleanup failed after CAPTCHA: %s", e)
                    driver = None
                if attempt < max_retries - 1:
                    # Rotate Tor circuit to get new exit node IP before retry
                    if proxy and "socks" in proxy.lower():
                        if _rotate_tor_circuit():
                            logger.info("Tor circuit rotated - new exit node for browser retry")
                        else:
                            logger.debug("Tor rotation unavailable, continuing with same circuit")
                    base_wait = cfg.captcha_backoff_base * (attempt + 1)  # Exponential backoff
                    wait_time = _humanize_delay(base_wait, cfg)
                    logger.info("Waiting %.1fs before retry (humanized)...", wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    raise WebDriverException("CAPTCHA detected after all retries")

            # Now wait for results (with shorter timeout since we already loaded)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.result__a"))
            )

            # Inject human behavior - the nervous pulse, the wandering eye
            _simulate_human_behavior(driver, cfg)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            result_links = soup.select("a.result__a")[:max_results]
            result_snippets = soup.select("a.result__snippet")[:max_results]

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

            logger.info("Browser search SUCCESS: returning %d results", len(return_content))
            return return_content
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
    """
    url = "https://html.duckduckgo.com/html"
    headers = dict(headers or {})
    headers.setdefault("User-Agent", _DEFAULT_UA)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    resp = requests.post(
        url,
        data=urlencode({"q": query}),
        headers=headers,
        proxies=proxies,
        timeout=timeout,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    results: List[Dict[str, str]] = []
    for i, a in enumerate(soup.select("a.result__a")[:max_results], 1):
        results.append({"id": i, "title": a.get_text(strip=True), "href": a["href"], "body": "", "_tier": "requests"})
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
        global _last_search_time
        cfg = _default_config

        # Apply inter-search delay to avoid rate limiting (thread-safe)
        # Uses humanized timing for less predictable patterns
        with _search_lock:
            if cfg.inter_search_delay > 0:
                elapsed = time.time() - _last_search_time
                base_delay = cfg.inter_search_delay
                humanized_delay = _humanize_delay(base_delay, cfg)
                if elapsed < humanized_delay:
                    wait_time = humanized_delay - elapsed
                    logger.debug("Inter-search delay: waiting %.2fs (humanized from %.2fs)", wait_time, base_delay)
                    time.sleep(wait_time)
            _last_search_time = time.time()

        # ────────────────────────────────────────────────────────────────────────
        # Proactive Anti-Detection: Session reset and circuit rotation
        # ────────────────────────────────────────────────────────────────────────
        _maybe_session_reset()
        _maybe_proactive_rotation(self.proxy)

        # tier 0 - primp
        # ────────────────────────────────────────────────────────────────────────
        # Tier 0 — PRIMP (fast HTTP client with browser impersonation)
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

                # Strategy: First 2 attempts use proxy, 3rd attempt uses direct IP
                _use_proxy = self.proxy if _attempt < 2 else None
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
                            if "captcha" in _page_text or "robot" in _page_text:
                                logger.warning("PRIMP CAPTCHA detected on attempt %d", _attempt + 1)
                                if _attempt < _max_retries - 1:
                                    # Rotate Tor circuit to get new exit node IP
                                    if self.proxy and "socks" in self.proxy.lower():
                                        if _rotate_tor_circuit():
                                            logger.info("Tor circuit rotated - new exit node for retry")
                                        else:
                                            logger.debug("Tor rotation unavailable, continuing with same circuit")
                                    _humanized_retry = _humanize_delay(_retry_delay, cfg)
                                    logger.info("PRIMP waiting %.1fs before retry (humanized)...", _humanized_retry)
                                    time.sleep(_humanized_retry)
                                    _retry_delay *= cfg.backoff_multiplier
                                    continue  # retry with different impersonation + new circuit
                            elif "rate" in _page_text and "limit" in _page_text:
                                logger.warning("PRIMP rate limiting detected")
                                # Also try rotating on rate limit
                                if self.proxy and "socks" in self.proxy.lower():
                                    _rotate_tor_circuit()
                            else:
                                logger.debug("PRIMP no results - page title: %s",
                                           _soup.title.string if _soup.title else 'N/A')

                        _out = []
                        _limit = int(max_results or cfg.max_results)
                        for i, a in enumerate(_links[:_limit], 1):
                            _raw = a.get("href", "")
                            _parsed = urlparse(_raw)
                            _real = unquote(parse_qs(_parsed.query).get("uddg", [_raw])[0])

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
                            logger.info("PRIMP SUCCESS: returning %d results", len(_out))
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
        # End tier 0

        # Tier 1 – Selenium
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
                logger.warning("Browser tier WebDriverException: %s", exc)
                logger.debug("Browser tier traceback: %s", traceback.format_exc())

        logger.debug("Selenium tier complete, trying next tier")

        # Tier 2 – ddgs HTTP API
        logger.debug("Trying DDGS tier...")
        try:
            ddgs_results = _with_ddgs(
                self.proxy,
                self.headers,
                lambda d: list(
                    d.text(
                        query,
                        max_results=max_results,
                        region=self.region,
                        safesearch=self.safesearch,
                        timelimit=self.time,
                    )
                ),
            )
            # Add tier info to each result
            for item in ddgs_results:
                item["_tier"] = "ddgs"
            return ddgs_results
        except DDGSException as exc:
            logger.warning("DDGS tier failed (%s); falling back to raw scrape.", exc)

        # Tier 3 – raw Requests scrape
        logger.debug("Trying requests scrape tier...")
        return _requests_scrape(
            query,
            max_results=max_results,
            proxy=self.proxy,
            headers=self.headers,
        )

    # LangChain calls the four "_ddgs_*" methods – just delegate.
    def _ddgs_text(self, query: str, **kw):
        return self._search_text(query, kw.get("max_results", self.k))

    def _ddgs_images(self, query: str, **kw):
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.images(
                    query,
                    max_results=kw.get("max_results", self.k),
                    region=self.region,
                    safesearch=self.safesearch,
                    timelimit=self.time,
                )
            ),
        )

    def _ddgs_videos(self, query: str, **kw):
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.videos(
                    query,
                    max_results=kw.get("max_results", self.k),
                    region=self.region,
                    safesearch=self.safesearch,
                    timelimit=self.time,
                )
            ),
        )

    def _ddgs_news(self, query: str, **kw):
        return _with_ddgs(
            self.proxy,
            self.headers,
            lambda d: list(
                d.news(
                    query,
                    max_results=kw.get("max_results", self.k),
                    region=self.region,
                    safesearch=self.safesearch,
                    timelimit=self.time,
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


# ────────────────────────────────────────────────────────────────────────
# Autonomous Memory Folding Agent (DeepAgent-style)
# ────────────────────────────────────────────────────────────────────────
# This implements the memory folding architecture from the DeepAgent paper
# using LangGraph's StateGraph with conditional edges for context management.
#
# Key concepts:
#   • AgentState separates 'messages' (working memory) from 'summary' (long-term)
#   • Conditional edge checks message count after each agent step
#   • If threshold exceeded, flow diverts to summarizer node
#   • Summarizer compresses oldest messages into summary, removes them from state

from typing import Annotated, TypedDict, Sequence
from operator import add as operator_add

def _add_messages(left: list, right: list) -> list:
    """Reducer for messages that handles RemoveMessage objects."""
    # Import here to avoid circular imports
    try:
        from langchain_core.messages import RemoveMessage

        # Start with a copy of left messages
        result = list(left) if left else []

        for msg in (right or []):
            if isinstance(msg, RemoveMessage):
                # Remove message by ID
                result = [m for m in result if getattr(m, 'id', None) != msg.id]
            else:
                result.append(msg)
        return result
    except Exception:
        # Fallback: simple concatenation
        return (left or []) + (right or [])


class MemoryFoldingAgentState(TypedDict):
    """
    State schema for DeepAgent-style memory folding.

    Attributes:
        messages: Working memory - recent messages in the conversation
        summary: Long-term memory - compressed summary of older interactions
        fold_count: Number of times memory has been folded (for debugging)
    """
    messages: Annotated[list, _add_messages]
    summary: str
    fold_count: int


def create_memory_folding_agent(
    model,
    tools: list,
    *,
    checkpointer=None,
    message_threshold: int = 6,
    keep_recent: int = 2,
    summarizer_model = None,
    debug: bool = False
):
    """
    Create a LangGraph agent with autonomous memory folding.

    This implements the DeepAgent paper's "Autonomous Memory Folding" where
    the agent monitors context load and automatically summarizes older
    messages to prevent context overflow.

    Args:
        model: The LLM to use for the agent (e.g., ChatOpenAI, ChatGroq)
        tools: List of LangChain tools (e.g., search, wikipedia)
        checkpointer: Optional LangGraph checkpointer for persistence (e.g., MemorySaver())
        message_threshold: Trigger folding when messages exceed this count
        keep_recent: Number of recent messages to preserve after folding
        summarizer_model: Optional separate model for summarization (defaults to main model)
        debug: Enable debug logging

    Returns:
        A compiled LangGraph StateGraph that can be invoked with .invoke()
    """
    from langgraph.graph import StateGraph, END
    from langchain_core.messages import RemoveMessage, SystemMessage, HumanMessage, AIMessage

    # Use main model for summarization if not specified
    if summarizer_model is None:
        summarizer_model = model

    # Bind tools to model for the agent node
    model_with_tools = model.bind_tools(tools)

    # Create tool executor
    from langgraph.prebuilt import ToolNode
    tool_node = ToolNode(tools)

    def _get_system_prompt(summary: str) -> str:
        """Generate system prompt that includes folded memory context."""
        base_prompt = (
            "You are a helpful research assistant with access to search tools. "
            "Use tools when you need current information or facts you're unsure about."
        )
        if summary:
            return (
                f"{base_prompt}\n\n"
                f"=== LONG-TERM MEMORY (Summary of previous interactions) ===\n"
                f"{summary}\n"
                f"=== END LONG-TERM MEMORY ===\n\n"
                f"Consult your long-term memory above for context before acting."
            )
        return base_prompt

    def agent_node(state: MemoryFoldingAgentState) -> dict:
        """The main agent reasoning node."""
        messages = state.get("messages", [])
        summary = state.get("summary", "")

        # Prepend system message with summary context
        system_msg = SystemMessage(content=_get_system_prompt(summary))
        full_messages = [system_msg] + list(messages)

        if debug:
            logger.info(f"Agent node: {len(messages)} messages, summary={bool(summary)}")

        # Invoke the model
        response = model_with_tools.invoke(full_messages)

        return {"messages": [response]}

    def summarize_conversation(state: MemoryFoldingAgentState) -> dict:
        """
        The memory folding node - compresses old messages into summary.

        This is the key innovation from DeepAgent: instead of truncating
        context, we intelligently summarize older interactions to preserve
        important information while freeing up context space.

        IMPORTANT: We must be careful to maintain message coherence:
        - Tool response messages must always follow their corresponding AI tool_calls
        - We fold complete "rounds" of conversation, not partial sequences
        """
        messages = state.get("messages", [])
        current_summary = state.get("summary", "")
        fold_count = state.get("fold_count", 0)

        if len(messages) <= keep_recent:
            return {}  # Nothing to fold

        # Find safe fold boundary - we need to fold complete "rounds"
        # A round = HumanMessage -> AIMessage(with tool_calls) -> ToolMessages -> AIMessage(final)
        # We should only fold messages up to a complete AI response (no pending tool calls)
        safe_fold_idx = 0
        i = 0
        while i < len(messages) - keep_recent:
            msg = messages[i]
            msg_type = type(msg).__name__

            if msg_type == 'AIMessage':
                tool_calls = getattr(msg, 'tool_calls', None)
                if not tool_calls:
                    # This AI message has no tool calls - safe boundary
                    safe_fold_idx = i + 1
            elif msg_type == 'HumanMessage':
                # Human messages are safe fold boundaries
                safe_fold_idx = i + 1

            i += 1

        # If safe_fold_idx is 0, we can't safely fold anything yet
        if safe_fold_idx == 0:
            if debug:
                logger.info("No safe fold boundary found, skipping fold")
            return {}

        messages_to_fold = messages[:safe_fold_idx]

        if not messages_to_fold:
            return {}

        if debug:
            logger.info(f"Folding {len(messages_to_fold)} messages into summary (safe boundary at {safe_fold_idx})")

        # Build the summarization prompt
        fold_text_parts = []
        for msg in messages_to_fold:
            msg_type = type(msg).__name__
            content = getattr(msg, 'content', str(msg))
            # Handle tool calls in AI messages
            tool_calls = getattr(msg, 'tool_calls', None)
            if tool_calls:
                tool_info = ", ".join([f"{tc.get('name', 'tool')}" for tc in tool_calls])
                fold_text_parts.append(f"[{msg_type}] (called tools: {tool_info}) {content[:200] if len(content) > 200 else content}")
            elif msg_type == 'ToolMessage':
                # Truncate tool responses for summary
                truncated = content[:300] + "..." if len(content) > 300 else content
                fold_text_parts.append(f"[{msg_type}] {truncated}")
            else:
                fold_text_parts.append(f"[{msg_type}] {content}")

        fold_text = "\n".join(fold_text_parts)

        summarize_prompt = (
            f"You are summarizing a conversation for long-term memory storage.\n\n"
            f"Current summary (if any):\n{current_summary or '(empty)'}\n\n"
            f"New messages to incorporate:\n{fold_text}\n\n"
            f"Create a concise but comprehensive summary that:\n"
            f"1. Preserves key facts, findings, and conclusions\n"
            f"2. Notes any tools used and their results\n"
            f"3. Maintains context needed for future queries\n"
            f"Keep the summary under 500 words. Focus on information density."
        )

        # Generate new summary
        summary_response = summarizer_model.invoke([HumanMessage(content=summarize_prompt)])
        new_summary = summary_response.content if hasattr(summary_response, 'content') else str(summary_response)

        # Create RemoveMessage objects for old messages
        remove_messages = []
        for msg in messages_to_fold:
            msg_id = getattr(msg, 'id', None)
            if msg_id:
                remove_messages.append(RemoveMessage(id=msg_id))

        return {
            "summary": new_summary,
            "messages": remove_messages,
            "fold_count": fold_count + 1
        }

    def should_continue(state: MemoryFoldingAgentState) -> str:
        """
        Determine next step after agent node.

        Routes to:
        - 'tools': If the agent wants to use a tool
        - 'summarize': If message count exceeds threshold AND agent is done (no tool calls)
        - 'end': If the agent is done and no folding needed

        IMPORTANT: We only fold memory when the agent has completed its response
        (no pending tool calls) to maintain message sequence integrity.
        """
        messages = state.get("messages", [])

        if not messages:
            return "end"

        last_message = messages[-1]

        # Check if agent wants to use tools - if so, DON'T fold yet
        tool_calls = getattr(last_message, 'tool_calls', None)
        if tool_calls:
            return "tools"

        # Agent is done (no tool calls) - NOW check if we need to fold memory
        if len(messages) > message_threshold:
            if debug:
                logger.info(f"Memory threshold exceeded: {len(messages)} > {message_threshold}, triggering fold")
            return "summarize"

        return "end"

    def after_tools(state: MemoryFoldingAgentState) -> str:
        """
        Determine next step after tool execution.

        Routes to:
        - 'agent': Always return to agent for next step

        NOTE: We do NOT fold memory after tools - only after agent completes.
        This prevents breaking the tool_call -> tool_response sequence.
        """
        return "agent"

    def after_summarize(state: MemoryFoldingAgentState) -> str:
        """After summarizing, return to agent to continue."""
        return "agent"

    # Build the StateGraph
    workflow = StateGraph(MemoryFoldingAgentState)

    # Add nodes
    workflow.add_node("agent", agent_node)
    workflow.add_node("tools", tool_node)
    workflow.add_node("summarize", summarize_conversation)

    # Set entry point
    workflow.set_entry_point("agent")

    # Add conditional edges from agent
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "tools": "tools",
            "summarize": "summarize",
            "end": END
        }
    )

    # Tools always return to agent (no folding mid-tool-loop)
    workflow.add_edge("tools", "agent")

    # After summarizing, we END (the response was already given by agent)
    workflow.add_edge("summarize", END)

    # Compile with optional checkpointer and return
    return workflow.compile(checkpointer=checkpointer)


def create_memory_folding_agent_with_checkpointer(
    model,
    tools: list,
    checkpointer,
    **kwargs
):
    """
    Create a memory folding agent with a checkpointer for persistence.

    DEPRECATED: Use create_memory_folding_agent(checkpointer=...) instead.
    This function is kept for backward compatibility.

    Args:
        model: The LLM to use
        tools: List of tools
        checkpointer: LangGraph checkpointer (e.g., MemorySaver())
        **kwargs: Additional arguments passed to create_memory_folding_agent

    Returns:
        Compiled graph with checkpointer
    """
    return create_memory_folding_agent(
        model=model,
        tools=tools,
        checkpointer=checkpointer,
        **kwargs
    )
