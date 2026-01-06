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
    """
    max_results: int = 10
    timeout: float = 15.0
    max_retries: int = 3
    retry_delay: float = 2.0
    backoff_multiplier: float = 1.5
    captcha_backoff_base: float = 3.0
    page_load_wait: float = 2.0
    inter_search_delay: float = 0.5


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
) -> SearchConfig:
    """Configure global search defaults. Call from R via reticulate.

    Only non-None values will update the defaults. Returns the updated config.
    Thread-safe: uses lock to prevent race conditions during configuration.
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
def _new_driver(
    *,
    proxy: str | None,
    headers: Dict[str, str] | None,
    headless: bool,
    bypass_proxy_for_driver: bool,
    use_proxy_for_browser: bool = True,
) -> webdriver.Firefox | webdriver.Chrome:
    """Create a Selenium WebDriver instance.

    Tries Firefox first (preferred), falls back to Chrome if unavailable.
    Each browser uses its appropriate options configuration.
    """
    # Temporarily clear proxies so Selenium-Manager can download drivers
    saved_env: Dict[str, str] = {}
    if bypass_proxy_for_driver:
        for var in ("HTTP_PROXY", "HTTPS_PROXY"):
            if var in os.environ:
                saved_env[var] = os.environ.pop(var)

    driver = None
    try:
        # Try Firefox first (default) with Firefox-compatible options
        firefox_opts = FirefoxOptions()
        firefox_opts.add_argument("--headless")
        firefox_opts.add_argument("--disable-gpu")
        firefox_opts.add_argument("--no-sandbox")
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
            logger.debug("Using Firefox WebDriver")
        except WebDriverException as firefox_err:
            logger.debug("Firefox unavailable (%s), trying Chrome", firefox_err)

            # Fallback to Chrome with Chrome-compatible options
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            chrome_opts = ChromeOptions()
            chrome_opts.add_argument("--headless=new")
            chrome_opts.add_argument("--disable-gpu")
            chrome_opts.add_argument("--disable-dev-shm-usage")
            chrome_opts.add_argument("--no-sandbox")
            if proxy and use_proxy_for_browser:
                chrome_opts.add_argument(f"--proxy-server={proxy}")

            driver = webdriver.Chrome(options=chrome_opts)
            logger.debug("Using Chrome WebDriver")

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
            time.sleep(cfg.page_load_wait)
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
                    wait_time = cfg.captcha_backoff_base * (attempt + 1)  # Exponential backoff
                    logger.info("Waiting %.1fs before retry...", wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    raise WebDriverException("CAPTCHA detected after all retries")

            # Now wait for results (with shorter timeout since we already loaded)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.result__a"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")

            #print(soup)
            #print("Selected:")
            #print(soup.select("a.result__snippet"))
            result_links = soup.select("a.result__a")[:max_results]
            result_snippets = soup.select("a.result__snippet")[:max_results]

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
        with _search_lock:
            if cfg.inter_search_delay > 0:
                elapsed = time.time() - _last_search_time
                if elapsed < cfg.inter_search_delay:
                    wait_time = cfg.inter_search_delay - elapsed
                    logger.debug("Inter-search delay: waiting %.2fs", wait_time)
                    time.sleep(wait_time)
            _last_search_time = time.time()

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
                                    logger.info("PRIMP waiting %.1fs before retry...", _retry_delay)
                                    time.sleep(_retry_delay)
                                    _retry_delay *= cfg.backoff_multiplier
                                    continue  # retry with different impersonation
                            elif "rate" in _page_text and "limit" in _page_text:
                                logger.warning("PRIMP rate limiting detected")
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
                    time.sleep(_retry_delay)
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
