# http_utils.py
#
# Shared HTTP request utilities with retry logic.
# Used by Wikidata and Wayback tools (not DuckDuckGo, which uses PRIMP).
#
import logging
import time
from typing import Dict, Optional, Any

import requests

logger = logging.getLogger(__name__)

# Default User-Agent for research tools (Wikidata/Wayback prefer honest identification)
DEFAULT_USER_AGENT = "ASA-Research-Agent/1.0 (https://github.com/cjerzak/asa-software)"


def build_headers(
    headers: Optional[Dict[str, str]] = None,
    user_agent: Optional[str] = None,
) -> Dict[str, str]:
    """Build request headers with a default User-Agent."""
    merged_headers = dict(headers or {})
    merged_headers.setdefault("User-Agent", user_agent or DEFAULT_USER_AGENT)
    return merged_headers


def make_request(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> requests.Response:
    """Make HTTP request with retry logic and exponential backoff.

    Args:
        url: Target URL
        method: HTTP method (GET, POST, etc.)
        params: URL query parameters
        data: Request body data
        headers: Custom headers (User-Agent added if not present)
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts
        retry_delay: Initial delay between retries (multiplied by attempt number)
        proxy: Optional proxy URL
        user_agent: Custom User-Agent (defaults to ASA research agent)

    Returns:
        requests.Response object

    Raises:
        requests.RequestException: After all retries exhausted
    """
    headers = build_headers(headers=headers, user_agent=user_agent)

    proxies = {"http": proxy, "https": proxy} if proxy else None

    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
                proxies=proxies,
            )
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout as e:
            last_error = e
            logger.warning(
                f"Request timeout (attempt {attempt + 1}/{max_retries}): {url}"
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning(
                f"Request failed (attempt {attempt + 1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

    logger.error(f"Request failed after {max_retries} attempts: {url}")
    raise last_error  # type: ignore


def request_json(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> Any:
    """Make request and parse JSON response."""
    response = make_request(
        url=url,
        method=method,
        params=params,
        data=data,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        proxy=proxy,
        user_agent=user_agent,
    )
    return response.json()


def request_text(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> str:
    """Make request and return response body text."""
    response = make_request(
        url=url,
        method=method,
        params=params,
        data=data,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        proxy=proxy,
        user_agent=user_agent,
    )
    return response.text
