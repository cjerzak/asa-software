# wayback_tool.py
#
# Internet Archive Wayback Machine integration for strict temporal filtering.
# Provides guaranteed pre-date content access via archived snapshots.
#
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from langchain_core.tools import BaseTool
from pydantic import Field

from state_utils import parse_date_filters
from http_utils import make_request

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────
WAYBACK_CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_AVAILABILITY_ENDPOINT = "https://archive.org/wayback/available"
WAYBACK_BASE_URL = "https://web.archive.org/web"

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "ASA-Research-Agent/1.0 (Wayback Integration)"


@dataclass
class WaybackConfig:
    """Configuration for Wayback Machine queries."""
    timeout: float = DEFAULT_TIMEOUT
    max_results: int = 100
    retry_count: int = 3
    retry_delay: float = 2.0
    # Rate limiting to be polite to Internet Archive
    request_delay: float = 0.5


# ────────────────────────────────────────────────────────────────────────
# Core API Functions
# ────────────────────────────────────────────────────────────────────────
def check_availability(
    url: str,
    timestamp: Optional[str] = None,
    config: Optional[WaybackConfig] = None
) -> Optional[Dict[str, Any]]:
    """Check if a URL is archived in the Wayback Machine.

    Args:
        url: URL to check
        timestamp: Optional timestamp in format YYYYMMDDhhmmss (14 digits max)
        config: Optional configuration

    Returns:
        Dict with snapshot info if available, None otherwise
    """
    config = config or WaybackConfig()

    params = {"url": url}
    if timestamp:
        params["timestamp"] = timestamp

    try:
        response = make_request(
            url=WAYBACK_AVAILABILITY_ENDPOINT,
            params=params,
            timeout=config.timeout,
            max_retries=config.retry_count,
            retry_delay=config.retry_delay,
            user_agent=DEFAULT_USER_AGENT,
        )

        data = response.json()
        archived = data.get("archived_snapshots", {})
        closest = archived.get("closest")

        if closest and closest.get("available"):
            return {
                "url": closest.get("url"),
                "timestamp": closest.get("timestamp"),
                "status": closest.get("status"),
                "original_url": url
            }
        return None

    except requests.RequestException as e:
        logger.warning(f"Wayback availability check failed for {url}: {e}")
        return None


def search_cdx(
    url: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    match_type: str = "exact",
    limit: int = 100,
    config: Optional[WaybackConfig] = None
) -> List[Dict[str, Any]]:
    """Search Wayback Machine CDX index for archived snapshots.

    Args:
        url: URL or URL pattern to search
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        match_type: "exact", "prefix", "host", or "domain"
        limit: Maximum results to return
        config: Optional configuration

    Returns:
        List of snapshot records with timestamp, url, status, etc.
    """
    config = config or WaybackConfig()

    params = {
        "url": url,
        "output": "json",
        "limit": min(limit, config.max_results),
        "matchType": match_type,
        "fl": "timestamp,original,statuscode,mimetype,length"
    }

    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    try:
        response = make_request(
            url=WAYBACK_CDX_ENDPOINT,
            params=params,
            timeout=config.timeout,
            max_retries=config.retry_count,
            retry_delay=config.retry_delay,
            user_agent=DEFAULT_USER_AGENT,
        )

        data = response.json()

        # First row is headers if results exist
        if not data or len(data) < 2:
            return []

        headers = data[0]
        results = []

        for row in data[1:]:
            record = dict(zip(headers, row))
            # Parse timestamp
            ts = record.get("timestamp", "")
            if ts:
                try:
                    dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
                    record["datetime"] = dt.isoformat()
                    record["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # Build full Wayback URL
            if ts and record.get("original"):
                record["wayback_url"] = f"{WAYBACK_BASE_URL}/{ts}/{record['original']}"

            results.append(record)

        logger.info(f"CDX search returned {len(results)} snapshots for {url}")
        return results

    except requests.RequestException as e:
        logger.warning(f"CDX search failed for {url}: {e}")
        return []


def get_snapshot_content(
    wayback_url: str,
    config: Optional[WaybackConfig] = None
) -> Optional[str]:
    """Fetch content from a Wayback Machine snapshot.

    Args:
        wayback_url: Full Wayback Machine URL (e.g., web.archive.org/web/2020.../...)
        config: Optional configuration

    Returns:
        HTML content of the archived page, or None on error
    """
    config = config or WaybackConfig()

    try:
        response = make_request(
            url=wayback_url,
            timeout=config.timeout,
            max_retries=config.retry_count,
            retry_delay=config.retry_delay,
            user_agent=DEFAULT_USER_AGENT,
        )
        return response.text

    except requests.RequestException as e:
        logger.warning(f"Failed to fetch Wayback content from {wayback_url}: {e}")
        return None


def find_snapshots_before(
    url: str,
    before_date: str,
    limit: int = 10,
    config: Optional[WaybackConfig] = None
) -> List[Dict[str, Any]]:
    """Find archived snapshots of a URL from BEFORE a given date.

    Args:
        url: URL to search for
        before_date: ISO 8601 date (YYYY-MM-DD) - find snapshots before this
        limit: Maximum snapshots to return
        config: Optional configuration

    Returns:
        List of snapshot records, newest first
    """
    # Convert ISO date to Wayback format (YYYYMMDD)
    try:
        dt = datetime.strptime(before_date, "%Y-%m-%d")
        to_date = dt.strftime("%Y%m%d")
    except ValueError:
        logger.error(f"Invalid date format: {before_date}")
        return []

    snapshots = search_cdx(
        url=url,
        to_date=to_date,
        limit=limit,
        config=config
    )

    # Sort by timestamp descending (newest first within the pre-date range)
    snapshots.sort(key=lambda s: s.get("timestamp", ""), reverse=True)

    return snapshots


def find_snapshots_in_range(
    url: str,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    limit: int = 50,
    config: Optional[WaybackConfig] = None
) -> List[Dict[str, Any]]:
    """Find archived snapshots of a URL within a date range.

    Args:
        url: URL to search for
        after_date: ISO 8601 date (YYYY-MM-DD) - find snapshots after this
        before_date: ISO 8601 date (YYYY-MM-DD) - find snapshots before this
        limit: Maximum snapshots to return
        config: Optional configuration

    Returns:
        List of snapshot records
    """
    from_date = None
    to_date = None

    try:
        if after_date:
            dt = datetime.strptime(after_date, "%Y-%m-%d")
            from_date = dt.strftime("%Y%m%d")
        if before_date:
            dt = datetime.strptime(before_date, "%Y-%m-%d")
            to_date = dt.strftime("%Y%m%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return []

    return search_cdx(
        url=url,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        config=config
    )


# ────────────────────────────────────────────────────────────────────────
# LangChain Tool Interface
# ────────────────────────────────────────────────────────────────────────
class WaybackSearchTool(BaseTool):
    """LangChain tool for searching Internet Archive Wayback Machine.

    Use this tool when you need:
    - Historical versions of web pages
    - Guaranteed pre-date content
    - Content that may have been removed or changed
    """

    name: str = "wayback_search"
    description: str = """Search Internet Archive Wayback Machine for historical web content.

    Best for:
    - Finding content from a specific time period
    - Verifying what a page said on a past date
    - Accessing content that may have been removed

    Input format:
    - "url:https://example.com before:2020-01-01" - Find snapshots before date
    - "url:https://example.com after:2018-01-01 before:2020-01-01" - Range
    - Just a URL to find all available snapshots

    Returns snapshot URLs and dates that can be fetched for content.
    """

    config: WaybackConfig = Field(default_factory=WaybackConfig)

    def _parse_query(self, query: str) -> Tuple[str, Optional[str], Optional[str]]:
        """Parse query string for URL and date constraints.

        Uses shared parse_date_filters for date extraction.
        """
        import re

        url = query

        # Extract url: prefix
        url_match = re.search(r'url:(\S+)', query)
        if url_match:
            url = url_match.group(1)
            query = query.replace(url_match.group(0), "").strip()

        # Use shared date filter parsing
        cleaned, after_date, before_date = parse_date_filters(query)

        # If no url: prefix, the cleaned query is the URL
        if not url_match:
            url = cleaned

        return url, after_date, before_date

    def _run(self, query: str) -> str:
        """Execute Wayback search and return formatted results."""
        try:
            url, after_date, before_date = self._parse_query(query)

            if not url or not url.startswith("http"):
                return f"Invalid URL: {url}. Please provide a valid HTTP/HTTPS URL."

            # Search for snapshots
            snapshots = find_snapshots_in_range(
                url=url,
                after_date=after_date,
                before_date=before_date,
                limit=20,
                config=self.config
            )

            if not snapshots:
                return f"No archived snapshots found for {url} in the specified date range."

            # Format results
            lines = [f"Found {len(snapshots)} archived snapshots:"]
            for i, snap in enumerate(snapshots[:10], 1):
                date = snap.get("date", "unknown")
                wayback_url = snap.get("wayback_url", "")
                status = snap.get("statuscode", "")
                lines.append(f"{i}. [{date}] {wayback_url} (status: {status})")

            if len(snapshots) > 10:
                lines.append(f"... and {len(snapshots) - 10} more snapshots")

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Wayback search error: {e}")
            return f"Error searching Wayback Machine: {str(e)}"

    async def _arun(self, query: str) -> str:
        """Async version - just calls sync for now."""
        return self._run(query)


def create_wayback_tool(config: Optional[WaybackConfig] = None) -> WaybackSearchTool:
    """Factory function to create a WaybackSearchTool instance.

    Args:
        config: Optional WaybackConfig for customization

    Returns:
        Configured WaybackSearchTool instance
    """
    return WaybackSearchTool(config=config or WaybackConfig())


# ────────────────────────────────────────────────────────────────────────
# Convenience Functions for R Integration
# ────────────────────────────────────────────────────────────────────────
def wayback_check_url(url: str, timestamp: Optional[str] = None) -> Optional[Dict]:
    """Check if URL is archived. Returns snapshot info or None."""
    return check_availability(url, timestamp)


def wayback_get_snapshots(
    url: str,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    limit: int = 50
) -> List[Dict]:
    """Get list of archived snapshots for a URL within date range."""
    return find_snapshots_in_range(url, after_date, before_date, limit)


def wayback_fetch_content(wayback_url: str) -> Optional[str]:
    """Fetch content from a Wayback Machine URL."""
    return get_snapshot_content(wayback_url)
