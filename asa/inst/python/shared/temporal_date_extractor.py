# date_extractor.py
#
# Post-hoc date extraction from web pages for strict temporal filtering.
# Extracts publication dates from metadata, structured data, and URL patterns.
#
import logging
import pathlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from shared.http_retry_utils import make_request
except ImportError:
    _module_dir = pathlib.Path(__file__).resolve().parent
    _root_dir = _module_dir.parent
    for _candidate in (str(_root_dir), str(_module_dir)):
        if _candidate not in sys.path:
            sys.path.insert(0, _candidate)
    from shared.http_retry_utils import make_request

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────
@dataclass
class DateExtractionConfig:
    """Configuration for date extraction."""
    timeout: float = 10.0
    max_retries: int = 2
    retry_delay: float = 1.0
    user_agent: str = "ASA-Research-Agent/1.0"
    proxy: Optional[str] = None
    # Minimum confidence threshold to accept a date
    min_confidence: float = 0.3


@dataclass
class ExtractedDate:
    """Result of date extraction with confidence scoring."""
    date: Optional[str]  # ISO 8601 format: YYYY-MM-DD
    confidence: float    # 0.0 to 1.0
    source: str          # Source of extraction: json_ld, meta_tag, url_pattern, etc.
    raw_value: str = ""  # Original value before parsing


# ────────────────────────────────────────────────────────────────────────
# Date Parsing Utilities
# ────────────────────────────────────────────────────────────────────────
def _parse_date_string(date_str: str) -> Optional[str]:
    """Parse various date formats into ISO 8601 (YYYY-MM-DD).

    Args:
        date_str: Date string in various formats

    Returns:
        ISO 8601 date string or None if unparseable
    """
    if not date_str:
        return None

    # Clean up the string
    date_str = date_str.strip()

    # Common formats to try
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",      # ISO 8601 with timezone
        "%Y-%m-%dT%H:%M:%SZ",       # ISO 8601 with Z
        "%Y-%m-%dT%H:%M:%S",        # ISO 8601 without timezone
        "%Y-%m-%d",                 # Simple ISO date
        "%B %d, %Y",                # January 15, 2024
        "%b %d, %Y",                # Jan 15, 2024
        "%d %B %Y",                 # 15 January 2024
        "%d %b %Y",                 # 15 Jan 2024
        "%m/%d/%Y",                 # 01/15/2024
        "%d/%m/%Y",                 # 15/01/2024
        "%Y/%m/%d",                 # 2024/01/15
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str[:30], fmt)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue

    # Try regex patterns for partial dates
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        year = year_match.group(0)
        month_match = re.search(r'\b(0?[1-9]|1[0-2])\b', date_str)
        day_match = re.search(r'\b(0?[1-9]|[12]\d|3[01])\b', date_str)

        if month_match and day_match:
            try:
                month = int(month_match.group(0))
                day = int(day_match.group(0))
                return f"{year}-{month:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass

        # Return year-only date with low confidence (caller should adjust)
        return f"{year}-01-01"

    return None


# ────────────────────────────────────────────────────────────────────────
# Extraction Methods
# ────────────────────────────────────────────────────────────────────────
def _extract_from_json_ld(soup: BeautifulSoup) -> List[ExtractedDate]:
    """Extract dates from JSON-LD structured data (schema.org)."""
    results = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(script.string or "")

            # Handle list of items
            items = data if isinstance(data, list) else [data]

            for item in items:
                if not isinstance(item, dict):
                    continue

                # Check for datePublished
                for date_field in ["datePublished", "dateCreated", "dateModified"]:
                    if date_field in item:
                        raw_value = str(item[date_field])
                        parsed = _parse_date_string(raw_value)
                        if parsed:
                            # datePublished has highest confidence
                            confidence = 0.95 if date_field == "datePublished" else 0.85
                            results.append(ExtractedDate(
                                date=parsed,
                                confidence=confidence,
                                source=f"json_ld.{date_field}",
                                raw_value=raw_value
                            ))

                # Check nested @graph structure
                if "@graph" in item:
                    for graph_item in item["@graph"]:
                        if isinstance(graph_item, dict):
                            for date_field in ["datePublished", "dateCreated"]:
                                if date_field in graph_item:
                                    raw_value = str(graph_item[date_field])
                                    parsed = _parse_date_string(raw_value)
                                    if parsed:
                                        results.append(ExtractedDate(
                                            date=parsed,
                                            confidence=0.90,
                                            source=f"json_ld.@graph.{date_field}",
                                            raw_value=raw_value
                                        ))

        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.debug(f"JSON-LD parsing error: {e}")
            continue

    return results


def _extract_from_meta_tags(soup: BeautifulSoup) -> List[ExtractedDate]:
    """Extract dates from HTML meta tags (Open Graph, Twitter, etc.)."""
    results = []

    # Meta tag patterns with confidence scores
    meta_patterns = [
        ("article:published_time", 0.90),
        ("og:article:published_time", 0.90),
        ("article:modified_time", 0.80),
        ("og:updated_time", 0.75),
        ("date", 0.70),
        ("pubdate", 0.85),
        ("DC.date", 0.80),
        ("DC.date.issued", 0.85),
        ("dcterms.created", 0.80),
    ]

    for pattern, confidence in meta_patterns:
        # Try property attribute
        meta = soup.find("meta", property=pattern)
        if not meta:
            # Try name attribute
            meta = soup.find("meta", attrs={"name": pattern})

        if meta and meta.get("content"):
            raw_value = meta["content"]
            parsed = _parse_date_string(raw_value)
            if parsed:
                results.append(ExtractedDate(
                    date=parsed,
                    confidence=confidence,
                    source=f"meta.{pattern}",
                    raw_value=raw_value
                ))

    return results


def _extract_from_url(url: str) -> List[ExtractedDate]:
    """Extract date from URL patterns like /2024/01/15/ or /2024-01-15/."""
    results = []

    # Pattern: /YYYY/MM/DD/ or /YYYY/MM/
    match = re.search(r'/(\d{4})/(\d{2})(?:/(\d{2}))?(?:/|$)', url)
    if match:
        year, month, day = match.groups()
        day = day or "01"
        try:
            date_str = f"{year}-{month}-{day}"
            # Validate it's a real date
            datetime.strptime(date_str, "%Y-%m-%d")
            results.append(ExtractedDate(
                date=date_str,
                confidence=0.60,
                source="url_pattern_ymd",
                raw_value=match.group(0)
            ))
        except ValueError:
            pass

    # Pattern: /YYYY-MM-DD/ or -YYYY-MM-DD- in URL
    match = re.search(r'[/-](\d{4}-\d{2}-\d{2})(?:/|$|-)', url)
    if match:
        date_str = match.group(1)
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            results.append(ExtractedDate(
                date=date_str,
                confidence=0.65,
                source="url_pattern_iso",
                raw_value=match.group(0)
            ))
        except ValueError:
            pass

    return results


def _extract_from_time_element(soup: BeautifulSoup) -> List[ExtractedDate]:
    """Extract dates from HTML5 <time> elements."""
    results = []

    for time_elem in soup.find_all("time"):
        # Check datetime attribute
        datetime_attr = time_elem.get("datetime")
        if datetime_attr:
            parsed = _parse_date_string(datetime_attr)
            if parsed:
                # Higher confidence if it has pubdate or itemprop
                confidence = 0.85
                if time_elem.get("pubdate") is not None:
                    confidence = 0.90
                if time_elem.get("itemprop") in ["datePublished", "dateCreated"]:
                    confidence = 0.90

                results.append(ExtractedDate(
                    date=parsed,
                    confidence=confidence,
                    source="time_element",
                    raw_value=datetime_attr
                ))

    return results


# ────────────────────────────────────────────────────────────────────────
# Main Extraction Function
# ────────────────────────────────────────────────────────────────────────
def extract_publication_date(
    url: str,
    html_content: Optional[str] = None,
    config: Optional[DateExtractionConfig] = None
) -> ExtractedDate:
    """Extract publication date from a web page.

    Uses multiple extraction methods and returns the highest-confidence result.

    Args:
        url: URL of the page
        html_content: Optional pre-fetched HTML content
        config: Optional configuration

    Returns:
        ExtractedDate with the best date found, or None date with 0 confidence
    """
    config = config or DateExtractionConfig()
    all_dates: List[ExtractedDate] = []

    # Extract from URL first (no fetch needed)
    all_dates.extend(_extract_from_url(url))

    # Fetch content if not provided
    if html_content is None:
        try:
            response = make_request(
                url=url,
                method="GET",
                timeout=float(config.timeout),
                max_retries=max(1, int(config.max_retries)),
                retry_delay=max(0.0, float(config.retry_delay)),
                proxy=config.proxy,
                user_agent=config.user_agent,
            )
            html_content = response.text
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            # Return best URL-based result or empty
            if all_dates:
                return max(all_dates, key=lambda d: d.confidence)
            return ExtractedDate(date=None, confidence=0.0, source="fetch_failed")

    # Parse HTML
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract from various sources
    all_dates.extend(_extract_from_json_ld(soup))
    all_dates.extend(_extract_from_meta_tags(soup))
    all_dates.extend(_extract_from_time_element(soup))

    # Return highest confidence result
    if all_dates:
        best = max(all_dates, key=lambda d: d.confidence)
        logger.debug(f"Best date for {url}: {best.date} (confidence={best.confidence}, source={best.source})")
        return best

    return ExtractedDate(date=None, confidence=0.0, source="none_found")


def verify_date_constraint(
    url: str,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    html_content: Optional[str] = None,
    config: Optional[DateExtractionConfig] = None
) -> Dict[str, Any]:
    """Verify if a URL's publication date meets temporal constraints.

    Args:
        url: URL to verify
        date_after: Require publication date AFTER this date
        date_before: Require publication date BEFORE this date
        html_content: Optional pre-fetched HTML
        config: Optional configuration

    Returns:
        Dict with: passes (bool), extracted_date, confidence, reason
    """
    config = config or DateExtractionConfig()
    extracted = extract_publication_date(url, html_content, config)

    result = {
        "url": url,
        "extracted_date": extracted.date,
        "confidence": extracted.confidence,
        "source": extracted.source,
        "passes": None,  # None = undetermined, True = passes, False = fails
        "reason": ""
    }

    # If no date found or low confidence, return undetermined
    if not extracted.date or extracted.confidence < config.min_confidence:
        result["passes"] = None
        result["reason"] = "insufficient_confidence" if extracted.date else "no_date_found"
        return result

    try:
        pub_date = datetime.strptime(extracted.date, "%Y-%m-%d")

        # Check after constraint
        if date_after:
            after_date = datetime.strptime(date_after, "%Y-%m-%d")
            if pub_date < after_date:
                result["passes"] = False
                result["reason"] = f"date {extracted.date} is before {date_after}"
                return result

        # Check before constraint
        if date_before:
            before_date = datetime.strptime(date_before, "%Y-%m-%d")
            if pub_date >= before_date:
                result["passes"] = False
                result["reason"] = f"date {extracted.date} is after {date_before}"
                return result

        # All constraints passed
        result["passes"] = True
        result["reason"] = "temporal_constraints_met"

    except ValueError as e:
        result["passes"] = None
        result["reason"] = f"date_parse_error: {e}"

    return result


def filter_results_by_date(
    results: List[Dict[str, Any]],
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    url_field: str = "url",
    config: Optional[DateExtractionConfig] = None
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Filter a list of search results by temporal constraints.

    Args:
        results: List of result dictionaries
        date_after: Filter to results AFTER this date
        date_before: Filter to results BEFORE this date
        url_field: Field name containing the URL in each result
        config: Optional configuration

    Returns:
        Tuple of (passed, failed, undetermined) result lists
    """
    config = config or DateExtractionConfig()
    passed = []
    failed = []
    undetermined = []

    for result in results:
        url = result.get(url_field, "")
        if not url:
            undetermined.append(result)
            continue

        verification = verify_date_constraint(
            url, date_after, date_before, config=config
        )

        # Add verification info to result
        result_with_meta = {
            **result,
            "_date_extracted": verification["extracted_date"],
            "_date_confidence": verification["confidence"],
            "_date_source": verification["source"],
            "_temporal_filter_passed": verification["passes"]
        }

        if verification["passes"] is True:
            passed.append(result_with_meta)
        elif verification["passes"] is False:
            failed.append(result_with_meta)
        else:
            undetermined.append(result_with_meta)

    logger.info(
        f"Date filtering: {len(passed)} passed, {len(failed)} failed, "
        f"{len(undetermined)} undetermined"
    )

    return passed, failed, undetermined
