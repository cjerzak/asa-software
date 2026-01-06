# config_base.py
#
# Shared base configuration classes for network operations and temporal filtering.
# These base classes are extended by tool-specific configurations.
#
from dataclasses import dataclass
from typing import Optional


@dataclass
class BaseNetworkConfig:
    """Base configuration for network operations.

    Provides common defaults for timeout, retry, and result limits
    that are shared across Wikidata, Wayback, and other network tools.
    """
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 2.0
    max_results: int = 10


@dataclass
class TemporalConfig:
    """Configuration for temporal filtering.

    Used to constrain research results to specific date ranges.
    Supports both query-time filtering (Wikidata SPARQL) and
    post-hoc filtering (date extraction from web results).
    """
    time_filter: Optional[str] = None      # DDG time filter: "d", "w", "m", "y"
    date_after: Optional[str] = None       # ISO 8601: "2020-01-01"
    date_before: Optional[str] = None      # ISO 8601: "2024-01-01"
    strictness: str = "best_effort"        # "best_effort" | "strict"
    use_wayback: bool = False              # Use Wayback Machine for pre-date guarantees
