"""Backward-compatible shim for the search subsystem.

Historically, ASA exposed a large `search_transport.py` module containing:
  - DuckDuckGo tiered transport (ddgs/selenium/requests)
  - Tor routing + registry
  - Anti-detection timing helpers
  - Result normalization + ranking helpers

The implementation now lives in `asa_backend.search.*`. This module keeps
legacy import paths working for internal modules and external callers.
"""

from __future__ import annotations

# Stable public surface (preferred imports)
from .search.config import SearchConfig, configure_logging, configure_search
from .search.langchain_wrappers import (
    BrowserDuckDuckGoSearchAPIWrapper,
    BrowserDuckDuckGoSearchRun,
    PatchedDuckDuckGoSearchAPIWrapper,
    PatchedDuckDuckGoSearchRun,
)
from .search.tor import configure_tor, configure_tor_registry
from .search.anti_detection import configure_anti_detection

# Legacy/private surface: re-export everything from the legacy implementation so
# internal compatibility modules can continue importing underscore helpers.
from .search._legacy_transport import *  # noqa: F401,F403

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

