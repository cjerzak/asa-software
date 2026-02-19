"""Search configuration primitives."""

from __future__ import annotations

from ._legacy_transport import SearchConfig, configure_logging, configure_search

__all__ = [
    "SearchConfig",
    "configure_search",
    "configure_logging",
]
