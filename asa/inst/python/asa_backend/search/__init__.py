"""Search subsystem (transport, Tor, ranking, wrappers)."""

from .search_config import SearchConfig, configure_logging, configure_search
from .langchain_adapters import (
    BrowserDuckDuckGoSearchAPIWrapper,
    BrowserDuckDuckGoSearchRun,
    PatchedDuckDuckGoSearchAPIWrapper,
    PatchedDuckDuckGoSearchRun,
)
from .tor_config import configure_tor, configure_tor_registry
from .anti_detection_config import configure_anti_detection

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
