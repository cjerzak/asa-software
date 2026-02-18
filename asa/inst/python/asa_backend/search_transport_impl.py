"""Compatibility alias for transitional transport import paths."""

from . import search_transport as _search_transport

for _name in dir(_search_transport):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_search_transport, _name)

__all__ = [name for name in globals() if not name.startswith("__")]
