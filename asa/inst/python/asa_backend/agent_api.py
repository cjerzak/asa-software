"""Stable internal entrypoints for ASA backend consumers.

This module re-exports the full legacy backend surface from `agent_graph`
so call sites can migrate off `custom_ddg_production` without behavior changes.
"""

from . import agent_graph as _agent_graph

for _name in dir(_agent_graph):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_agent_graph, _name)

__all__ = [name for name in globals() if not name.startswith("__")]
