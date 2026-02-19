"""Backward-compatible shim for LangGraph agent construction.

The bulk of the implementation lives in `asa_backend.graph._legacy_agent_graph`.
This module keeps the historical import path (`asa_backend.agent_graph`) stable
while allowing the codebase to be modularized over time.
"""

from __future__ import annotations

from state_utils import repair_json_output_to_schema as repair_json_output_to_schema

from .graph import _legacy_agent_graph as _legacy

# Explicitly re-export the main graph constructors (used by the R glue layer).
create_memory_folding_agent = _legacy.create_memory_folding_agent
create_standard_agent = _legacy.create_standard_agent
create_memory_folding_agent_with_checkpointer = _legacy.create_memory_folding_agent_with_checkpointer


def __getattr__(name: str):
    return getattr(_legacy, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_legacy)))


__all__ = [
    "create_memory_folding_agent",
    "create_standard_agent",
    "create_memory_folding_agent_with_checkpointer",
    "repair_json_output_to_schema",
]
