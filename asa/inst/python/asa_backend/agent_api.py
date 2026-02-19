"""Stable internal entrypoints for ASA backend consumers.

This module defines the *stable* Python surface that the R glue layer imports
via `reticulate::import_from_path("asa_backend.agent_api", ...)`.

It also provides small runtime helpers used by the R glue layer to make
LangGraph invocation more robust (e.g., ensuring recursion-limit exhaustion
returns a best-effort terminal state instead of raising).
"""

from __future__ import annotations

import warnings
from typing import Any, Optional

from langgraph.errors import GraphRecursionError

from . import agent_graph as _agent_graph
from .agent_graph import (
    create_memory_folding_agent,
    create_memory_folding_agent_with_checkpointer,
    create_standard_agent,
)
from .search import (
    BrowserDuckDuckGoSearchAPIWrapper,
    BrowserDuckDuckGoSearchRun,
    PatchedDuckDuckGoSearchAPIWrapper,
    PatchedDuckDuckGoSearchRun,
    SearchConfig,
    configure_anti_detection,
    configure_logging,
    configure_search,
    configure_tor,
    configure_tor_registry,
)


def invoke_graph_safely(
    graph: Any,
    initial_state: Any,
    config: Optional[dict] = None,
    *,
    stream_mode: str = "values",
    max_error_chars: int = 500,
) -> Any:
    """Invoke a compiled LangGraph graph, returning last known state on recursion exhaustion.

    Why this exists:
      - LangGraph raises GraphRecursionError when recursion_limit is reached
        without hitting END.
      - In production we prefer returning the last yielded state (with a
        best-effort terminal assistant message) so callers can still serialize
        traces and extract partial results.

    Notes:
      - Only GraphRecursionError is caught. Other exceptions propagate.
      - Uses graph.stream(stream_mode="values") so we can retain last_state.
    """
    last_state = None
    try:
        stream = getattr(graph, "stream", None)
        if callable(stream):
            for chunk in stream(initial_state, config=config, stream_mode=stream_mode):
                last_state = chunk
        else:
            # Fallback for unexpected graph types.
            return graph.invoke(initial_state, config=config)

        if last_state is None:
            # Some graphs may not yield values; fall back to invoke().
            return graph.invoke(initial_state, config=config)
        return last_state
    except GraphRecursionError as exc:
        state = last_state
        # Best-effort: normalize to a mutable dict-like object.
        try:
            if state is None:
                state = {}
            if not hasattr(state, "get") and isinstance(state, dict) is False:
                state = {"value": state}
        except Exception:
            state = {}

        try:
            # Stamp stop_reason and a short error string for downstream diagnostics.
            if isinstance(state, dict):
                state.setdefault("stop_reason", "recursion_limit")
                state["error"] = str(exc)[:max_error_chars] if str(exc) else "GraphRecursionError"
            else:
                try:
                    setattr(state, "stop_reason", getattr(state, "stop_reason", None) or "recursion_limit")
                    setattr(state, "error", str(exc)[:max_error_chars] if str(exc) else "GraphRecursionError")
                except Exception:
                    pass

            # If we have ASA-like state with messages, ensure we end with a terminal
            # assistant message (no pending tool calls).
            messages = state.get("messages") if isinstance(state, dict) else getattr(state, "messages", None)
            expected_schema = state.get("expected_schema") if isinstance(state, dict) else getattr(state, "expected_schema", None)
            field_status = state.get("field_status") if isinstance(state, dict) else getattr(state, "field_status", None)
            if isinstance(messages, list) and messages:
                terminal = None
                try:
                    terminal = _agent_graph._reusable_terminal_finalize_response(messages, expected_schema=expected_schema)
                except Exception:
                    terminal = None
                if terminal is None:
                    try:
                        fallback_text = _agent_graph._exception_fallback_text(
                            expected_schema,
                            context="agent",
                            messages=messages,
                            field_status=field_status,
                        )
                    except Exception:
                        fallback_text = "Unable to finalize due to recursion limit."

                    try:
                        from langchain_core.messages import AIMessage

                        messages.append(AIMessage(content=str(fallback_text)))
                    except Exception:
                        messages.append({"role": "assistant", "content": str(fallback_text)})

                    if isinstance(state, dict):
                        state["messages"] = messages
                    else:
                        try:
                            setattr(state, "messages", messages)
                        except Exception:
                            pass
        except Exception:
            # Never raise from the guard itself.
            pass

        return state


_WARNED_DEPRECATED: set[str] = set()


def __getattr__(name: str):
    """Compatibility fallback for legacy callers.

    Historically, `agent_api` re-exported nearly everything from `agent_graph`
    (and, indirectly, parts of the search transport). We keep a soft fallback
    so internal symbols can be migrated gradually without breaking callers.
    """
    if hasattr(_agent_graph, name):
        if name not in _WARNED_DEPRECATED:
            warnings.warn(
                f"`asa_backend.agent_api.{name}` is deprecated; import from `asa_backend.agent_graph` "
                "or the appropriate `asa_backend.graph.*` module instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            _WARNED_DEPRECATED.add(name)
        return getattr(_agent_graph, name)

    try:
        from . import search_transport as _search_transport
    except Exception:
        _search_transport = None

    if _search_transport is not None and hasattr(_search_transport, name):
        if name not in _WARNED_DEPRECATED:
            warnings.warn(
                f"`asa_backend.agent_api.{name}` is deprecated; import from `asa_backend.search_transport` "
                "or `asa_backend.search.*` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            _WARNED_DEPRECATED.add(name)
        return getattr(_search_transport, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    names = set(__all__)
    names.update(dir(_agent_graph))
    try:
        from . import search_transport as _search_transport

        names.update(dir(_search_transport))
    except Exception:
        pass
    return sorted(names)


__all__ = [
    # Runtime guards
    "invoke_graph_safely",
    # Graph constructors (used by R)
    "create_memory_folding_agent",
    "create_standard_agent",
    "create_memory_folding_agent_with_checkpointer",
    # Search subsystem (used by R)
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
