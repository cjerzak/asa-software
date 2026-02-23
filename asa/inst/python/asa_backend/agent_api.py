"""Stable internal entrypoints for ASA backend consumers.

This module defines the *stable* Python surface that the R glue layer imports
via `reticulate::import_from_path("asa_backend.agent_api", ...)`.

It also provides small runtime helpers used by the R glue layer to make
LangGraph invocation more robust (e.g., ensuring recursion-limit exhaustion
returns a best-effort terminal state instead of raising).
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Optional

from langgraph.errors import GraphRecursionError
from state_utils import repair_json_output_to_schema as repair_json_output_to_schema

from .graph.core import (
    _exception_fallback_text,
    _reusable_terminal_finalize_response,
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

_HEARTBEAT_STATE_ENV = "ASA_PROGRESS_STATE_FILE"


def _state_get(state: Any, key: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(key, default)
    try:
        return getattr(state, key, default)
    except Exception:
        return default


def _nonneg_int(value: Any, default: int = 0) -> int:
    try:
        out = int(value)
    except Exception:
        return int(default)
    return out if out >= 0 else int(default)


def _sanitize_token(value: Any, default: str = "na", max_chars: int = 96) -> str:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    text = re.sub(r"\s+", "_", text)
    text = text.replace("|", "/").replace("=", "/")
    text = re.sub(r"[^A-Za-z0-9_./:-]", "", text)
    if not text:
        return default
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def _pending_tool_calls(message: Any) -> bool:
    if message is None:
        return False
    if isinstance(message, dict):
        return bool(message.get("tool_calls"))
    try:
        return bool(getattr(message, "tool_calls", None))
    except Exception:
        return False


def _last_node_name(token_trace: Any) -> str:
    if not isinstance(token_trace, list) or not token_trace:
        return "na"
    last = token_trace[-1]
    if isinstance(last, dict):
        return _sanitize_token(last.get("node") or last.get("name"), default="na")
    try:
        return _sanitize_token(getattr(last, "node", None) or getattr(last, "name", None), default="na")
    except Exception:
        return "na"


def _build_progress_line(
    state: Any,
    *,
    phase: str,
    chunk_index: int,
    error_text: str = "",
) -> str:
    messages = _state_get(state, "messages", [])
    message_count = len(messages) if isinstance(messages, list) else 0
    pending_calls = False
    if isinstance(messages, list) and message_count > 0:
        pending_calls = _pending_tool_calls(messages[-1])

    budget_state = _state_get(state, "budget_state", {})
    if not isinstance(budget_state, dict):
        budget_state = {}

    diagnostics = _state_get(state, "diagnostics", {})
    if not isinstance(diagnostics, dict):
        diagnostics = {}

    tool_limit = _nonneg_int(
        budget_state.get("tool_calls_limit_effective", budget_state.get("tool_calls_limit", 0))
    )
    tool_used = _nonneg_int(budget_state.get("tool_calls_used", 0))
    tool_remaining = _nonneg_int(
        budget_state.get("tool_calls_remaining", max(0, tool_limit - tool_used))
    )
    resolved_fields = _nonneg_int(budget_state.get("resolved_fields", 0))
    total_fields = _nonneg_int(budget_state.get("total_fields", 0))
    unknown_fields = _nonneg_int(
        budget_state.get("unknown_fields", budget_state.get("unknown_fields_count", 0))
    )
    if unknown_fields <= 0:
        unknown_fields = _nonneg_int(
            diagnostics.get("unknown_fields_count_current", diagnostics.get("unknown_fields_count", 0))
        )

    token_trace = _state_get(state, "token_trace", [])
    node_name = _last_node_name(token_trace)
    stop_reason = _sanitize_token(_state_get(state, "stop_reason", "none"), default="none")
    final_emitted = 1 if bool(_state_get(state, "final_emitted", False)) else 0
    error_token = _sanitize_token(error_text, default="none")

    fields = [
        f"ts={int(time.time())}",
        f"phase={_sanitize_token(phase, default='unknown')}",
        f"chunk={_nonneg_int(chunk_index)}",
        f"node={node_name}",
        f"messages={_nonneg_int(message_count)}",
        f"pending_tool_calls={1 if pending_calls else 0}",
        f"tool_used={tool_used}",
        f"tool_limit={tool_limit}",
        f"tool_rem={tool_remaining}",
        f"resolved={resolved_fields}",
        f"total={total_fields}",
        f"unknown={unknown_fields}",
        f"stop_reason={stop_reason}",
        f"final_emitted={final_emitted}",
        f"error={error_token}",
    ]
    return " ".join(fields)


def _emit_progress_line(
    progress_path: str,
    state: Any,
    *,
    phase: str,
    chunk_index: int,
    error_text: str = "",
) -> None:
    if not progress_path:
        return
    try:
        line = _build_progress_line(
            state,
            phase=phase,
            chunk_index=chunk_index,
            error_text=error_text,
        )
        tmp_path = f"{progress_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(line + "\n")
        os.replace(tmp_path, progress_path)
    except Exception:
        # Heartbeat diagnostics are best-effort only.
        return


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
    progress_path = str(os.environ.get(_HEARTBEAT_STATE_ENV, "") or "").strip()
    chunk_index = 0
    _emit_progress_line(
        progress_path,
        initial_state,
        phase="start",
        chunk_index=chunk_index,
    )
    last_state = None
    try:
        stream = getattr(graph, "stream", None)
        if callable(stream):
            for chunk in stream(initial_state, config=config, stream_mode=stream_mode):
                chunk_index += 1
                last_state = chunk
                _emit_progress_line(
                    progress_path,
                    chunk,
                    phase="stream",
                    chunk_index=chunk_index,
                )
        else:
            # Fallback for unexpected graph types.
            invoked = graph.invoke(initial_state, config=config)
            _emit_progress_line(
                progress_path,
                invoked,
                phase="invoke_return",
                chunk_index=chunk_index,
            )
            return invoked

        if last_state is None:
            # Some graphs may not yield values; fall back to invoke().
            invoked = graph.invoke(initial_state, config=config)
            _emit_progress_line(
                progress_path,
                invoked,
                phase="invoke_return",
                chunk_index=chunk_index,
            )
            return invoked
        _emit_progress_line(
            progress_path,
            last_state,
            phase="complete",
            chunk_index=chunk_index,
        )
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
                    terminal = _reusable_terminal_finalize_response(messages, expected_schema=expected_schema)
                except Exception:
                    terminal = None
                if terminal is None:
                    try:
                        fallback_text = _exception_fallback_text(
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

        _emit_progress_line(
            progress_path,
            state,
            phase="recursion_limit",
            chunk_index=chunk_index,
            error_text=str(exc)[:max_error_chars] if str(exc) else "GraphRecursionError",
        )
        return state
    except Exception as exc:
        _emit_progress_line(
            progress_path,
            last_state if last_state is not None else initial_state,
            phase="error",
            chunk_index=chunk_index,
            error_text=str(exc)[:max_error_chars] if str(exc) else "invoke_error",
        )
        raise


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
    # Utility hook for test monkeypatching / guarded repair paths.
    "repair_json_output_to_schema",
]
