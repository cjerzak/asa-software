"""Shared helpers for robust LangGraph invocation across ASA workflows."""

from __future__ import annotations

from typing import Any, Callable, Optional

from langgraph.errors import GraphRecursionError


ProgressEmitter = Callable[..., None]


def _state_get(state: Any, key: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(key, default)
    try:
        return getattr(state, key)
    except Exception:
        return default


def _emit_progress(
    emit_progress_line: Optional[ProgressEmitter],
    progress_path: str,
    state: Any,
    *,
    phase: str,
    chunk_index: int,
    error_text: str = "",
) -> None:
    if not callable(emit_progress_line):
        return
    try:
        emit_progress_line(
            progress_path,
            state,
            phase=phase,
            chunk_index=chunk_index,
            error_text=error_text,
        )
    except Exception:
        # Progress emission is best-effort only.
        return


def _coerce_mutable_state(state: Any) -> Any:
    try:
        if state is None:
            return {}
        if not hasattr(state, "get") and not isinstance(state, dict):
            return {"value": state}
        return state
    except Exception:
        return {}


def _best_effort_finalize_state(state: Any, exc: Exception, max_error_chars: int) -> Any:
    state = _coerce_mutable_state(state)

    try:
        error_text = str(exc)[:max_error_chars] if str(exc) else "GraphRecursionError"
        if isinstance(state, dict):
            state.setdefault("stop_reason", "recursion_limit")
            state["error"] = error_text
        else:
            try:
                setattr(state, "stop_reason", getattr(state, "stop_reason", None) or "recursion_limit")
                setattr(state, "error", error_text)
            except Exception:
                pass

        messages = state.get("messages") if isinstance(state, dict) else getattr(state, "messages", None)
        expected_schema = state.get("expected_schema") if isinstance(state, dict) else getattr(state, "expected_schema", None)
        field_status = state.get("field_status") if isinstance(state, dict) else getattr(state, "field_status", None)
        if not isinstance(messages, list) or not messages:
            return state

        try:
            from asa_backend.graph.agent_graph_core import (
                _exception_fallback_text,
                _reusable_terminal_finalize_response,
            )
        except Exception:
            return state

        terminal = None
        try:
            terminal = _reusable_terminal_finalize_response(messages, expected_schema=expected_schema)
        except Exception:
            terminal = None
        if terminal is not None:
            return state

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
    return state


def invoke_graph_safely(
    graph: Any,
    initial_state: Any,
    config: Optional[dict] = None,
    *,
    stream_mode: str = "values",
    max_error_chars: int = 500,
    progress_path: str = "",
    emit_progress_line: Optional[ProgressEmitter] = None,
) -> Any:
    """Invoke a compiled LangGraph graph, preserving last state on recursion exhaustion."""
    chunk_index = 0
    _emit_progress(
        emit_progress_line,
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
                _emit_progress(
                    emit_progress_line,
                    progress_path,
                    chunk,
                    phase="stream",
                    chunk_index=chunk_index,
                )
        else:
            invoked = graph.invoke(initial_state, config=config)
            _emit_progress(
                emit_progress_line,
                progress_path,
                invoked,
                phase="invoke_return",
                chunk_index=chunk_index,
            )
            return invoked

        if last_state is None:
            invoked = graph.invoke(initial_state, config=config)
            _emit_progress(
                emit_progress_line,
                progress_path,
                invoked,
                phase="invoke_return",
                chunk_index=chunk_index,
            )
            return invoked

        _emit_progress(
            emit_progress_line,
            progress_path,
            last_state,
            phase="complete",
            chunk_index=chunk_index,
        )
        return last_state
    except GraphRecursionError as exc:
        state = _best_effort_finalize_state(last_state, exc, max_error_chars=max_error_chars)
        error_text = str(exc)[:max_error_chars] if str(exc) else "GraphRecursionError"
        _emit_progress(
            emit_progress_line,
            progress_path,
            state,
            phase="recursion_limit",
            chunk_index=chunk_index,
            error_text=error_text,
        )
        return state
    except Exception as exc:
        error_text = str(exc)[:max_error_chars] if str(exc) else "invoke_error"
        _emit_progress(
            emit_progress_line,
            progress_path,
            last_state if last_state is not None else initial_state,
            phase="error",
            chunk_index=chunk_index,
            error_text=error_text,
        )
        raise


__all__ = ["invoke_graph_safely"]
