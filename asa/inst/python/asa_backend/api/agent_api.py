"""Stable internal entrypoints for ASA backend consumers.

This module defines the *stable* Python surface that the R glue layer imports
via `reticulate::import_from_path("asa_backend.api.agent_api", ...)`.

It also provides small runtime helpers used by the R glue layer to make
LangGraph invocation more robust (e.g., ensuring recursion-limit exhaustion
returns a best-effort terminal state instead of raising).
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from langgraph.errors import GraphRecursionError
from shared.state_graph_utils import repair_json_output_to_schema as repair_json_output_to_schema

from ..graph.agent_graph_core import (
    _exception_fallback_text,
    _reusable_terminal_finalize_response,
    create_memory_folding_agent,
    create_memory_folding_agent_with_checkpointer,
    create_standard_agent,
)
from ..search import (
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
ASA_BRIDGE_SCHEMA_VERSION = "asa_bridge_contract_v1"

_CONTRACT_DEFAULTS: Dict[str, Any] = {
    "trace_metadata": {},
    "budget_state": {},
    "field_status": {},
    "diagnostics": {},
    "json_repair": [],
    "completion_gate": {},
    "retrieval_metrics": {},
    "tool_quality_events": [],
    "candidate_resolution": {},
    "finalization_status": {},
    "orchestration_options": {},
    "artifact_status": {},
    "fold_stats": {},
    "plan": {},
    "plan_history": [],
    "om_stats": {},
    "observations": [],
    "reflections": [],
    "token_trace": [],
    "tokens_used": 0,
    "input_tokens": 0,
    "output_tokens": 0,
    "stop_reason": None,
    "policy_version": None,
}


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


def _collect_contract_fields(state: Any) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for key, default in _CONTRACT_DEFAULTS.items():
        value = _state_get(state, key, default)
        if value is None and default is not None:
            value = default
        fields[key] = value
    return fields


def _safe_config_snapshot(config_snapshot: Any, config: Optional[dict]) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    if isinstance(config_snapshot, dict):
        snapshot.update(config_snapshot)
    elif config_snapshot is not None:
        snapshot["value"] = str(config_snapshot)

    if isinstance(config, dict):
        snapshot.setdefault("invoke_config", config)
    return snapshot


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_trace_metadata(
    state: Any,
    *,
    config: Optional[dict] = None,
    config_snapshot: Optional[dict] = None,
) -> Dict[str, Any]:
    diagnostics = _state_get(state, "diagnostics", {}) or {}
    if not isinstance(diagnostics, dict):
        diagnostics = {}
    retrieval_metrics = _state_get(state, "retrieval_metrics", {}) or {}
    if not isinstance(retrieval_metrics, dict):
        retrieval_metrics = {}
    completion_gate = _state_get(state, "completion_gate", {}) or {}
    if not isinstance(completion_gate, dict):
        completion_gate = {}
    budget_state = _state_get(state, "budget_state", {}) or {}
    if not isinstance(budget_state, dict):
        budget_state = {}
    orchestration_options = _state_get(state, "orchestration_options", {}) or {}
    if not isinstance(orchestration_options, dict):
        orchestration_options = {}

    policy_version = _state_get(state, "policy_version", None)
    if not policy_version and isinstance(orchestration_options, dict):
        policy_version = orchestration_options.get("policy_version")

    recovery_reason_counts = diagnostics.get("recovery_reason_counts", {})
    if not isinstance(recovery_reason_counts, dict):
        recovery_reason_counts = {}

    safe_snapshot = _safe_config_snapshot(config_snapshot, config)
    invoke_config = safe_snapshot.get("invoke_config", {})
    if not isinstance(invoke_config, dict):
        invoke_config = {}

    model_name = str(
        invoke_config.get("model")
        or invoke_config.get("model_name")
        or invoke_config.get("deployment")
        or ""
    ).strip() or None
    backend_name = str(
        invoke_config.get("backend")
        or invoke_config.get("provider")
        or ""
    ).strip() or None

    return {
        "schema_version": "trace_metadata_v1",
        "generated_at_utc": _iso_utc_now(),
        "thread_id": _state_get(state, "thread_id", None),
        "policy_version": policy_version,
        "backend": backend_name,
        "model": model_name,
        "stop_reason": _state_get(state, "stop_reason", None),
        "completion_status": completion_gate.get("completion_status"),
        "finalization_invariant_failed": bool(
            completion_gate.get("finalization_invariant_failed", False)
        ),
        "finalization_invariant_reasons": list(
            completion_gate.get("finalization_invariant_reasons") or []
        )[:16],
        "unknown_fields_count_current": _nonneg_int(
            diagnostics.get("unknown_fields_count_current", diagnostics.get("unknown_fields_count", 0))
        ),
        "recovery_blocked_anchor_mismatch_count": _nonneg_int(
            recovery_reason_counts.get("recovery_blocked_anchor_mismatch", 0)
        ),
        "search_calls": _nonneg_int(retrieval_metrics.get("search_calls", 0)),
        "query_dedupe_hits": _nonneg_int(retrieval_metrics.get("query_dedupe_hits", 0)),
        "empty_round_streak": _nonneg_int(retrieval_metrics.get("empty_round_streak", 0)),
        "tool_calls_used": _nonneg_int(budget_state.get("tool_calls_used", 0)),
        "tool_calls_limit_effective": _nonneg_int(
            budget_state.get("tool_calls_limit_effective", budget_state.get("tool_calls_limit", 0))
        ),
        "tool_calls_remaining": _nonneg_int(budget_state.get("tool_calls_remaining", 0)),
    }


def invoke_graph_with_payload(
    graph: Any,
    initial_state: Any,
    config: Optional[dict] = None,
    *,
    stream_mode: str = "values",
    max_error_chars: int = 500,
    config_snapshot: Optional[dict] = None,
) -> Dict[str, Any]:
    """Invoke a graph and return a versioned bridge payload for R callers."""
    invoke_started = time.time()
    state = invoke_graph_safely(
        graph,
        initial_state,
        config=config,
        stream_mode=stream_mode,
        max_error_chars=max_error_chars,
    )
    invoke_finished = time.time()
    duration_s = max(0.0, float(invoke_finished - invoke_started))
    contract_fields = _collect_contract_fields(state)
    trace_metadata = _build_trace_metadata(
        state,
        config=config,
        config_snapshot=config_snapshot,
    )

    payload: Dict[str, Any] = {
        "schema_version": ASA_BRIDGE_SCHEMA_VERSION,
        "response": state,
        "diagnostics": _state_get(state, "diagnostics", {}) or {},
        "trace_metadata": trace_metadata,
        "phase_timings": {
            "invoke_graph_seconds": duration_s,
            "invoke_graph_minutes": duration_s / 60.0,
        },
        "config_snapshot": _safe_config_snapshot(config_snapshot, config),
    }
    payload.update(contract_fields)
    payload["trace_metadata"] = trace_metadata
    return payload


__all__ = [
    # Runtime guards
    "invoke_graph_safely",
    "invoke_graph_with_payload",
    "ASA_BRIDGE_SCHEMA_VERSION",
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
