"""Stable internal entrypoints for ASA backend consumers.

This module defines the *stable* Python surface that the R glue layer imports
via `reticulate::import_from_path("asa_backend.api.agent_api", ...)`.

It also provides small runtime helpers used by the R glue layer to make
LangGraph invocation more robust (e.g., ensuring recursion-limit exhaustion
returns a best-effort terminal state instead of raising).
"""

from __future__ import annotations

import os
import random
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

_RUNTIME_DEFAULTS: Dict[str, Any] = {
    "rate_limit": 0.1,
    "rate_limit_bucket_size": 10.0,
    "rate_limit_proactive": True,
    "humanize_timing": True,
    "jitter_factor": 0.5,
    "adaptive_enabled": True,
    "adaptive_window": 20,
    "adaptive_increase": 1.5,
    "adaptive_decrease": 0.9,
    "adaptive_max": 5.0,
    "adaptive_min": 0.5,
    "circuit_enabled": True,
    "circuit_threshold": 0.10,
    "circuit_window": 20,
    "circuit_cooldown": 60.0,
}

_RUNTIME_CONTROL: Optional[Dict[str, Any]] = None


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _as_float(value: Any, default: float, minimum: Optional[float] = None) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if minimum is not None:
        out = max(float(minimum), out)
    return out


def _as_int(value: Any, default: int, minimum: Optional[int] = None) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    return out


def _runtime_config(raw: Any = None) -> Dict[str, Any]:
    cfg = dict(_RUNTIME_DEFAULTS)
    if isinstance(raw, dict):
        cfg.update(raw)

    cfg["rate_limit"] = _as_float(cfg.get("rate_limit"), _RUNTIME_DEFAULTS["rate_limit"], minimum=1e-6)
    cfg["rate_limit_bucket_size"] = _as_float(
        cfg.get("rate_limit_bucket_size"),
        _RUNTIME_DEFAULTS["rate_limit_bucket_size"],
        minimum=1.0,
    )
    cfg["rate_limit_proactive"] = _as_bool(
        cfg.get("rate_limit_proactive"),
        _RUNTIME_DEFAULTS["rate_limit_proactive"],
    )
    cfg["humanize_timing"] = _as_bool(
        cfg.get("humanize_timing"),
        _RUNTIME_DEFAULTS["humanize_timing"],
    )
    cfg["jitter_factor"] = _as_float(
        cfg.get("jitter_factor"),
        _RUNTIME_DEFAULTS["jitter_factor"],
        minimum=0.0,
    )
    cfg["adaptive_enabled"] = _as_bool(
        cfg.get("adaptive_enabled"),
        _RUNTIME_DEFAULTS["adaptive_enabled"],
    )
    cfg["adaptive_window"] = _as_int(
        cfg.get("adaptive_window"),
        _RUNTIME_DEFAULTS["adaptive_window"],
        minimum=1,
    )
    cfg["adaptive_increase"] = _as_float(
        cfg.get("adaptive_increase"),
        _RUNTIME_DEFAULTS["adaptive_increase"],
        minimum=1.0,
    )
    cfg["adaptive_decrease"] = _as_float(
        cfg.get("adaptive_decrease"),
        _RUNTIME_DEFAULTS["adaptive_decrease"],
        minimum=0.1,
    )
    cfg["adaptive_max"] = _as_float(
        cfg.get("adaptive_max"),
        _RUNTIME_DEFAULTS["adaptive_max"],
        minimum=1.0,
    )
    cfg["adaptive_min"] = _as_float(
        cfg.get("adaptive_min"),
        _RUNTIME_DEFAULTS["adaptive_min"],
        minimum=0.1,
    )
    if cfg["adaptive_min"] > cfg["adaptive_max"]:
        cfg["adaptive_min"], cfg["adaptive_max"] = cfg["adaptive_max"], cfg["adaptive_min"]

    cfg["circuit_enabled"] = _as_bool(
        cfg.get("circuit_enabled"),
        _RUNTIME_DEFAULTS["circuit_enabled"],
    )
    cfg["circuit_threshold"] = min(
        1.0,
        _as_float(cfg.get("circuit_threshold"), _RUNTIME_DEFAULTS["circuit_threshold"], minimum=0.0),
    )
    cfg["circuit_window"] = _as_int(
        cfg.get("circuit_window"),
        _RUNTIME_DEFAULTS["circuit_window"],
        minimum=1,
    )
    cfg["circuit_cooldown"] = _as_float(
        cfg.get("circuit_cooldown"),
        _RUNTIME_DEFAULTS["circuit_cooldown"],
        minimum=0.0,
    )
    return cfg


def _new_rate_state(cfg: Dict[str, Any]) -> Dict[str, Any]:
    bucket_size = float(cfg["rate_limit_bucket_size"])
    return {
        "tokens": bucket_size,
        "bucket_size": bucket_size,
        "refill_rate": float(cfg["rate_limit"]),
        "last_refill": float(time.time()),
    }


def _new_adaptive_state(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "recent_results": [],
        "multiplier": 1.0,
        "success_streak": 0,
        "window_size": int(cfg["adaptive_window"]),
    }


def _new_circuit_state() -> Dict[str, Any]:
    return {
        "recent_results": [],
        "tripped": False,
        "tripped_at": None,
        "trip_count": 0,
    }


def _new_humanize_state() -> Dict[str, Any]:
    return {
        "session_start": None,
        "request_count": 0,
    }


def _runtime_status_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rate_limiter": {
            "tokens": float((state.get("rate") or {}).get("tokens", 0.0) or 0.0),
            "bucket_size": float((state.get("rate") or {}).get("bucket_size", 0.0) or 0.0),
            "refill_rate": float((state.get("rate") or {}).get("refill_rate", 0.0) or 0.0),
        },
        "adaptive": runtime_adaptive_status(),
        "circuit": runtime_circuit_status(),
    }


def _ensure_runtime_control() -> Dict[str, Any]:
    global _RUNTIME_CONTROL
    if _RUNTIME_CONTROL is None:
        cfg = _runtime_config()
        _RUNTIME_CONTROL = {
            "config": cfg,
            "rate": _new_rate_state(cfg),
            "adaptive": _new_adaptive_state(cfg),
            "circuit": _new_circuit_state(),
            "humanize": _new_humanize_state(),
            "clients": {"direct": None, "proxied": None},
        }
    return _RUNTIME_CONTROL


def runtime_control_init(config: Optional[dict] = None) -> Dict[str, Any]:
    """Initialize or refresh runtime control state used by the R glue layer."""
    global _RUNTIME_CONTROL
    current = _ensure_runtime_control()
    clients = dict(current.get("clients") or {"direct": None, "proxied": None})

    cfg = _runtime_config(config)
    _RUNTIME_CONTROL = {
        "config": cfg,
        "rate": _new_rate_state(cfg),
        "adaptive": _new_adaptive_state(cfg),
        "circuit": _new_circuit_state(),
        "humanize": _new_humanize_state(),
        "clients": {
            "direct": clients.get("direct"),
            "proxied": clients.get("proxied"),
        },
    }
    return _runtime_status_snapshot(_RUNTIME_CONTROL)


def runtime_control_reset() -> Dict[str, Any]:
    """Reset runtime control state and close registered HTTP clients."""
    state = _ensure_runtime_control()
    cfg = dict(state.get("config") or _runtime_config())
    runtime_clients_close()
    return runtime_control_init(config=cfg)


def _humanize_delay_runtime(base_delay: float) -> float:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("humanize_timing")) or base_delay <= 0:
        return max(0.0, float(base_delay))

    human = state.get("humanize") or _new_humanize_state()
    now = float(time.time())
    if human.get("session_start") is None:
        human["session_start"] = now
    human["request_count"] = int(human.get("request_count", 0)) + 1
    state["humanize"] = human

    log_factor = max(0.5, min(3.0, random.lognormvariate(0, 0.4)))
    micro_stutter = random.uniform(0.05, 0.2)
    session_minutes = max(0.0, (now - float(human["session_start"])) / 60.0)
    fatigue_factor = min(
        1.5,
        1.0 + (session_minutes * 0.01) + (float(human["request_count"]) * 0.001),
    )
    thinking_pause = random.uniform(0.5, 2.0) if random.random() < 0.05 else 0.0
    pre_commit_hesitation = random.uniform(0.02, 0.08)
    jitter_factor = max(0.0, float(cfg.get("jitter_factor", 0.0) or 0.0))
    jitter = random.uniform(0.0, max(0.0, base_delay * jitter_factor))

    delay = (
        float(base_delay) * log_factor * fatigue_factor
        + micro_stutter
        + thinking_pause
        + pre_commit_hesitation
        + jitter
    )
    return max(0.1, float(delay))


def runtime_rate_reset() -> None:
    state = _ensure_runtime_control()
    rate = state.get("rate") or _new_rate_state(state.get("config") or _runtime_config())
    rate["tokens"] = float(rate.get("bucket_size", 1.0) or 1.0)
    rate["last_refill"] = float(time.time())
    state["rate"] = rate


def runtime_rate_acquire(verbose: bool = False) -> float:
    """Acquire a token from the process-local runtime rate limiter."""
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("rate_limit_proactive")):
        return 0.0

    rate = state.get("rate") or _new_rate_state(cfg)
    now = float(time.time())
    elapsed = max(0.0, now - float(rate.get("last_refill", now)))
    refill_rate = max(1e-6, float(rate.get("refill_rate", cfg.get("rate_limit", 0.1))))
    bucket_size = max(1.0, float(rate.get("bucket_size", cfg.get("rate_limit_bucket_size", 10.0))))
    tokens_to_add = elapsed * refill_rate
    rate["tokens"] = min(bucket_size, float(rate.get("tokens", bucket_size)) + tokens_to_add)
    rate["last_refill"] = now

    wait_time = 0.0
    if float(rate["tokens"]) < 1.0:
        base_wait = (1.0 - float(rate["tokens"])) / refill_rate
        adaptive_multiplier = 1.0
        if bool(cfg.get("adaptive_enabled", True)):
            adaptive = state.get("adaptive") or {}
            adaptive_multiplier = _as_float(adaptive.get("multiplier"), 1.0, minimum=0.1)
        adjusted_wait = base_wait * adaptive_multiplier
        wait_time = _humanize_delay_runtime(adjusted_wait)
        if verbose:
            print(
                (
                    "  Rate limiter: waiting "
                    f"{wait_time:.1f}s (base={base_wait:.1f}s, adaptive={adaptive_multiplier:.2f}x, humanized)"
                )
            )
        if wait_time > 0:
            time.sleep(wait_time)
        rate["tokens"] = 1.0

    rate["tokens"] = max(0.0, float(rate.get("tokens", 0.0)) - 1.0)
    state["rate"] = rate
    return float(wait_time)


def runtime_adaptive_reset() -> Dict[str, Any]:
    state = _ensure_runtime_control()
    state["adaptive"] = _new_adaptive_state(state.get("config") or _runtime_config())
    return runtime_adaptive_status()


def runtime_adaptive_record(status: Any, verbose: bool = False) -> Dict[str, Any]:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("adaptive_enabled", True)):
        return runtime_adaptive_status()

    adaptive = state.get("adaptive") or _new_adaptive_state(cfg)
    token = str(status or "").strip().lower() or "error"
    adaptive["recent_results"] = list(adaptive.get("recent_results") or []) + [token]
    window_size = max(1, int(adaptive.get("window_size", cfg.get("adaptive_window", 20))))
    if len(adaptive["recent_results"]) > window_size:
        adaptive["recent_results"] = adaptive["recent_results"][-window_size:]

    if token == "success":
        adaptive["success_streak"] = int(adaptive.get("success_streak", 0)) + 1
    else:
        adaptive["success_streak"] = 0

    old_multiplier = _as_float(adaptive.get("multiplier"), 1.0, minimum=0.1)
    multiplier = old_multiplier
    if token in {"captcha", "blocked"}:
        multiplier = min(
            float(cfg.get("adaptive_max", 5.0)),
            old_multiplier * float(cfg.get("adaptive_increase", 1.5)),
        )
        if verbose and multiplier != old_multiplier:
            print(
                (
                    "Adaptive rate: INCREASED to "
                    f"{multiplier:.2f}x (was {old_multiplier:.2f}x) due to {token}"
                )
            )
    elif int(adaptive.get("success_streak", 0)) >= 10:
        multiplier = max(
            float(cfg.get("adaptive_min", 0.5)),
            old_multiplier * float(cfg.get("adaptive_decrease", 0.9)),
        )
        if verbose and multiplier != old_multiplier:
            print(
                (
                    "Adaptive rate: DECREASED to "
                    f"{multiplier:.2f}x (was {old_multiplier:.2f}x) after successes"
                )
            )
        adaptive["success_streak"] = 0

    adaptive["multiplier"] = float(multiplier)
    state["adaptive"] = adaptive
    return runtime_adaptive_status()


def runtime_adaptive_status() -> Dict[str, Any]:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("adaptive_enabled", True)):
        return {
            "enabled": False,
            "multiplier": 1.0,
            "success_streak": 0,
            "recent_count": 0,
        }

    adaptive = state.get("adaptive") or _new_adaptive_state(cfg)
    return {
        "enabled": True,
        "multiplier": _as_float(adaptive.get("multiplier"), 1.0, minimum=0.1),
        "success_streak": _as_int(adaptive.get("success_streak"), 0, minimum=0),
        "recent_count": len(list(adaptive.get("recent_results") or [])),
    }


def runtime_circuit_init() -> Dict[str, Any]:
    state = _ensure_runtime_control()
    state["circuit"] = _new_circuit_state()
    return runtime_circuit_status()


def runtime_circuit_record(status: Any, verbose: bool = False) -> bool:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("circuit_enabled", True)):
        return False

    circuit = state.get("circuit") or _new_circuit_state()
    if bool(circuit.get("tripped", False)):
        state["circuit"] = circuit
        return True

    token = str(status or "").strip().lower()
    token = "error" if token == "error" else "success"

    circuit["recent_results"] = list(circuit.get("recent_results") or []) + [token]
    window = max(1, int(cfg.get("circuit_window", 20)))
    if len(circuit["recent_results"]) > window:
        circuit["recent_results"] = circuit["recent_results"][-window:]

    if len(circuit["recent_results"]) >= 5:
        errors = sum(1 for item in circuit["recent_results"] if item == "error")
        error_rate = errors / float(len(circuit["recent_results"]))
        if error_rate > float(cfg.get("circuit_threshold", 0.10)):
            circuit["tripped"] = True
            circuit["tripped_at"] = float(time.time())
            circuit["trip_count"] = int(circuit.get("trip_count", 0)) + 1
            if verbose:
                print(
                    "Circuit breaker TRIPPED! "
                    f"Error rate: {error_rate * 100:.0f}% "
                    f"(threshold: {float(cfg.get('circuit_threshold', 0.10)) * 100:.0f}%)"
                )

    state["circuit"] = circuit
    return bool(circuit.get("tripped", False))


def runtime_circuit_check(verbose: bool = False) -> bool:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    if not bool(cfg.get("circuit_enabled", True)):
        return True

    circuit = state.get("circuit") or _new_circuit_state()
    if not bool(circuit.get("tripped", False)):
        state["circuit"] = circuit
        return True

    tripped_at = circuit.get("tripped_at")
    elapsed = max(0.0, float(time.time()) - float(tripped_at)) if tripped_at is not None else 0.0
    cooldown = max(0.0, float(cfg.get("circuit_cooldown", 60.0)))
    if elapsed >= cooldown:
        circuit["tripped"] = False
        circuit["recent_results"] = []
        state["circuit"] = circuit
        if verbose:
            print(f"Circuit breaker RESET after {elapsed:.0f}s cooldown")
        return True

    state["circuit"] = circuit
    return False


def runtime_circuit_status() -> Dict[str, Any]:
    state = _ensure_runtime_control()
    cfg = state.get("config") or _runtime_config()
    circuit = state.get("circuit") or _new_circuit_state()
    recent_results = list(circuit.get("recent_results") or [])
    error_count = sum(1 for item in recent_results if item == "error")
    error_rate = (error_count / float(len(recent_results))) if recent_results else 0.0
    return {
        "enabled": bool(cfg.get("circuit_enabled", True)),
        "tripped": bool(circuit.get("tripped", False)),
        "error_rate": float(error_rate),
        "recent_count": len(recent_results),
        "trip_count": _as_int(circuit.get("trip_count"), 0, minimum=0),
    }


def runtime_clients_register(direct: Any = None, proxied: Any = None) -> None:
    state = _ensure_runtime_control()
    clients = state.get("clients") or {"direct": None, "proxied": None}
    clients["direct"] = direct
    clients["proxied"] = proxied
    state["clients"] = clients


def _safe_close_client(client: Any) -> None:
    if client is None:
        return
    close_fn = getattr(client, "close", None)
    if callable(close_fn):
        try:
            close_fn()
        except Exception:
            return


def runtime_clients_close() -> None:
    state = _ensure_runtime_control()
    clients = state.get("clients") or {"direct": None, "proxied": None}
    _safe_close_client(clients.get("direct"))
    _safe_close_client(clients.get("proxied"))
    state["clients"] = {"direct": None, "proxied": None}


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
    # Runtime controls (used by R runtime wrappers)
    "runtime_control_init",
    "runtime_control_reset",
    "runtime_rate_acquire",
    "runtime_rate_reset",
    "runtime_adaptive_record",
    "runtime_adaptive_status",
    "runtime_adaptive_reset",
    "runtime_circuit_init",
    "runtime_circuit_record",
    "runtime_circuit_check",
    "runtime_circuit_status",
    "runtime_clients_register",
    "runtime_clients_close",
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
