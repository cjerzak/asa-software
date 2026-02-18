# outcome_gate.py
#
# Deterministic outcome-verification helpers shared by agent graphs.
#
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from asa_backend.schema_state import (
    field_key_aliases as _field_key_aliases,
    schema_leaf_paths as _flatten_schema_leaf_paths,
)

def _normalize_field_status_map(field_status: Any) -> Dict[str, Dict[str, Any]]:
    """Best-effort conversion to a canonical field_status dictionary."""
    if not isinstance(field_status, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in field_status.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, dict):
            out[key] = value
        else:
            out[key] = {"status": str(value)}
    return out


def _lookup_field_entry(field_status: Dict[str, Dict[str, Any]], path: str) -> Optional[Dict[str, Any]]:
    """Resolve a schema path to a field_status entry using conservative aliases."""
    if not path:
        return None
    for alias in (path, *_field_key_aliases(path)):
        entry = field_status.get(alias)
        if isinstance(entry, dict):
            return entry
    return None


def _status_label(entry: Optional[Dict[str, Any]]) -> str:
    if not isinstance(entry, dict):
        return "missing"
    return str(entry.get("status") or "missing").strip().lower()


def evaluate_schema_outcome(
    *,
    expected_schema: Any,
    field_status: Any,
    budget_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Deterministically verify whether schema completion criteria were met."""
    normalized_field_status = _normalize_field_status_map(field_status)
    leaf_paths = _flatten_schema_leaf_paths(expected_schema)

    if not leaf_paths:
        budget = budget_state if isinstance(budget_state, dict) else {}
        exhausted = bool(budget.get("budget_exhausted", False))
        return {
            "done": False,
            "completion_status": "partial" if exhausted else "in_progress",
            "resolved_fields": 0,
            "unknown_fields": 0,
            "unresolved_fields": 0,
            "total_fields": 0,
            "missing_fields": [],
            "budget_exhausted": exhausted,
            "reason": "no_schema",
        }

    resolved_fields = 0
    unknown_fields = 0
    missing_fields: List[str] = []

    for path, _descriptor in leaf_paths:
        entry = _lookup_field_entry(normalized_field_status, path)
        status = _status_label(entry)
        if status == "found":
            resolved_fields += 1
            continue
        if status == "unknown":
            resolved_fields += 1
            unknown_fields += 1
            continue
        missing_fields.append(path)

    total_fields = len(leaf_paths)
    unresolved_fields = len(missing_fields)
    done = unresolved_fields == 0 and total_fields > 0

    budget = budget_state if isinstance(budget_state, dict) else {}
    budget_exhausted = bool(budget.get("budget_exhausted", False))
    if done:
        completion_status = "complete"
        reason = "all_required_fields_resolved"
    elif budget_exhausted:
        completion_status = "partial"
        reason = "budget_exhausted_before_resolution"
    else:
        completion_status = "in_progress"
        reason = "missing_required_fields"

    return {
        "done": done,
        "completion_status": completion_status,
        "resolved_fields": resolved_fields,
        "unknown_fields": unknown_fields,
        "unresolved_fields": unresolved_fields,
        "total_fields": total_fields,
        "missing_fields": missing_fields,
        "budget_exhausted": budget_exhausted,
        "reason": reason,
    }


_RESEARCH_PARTIAL_STOP_REASONS = {
    "budget_time",
    "budget_queries",
    "budget_tokens",
    "max_rounds",
    "novelty_plateau",
    "recursion_limit",
}


def classify_research_completion(
    *,
    stop_reason: Optional[str],
    target_items: Optional[int],
    items_found: int,
) -> str:
    """Classify final research status based on deterministic success criteria."""
    reason = str(stop_reason or "").strip().lower()

    if target_items is not None and items_found >= int(target_items):
        return "complete"
    if reason == "target_reached":
        return "complete"
    if reason in _RESEARCH_PARTIAL_STOP_REASONS:
        return "partial"
    if reason == "":
        return "searching"
    return "complete"


def evaluate_research_outcome(
    *,
    round_number: int,
    queries_used: int,
    tokens_used: int,
    elapsed_sec: float,
    items_found: int,
    novelty_history: Optional[List[float]] = None,
    max_rounds: Optional[int] = None,
    budget_queries: Optional[int] = None,
    budget_tokens: Optional[int] = None,
    budget_time_sec: Optional[int] = None,
    target_items: Optional[int] = None,
    plateau_rounds: Optional[int] = None,
    novelty_min: Optional[float] = None,
    recursion_stop: bool = False,
) -> Dict[str, Any]:
    """Deterministically evaluate research stop conditions and completion class."""
    novelty_history = novelty_history or []
    stop_reason = None
    missing_constraints: List[str] = []

    if target_items is not None and items_found >= int(target_items):
        stop_reason = "target_reached"
    elif budget_time_sec and elapsed_sec >= float(budget_time_sec):
        stop_reason = "budget_time"
    elif budget_queries and queries_used >= int(budget_queries):
        stop_reason = "budget_queries"
    elif budget_tokens and tokens_used >= int(budget_tokens):
        stop_reason = "budget_tokens"
    elif max_rounds and round_number >= int(max_rounds):
        stop_reason = "max_rounds"
    elif (
        plateau_rounds
        and novelty_min is not None
        and len(novelty_history) >= int(plateau_rounds)
        and all(float(rate) < float(novelty_min) for rate in novelty_history[-int(plateau_rounds):])
    ):
        stop_reason = "novelty_plateau"
    elif recursion_stop:
        stop_reason = "recursion_limit"

    if target_items is not None and items_found < int(target_items):
        missing_constraints.append(
            f"target_items:{items_found}/{int(target_items)}"
        )

    should_stop = stop_reason is not None
    completion_status = classify_research_completion(
        stop_reason=stop_reason,
        target_items=target_items,
        items_found=items_found,
    )
    goal_met = completion_status == "complete"

    return {
        "should_stop": should_stop,
        "stop_reason": stop_reason,
        "completion_status": completion_status,
        "goal_met": goal_met,
        "missing_constraints": missing_constraints,
    }
