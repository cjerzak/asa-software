"""Shared schema/field-status normalization helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


def schema_leaf_paths(schema: Any, prefix: str = "") -> List[Tuple[str, Any]]:
    """Return dotted leaf paths for a JSON-like schema tree."""
    leaves: List[Tuple[str, Any]] = []
    if isinstance(schema, dict):
        for key, child in schema.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            leaves.extend(schema_leaf_paths(child, child_prefix))
        return leaves

    if isinstance(schema, list):
        if not schema:
            return leaves
        child_prefix = f"{prefix}[]" if prefix else "[]"
        leaves.extend(schema_leaf_paths(schema[0], child_prefix))
        return leaves

    if prefix:
        leaves.append((prefix, schema))
    return leaves


def field_key_aliases(path: str) -> List[str]:
    """Generate conservative aliases for field status path lookups."""
    if not path:
        return []
    clean = path.replace("[]", "")
    aliases = [clean]
    if "." in clean:
        aliases.append(clean.split(".")[-1])
    return aliases


def normalize_field_status_map(
    field_status: Any,
    expected_schema: Any = None,
    *,
    status_pending: str = "pending",
    valid_statuses: Optional[Iterable[str]] = None,
    is_empty_like: Optional[Callable[[Any], bool]] = None,
    normalize_url: Optional[Callable[[Any], Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Normalize and seed field status entries from optional expected schema."""
    normalized: Dict[str, Dict[str, Any]] = {}
    valid_status_set: Optional[Set[str]] = None
    if valid_statuses is not None:
        valid_status_set = {str(s).lower() for s in valid_statuses}

    def _empty(value: Any) -> bool:
        if is_empty_like is None:
            return value is None
        try:
            return bool(is_empty_like(value))
        except Exception:
            return value is None

    def _normalize_source(url: Any) -> Any:
        if normalize_url is None:
            return url
        try:
            return normalize_url(url)
        except Exception:
            return url

    if isinstance(field_status, dict):
        for key, value in field_status.items():
            key_str = str(key)
            if not key_str:
                continue
            entry = value if isinstance(value, dict) else {"value": value}
            status = str(entry.get("status") or status_pending).lower()
            if valid_status_set is not None and status not in valid_status_set:
                status = status_pending

            normalized_entry = {
                "status": status,
                "value": entry.get("value"),
                "source_url": _normalize_source(entry.get("source_url")),
                "evidence": None if entry.get("evidence") is None else str(entry.get("evidence")),
                "attempts": 0,
            }
            try:
                normalized_entry["attempts"] = max(0, int(entry.get("attempts", 0)))
            except Exception:
                normalized_entry["attempts"] = 0

            if normalized_entry["status"] == "found" and _empty(normalized_entry["value"]):
                normalized_entry["status"] = status_pending

            normalized[key_str] = normalized_entry

    if expected_schema is None:
        return normalized

    for path, descriptor in schema_leaf_paths(expected_schema):
        if "[]" in path:
            continue
        aliases = field_key_aliases(path)
        existing_key = next((alias for alias in aliases if alias in normalized), None)
        key = existing_key or path.replace("[]", "")
        entry = normalized.get(
            key,
            {
                "status": status_pending,
                "value": None,
                "source_url": None,
                "evidence": None,
                "attempts": 0,
            },
        )
        entry["descriptor"] = None if descriptor is None else str(descriptor)
        if entry.get("status") == "found" and _empty(entry.get("value")):
            entry["status"] = status_pending
        normalized[key] = entry

    return normalized


__all__ = [
    "schema_leaf_paths",
    "field_key_aliases",
    "normalize_field_status_map",
]
