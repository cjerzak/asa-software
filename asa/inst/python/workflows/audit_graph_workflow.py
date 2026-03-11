# audit_graph.py
#
# LangGraph pipeline for auditing enumeration results.
# Performs completeness, consistency, gap, and anomaly checks.
#
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Annotated, Dict, List, Optional, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph

from shared.state_graph_utils import add_to_list, hash_result, merge_dicts, parse_llm_json

logger = logging.getLogger(__name__)
_AUDIT_DEFAULT_CHECKS = ["completeness", "consistency", "gaps", "anomalies"]
_AUDIT_REPRESENTATIVE_ROW_LIMIT = 25


# ────────────────────────────────────────────────────────────────────────
# State Definitions
# ────────────────────────────────────────────────────────────────────────
class AuditIssue(TypedDict):
    """A single audit issue."""
    severity: str  # "critical", "high", "medium", "low"
    description: str
    affected_rows: List[int]
    check_type: str


class FlaggedRow(TypedDict):
    """A flagged row with audit annotation."""
    index: int
    flag: str  # "ok", "warning", "suspect"
    note: str
    confidence: Optional[float]


class AuditState(TypedDict):
    """Main state for audit graph."""
    # Input
    data: List[Dict[str, Any]]
    query: str
    schema: Dict[str, str]
    known_universe: Optional[List[str]]
    checks: List[str]
    confidence_threshold: float
    dataset_context: Dict[str, Any]

    # Results (accumulated)
    issues: Annotated[List[AuditIssue], add_to_list]
    flagged_rows: Annotated[List[FlaggedRow], add_to_list]
    recommendations: Annotated[List[str], add_to_list]

    # Scores
    completeness_score: float
    consistency_score: float

    # Control
    summary: str
    status: str  # "checking", "complete", "failed"
    errors: Annotated[List[Dict], add_to_list]


def _normalize_audit_checks(checks: Optional[List[str]]) -> List[str]:
    """Normalize checks into a plain list of strings."""
    if checks is None:
        return list(_AUDIT_DEFAULT_CHECKS)

    if isinstance(checks, (list, tuple, set)):
        items = list(checks)
    else:
        items = [checks]

    normalized = []
    for item in items:
        token = str(item or "").strip()
        if token:
            normalized.append(token)
    return normalized


def _is_missing_audit_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _audit_value_fingerprint(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return str(value)


def _coerce_audit_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _coerce_audit_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    token = value.strip()
    if not token:
        return None

    iso_candidate = token.replace("Z", "+00:00") if token.endswith("Z") else token
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(token, fmt)
        except ValueError:
            continue
    return None


def _ordered_audit_columns(schema: Dict[str, str], data: List[Dict[str, Any]]) -> List[str]:
    seen = set()
    ordered = []

    for field in (schema or {}).keys():
        if field not in seen:
            seen.add(field)
            ordered.append(field)

    for row in data:
        if not isinstance(row, dict):
            continue
        for field in row.keys():
            if field not in seen:
                seen.add(field)
                ordered.append(field)

    return ordered


def _audit_key_fields(schema: Dict[str, str], data: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    schema_keys = list((schema or {}).keys())
    if schema_keys:
        return schema_keys[:3]

    for row in data or []:
        if isinstance(row, dict) and row:
            return list(row.keys())[:3]

    return []


def _extract_row_confidence(row: Dict[str, Any]) -> Optional[float]:
    for field in ("confidence", "_confidence"):
        value = row.get(field)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _find_duplicate_rows(data: List[Dict[str, Any]], schema: Dict[str, str]) -> List[Dict[str, int]]:
    duplicates = []
    seen_hashes = {}
    audit_key_fields = _audit_key_fields(schema, data)

    for idx, row in enumerate(data):
        row_hash = hash_result(row, schema, key_fields=audit_key_fields)
        if row_hash in seen_hashes:
            duplicates.append({"index": idx, "duplicate_of": seen_hashes[row_hash]})
        else:
            seen_hashes[row_hash] = idx

    return duplicates


def _build_field_summary(
    field: str,
    expected_type: Optional[str],
    data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    values = [row.get(field) if isinstance(row, dict) else None for row in data]
    missing_count = sum(1 for value in values if _is_missing_audit_value(value))
    non_missing = [value for value in values if not _is_missing_audit_value(value)]

    summary = {
        "expected_type": expected_type,
        "non_null_count": len(non_missing),
        "missing_count": missing_count,
        "missing_rate": (missing_count / len(values)) if values else 0.0,
        "unique_count": len({_audit_value_fingerprint(value) for value in non_missing}),
    }

    expected_token = str(expected_type or "").strip().lower()
    numeric_values = []
    numeric_like = True
    for value in non_missing:
        numeric = _coerce_audit_number(value)
        if numeric is None:
            numeric_like = False
            break
        numeric_values.append(numeric)
    if numeric_like and numeric_values:
        summary["numeric_range"] = {
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": sum(numeric_values) / len(numeric_values),
        }

    parsed_dates = []
    for value in non_missing:
        parsed = _coerce_audit_datetime(value)
        if parsed is not None:
            parsed_dates.append(parsed)
    if parsed_dates and ("date" in expected_token or "time" in expected_token or len(parsed_dates) == len(non_missing)):
        summary["date_range"] = {
            "earliest": min(parsed_dates).isoformat(),
            "latest": max(parsed_dates).isoformat(),
        }

    string_like = (
        expected_token in {"character", "str", "string"}
        or all(isinstance(value, str) for value in non_missing)
    )
    if string_like and non_missing:
        top_values = Counter(str(value).strip() for value in non_missing)
        summary["top_values"] = [
            {"value": value, "count": count}
            for value, count in top_values.most_common(5)
        ]

    return summary


def _build_representative_rows(
    data: List[Dict[str, Any]],
    schema: Dict[str, str],
    confidence_threshold: float,
    columns: List[str],
) -> List[Dict[str, Any]]:
    if not data:
        return []

    row_limit = _AUDIT_REPRESENTATIVE_ROW_LIMIT
    head_count = min(5, len(data))
    tail_count = min(5, len(data))

    def _entry(index: int, bucket: str, **extra: Any) -> Dict[str, Any]:
        entry = {
            "row_index": index,
            "bucket": bucket,
            "row": data[index],
        }
        entry.update(extra)
        return entry

    missing_bucket = []
    for idx, row in enumerate(data):
        missing_fields = [field for field in columns if _is_missing_audit_value(row.get(field))]
        if missing_fields:
            missing_bucket.append((len(missing_fields), idx, missing_fields))
    missing_bucket.sort(key=lambda item: (-item[0], item[1]))
    missing_rows = [
        _entry(idx, "missing_fields", missing_count=count, missing_fields=fields)
        for count, idx, fields in missing_bucket[:5]
    ]

    low_conf_bucket = []
    for idx, row in enumerate(data):
        confidence = _extract_row_confidence(row)
        if confidence is not None and confidence < confidence_threshold:
            low_conf_bucket.append((confidence, idx))
    low_conf_bucket.sort(key=lambda item: (item[0], item[1]))
    low_conf_rows = [
        _entry(idx, "low_confidence", confidence=confidence)
        for confidence, idx in low_conf_bucket[:5]
    ]

    duplicate_rows = [
        _entry(item["index"], "duplicate", duplicate_of=item["duplicate_of"])
        for item in _find_duplicate_rows(data, schema)[:5]
    ]

    head_rows = [_entry(idx, "head") for idx in range(head_count)]
    tail_rows = [_entry(idx, "tail") for idx in range(max(0, len(data) - tail_count), len(data))]

    spread_indices = []
    interior_count = min(10, max(0, len(data) - head_count - tail_count))
    if interior_count > 0:
        denominator = interior_count + 1
        for step in range(1, interior_count + 1):
            idx = int(round(step * (len(data) - 1) / denominator))
            if idx < head_count or idx >= len(data) - tail_count:
                continue
            if idx not in spread_indices:
                spread_indices.append(idx)
    spread_rows = [_entry(idx, "spread") for idx in spread_indices]

    representative_rows = []
    seen_indices = set()
    for bucket in (missing_rows, low_conf_rows, duplicate_rows, head_rows, tail_rows, spread_rows):
        for row_entry in bucket:
            idx = row_entry["row_index"]
            if idx in seen_indices:
                continue
            seen_indices.add(idx)
            representative_rows.append(row_entry)
            if len(representative_rows) >= row_limit:
                return representative_rows

    return representative_rows


def _build_audit_dataset_context(
    data: List[Dict[str, Any]],
    schema: Dict[str, str],
    confidence_threshold: float,
) -> Dict[str, Any]:
    columns = _ordered_audit_columns(schema or {}, data)
    field_summaries = {
        field: _build_field_summary(field, (schema or {}).get(field), data)
        for field in columns
    }

    return {
        "row_count": len(data),
        "schema": schema or {},
        "columns": columns,
        "confidence_threshold": confidence_threshold,
        "duplicate_key_fields": _audit_key_fields(schema or {}, data),
        "field_summaries": field_summaries,
        "representative_rows": _build_representative_rows(
            data,
            schema or {},
            confidence_threshold,
            columns,
        ),
    }


def _get_dataset_context(state: AuditState) -> Dict[str, Any]:
    dataset_context = state.get("dataset_context")
    if isinstance(dataset_context, dict):
        return dataset_context

    return _build_audit_dataset_context(
        data=state.get("data", []),
        schema=state.get("schema", {}),
        confidence_threshold=state.get("confidence_threshold", 0.8),
    )


# ────────────────────────────────────────────────────────────────────────
# Node Factories
# ────────────────────────────────────────────────────────────────────────

def create_completeness_node(llm):
    """Check for missing items and coverage gaps."""

    def completeness_node(state: AuditState) -> Dict:
        if "completeness" not in state.get("checks", []):
            return {}

        data = state.get("data", [])
        query = state.get("query", "")
        known_universe = state.get("known_universe")
        schema = state.get("schema", {})
        dataset_context = _get_dataset_context(state)

        issues = []
        recommendations = []
        flagged_rows = []

        # If known universe provided, check coverage
        if known_universe:
            # Get the first schema field as identifier
            id_field = list(schema.keys())[0] if schema else None
            if id_field:
                found_items = set()
                for row in data:
                    val = row.get(id_field, "")
                    if val:
                        found_items.add(str(val).strip().lower())

                universe_set = set(str(x).strip().lower() for x in known_universe)
                missing = universe_set - found_items
                extra = found_items - universe_set

                if missing:
                    issues.append({
                        "severity": "high" if len(missing) > 5 else "medium",
                        "description": f"Missing {len(missing)} expected items: {', '.join(list(missing)[:5])}{'...' if len(missing) > 5 else ''}",
                        "affected_rows": [],
                        "check_type": "completeness"
                    })
                    recommendations.append(
                        f"Search specifically for: {', '.join(list(missing)[:3])}"
                    )

                completeness = len(found_items & universe_set) / len(universe_set) if universe_set else 1.0
            else:
                completeness = 1.0
        else:
            # Use LLM to assess completeness
            prompt = f"""Analyze this data for completeness.

Query: {query}
Dataset context: {json.dumps(dataset_context, default=str)}

Are there obvious gaps or missing items based on what you'd expect for this query?
Return JSON: {{"completeness_score": 0.95, "issues": ["issue1"], "recommendations": ["rec1"]}}"""

            try:
                response = llm.invoke([HumanMessage(content=prompt)])
                result = parse_llm_json(response.content)
                if not isinstance(result, dict):
                    result = {}
                completeness = result.get("completeness_score", 0.9)
                for issue in result.get("issues", []):
                    issues.append({
                        "severity": "medium",
                        "description": issue,
                        "affected_rows": [],
                        "check_type": "completeness"
                    })
                recommendations.extend(result.get("recommendations", []))
            except Exception as e:
                logger.warning(f"Completeness check LLM call failed: {e}")
                completeness = 0.9

        return {
            "completeness_score": completeness,
            "issues": issues,
            "recommendations": recommendations,
            "flagged_rows": flagged_rows
        }

    return completeness_node


def create_consistency_node(llm):
    """Validate data patterns and formats."""

    def consistency_node(state: AuditState) -> Dict:
        if "consistency" not in state.get("checks", []):
            return {}

        data = state.get("data", [])
        schema = state.get("schema", {})

        issues = []
        flagged_rows = []
        invalid_count = 0

        for idx, row in enumerate(data):
            row_issues = []

            for field, expected_type in schema.items():
                value = row.get(field)

                # Check for missing required fields
                if value is None or value == "":
                    row_issues.append(f"Missing {field}")

                # Check type consistency
                elif expected_type == "character" or expected_type == "str":
                    if not isinstance(value, str):
                        row_issues.append(f"{field} should be string")

                elif expected_type in ("numeric", "integer", "int", "float"):
                    if not isinstance(value, (int, float)):
                        try:
                            float(value)
                        except (ValueError, TypeError):
                            row_issues.append(f"{field} should be numeric")

            if row_issues:
                invalid_count += 1
                flagged_rows.append({
                    "index": idx,
                    "flag": "warning",
                    "note": "; ".join(row_issues),
                    "confidence": None
                })

        consistency_score = 1.0 - (invalid_count / len(data)) if data else 1.0

        if invalid_count > 0:
            issues.append({
                "severity": "medium" if invalid_count < 5 else "high",
                "description": f"{invalid_count} rows have consistency issues",
                "affected_rows": [f["index"] for f in flagged_rows],
                "check_type": "consistency"
            })

        return {
            "consistency_score": consistency_score,
            "issues": issues,
            "flagged_rows": flagged_rows
        }

    return consistency_node


def create_gaps_node(llm):
    """Identify systematic patterns of missing data."""

    def gaps_node(state: AuditState) -> Dict:
        if "gaps" not in state.get("checks", []):
            return {}

        data = state.get("data", [])
        query = state.get("query", "")
        schema = state.get("schema", {})
        dataset_context = _get_dataset_context(state)

        issues = []
        recommendations = []

        # Use LLM for gap analysis
        prompt = f"""Analyze this data for systematic gaps.

Query: {query}
Schema: {json.dumps(schema)}
Dataset context: {json.dumps(dataset_context, default=str)}

Look for:
1. Geographic gaps (missing regions/states/countries)
2. Temporal gaps (missing time periods)
3. Categorical gaps (missing categories/types)
4. Structural patterns in what's missing

Return JSON: {{
    "gaps_found": [
        {{"type": "geographic|temporal|categorical", "description": "...", "severity": "high|medium|low"}}
    ],
    "recommendations": ["..."]
}}"""

        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            result = parse_llm_json(response.content)
            if not isinstance(result, dict):
                result = {}

            for gap in result.get("gaps_found", []):
                issues.append({
                    "severity": gap.get("severity", "medium"),
                    "description": f"[{gap.get('type', 'gap')}] {gap.get('description', '')}",
                    "affected_rows": [],
                    "check_type": "gaps"
                })

            recommendations.extend(result.get("recommendations", []))

        except Exception as e:
            logger.warning(f"Gap analysis LLM call failed: {e}")

        return {
            "issues": issues,
            "recommendations": recommendations
        }

    return gaps_node


def create_anomaly_node(llm):
    """Detect duplicates, outliers, and suspicious patterns."""

    def anomaly_node(state: AuditState) -> Dict:
        if "anomalies" not in state.get("checks", []):
            return {}

        data = state.get("data", [])
        schema = state.get("schema", {})
        confidence_threshold = state.get("confidence_threshold", 0.8)

        issues = []
        flagged_rows = []

        # Check for duplicates using hash (first 3 schema fields for audit)
        for duplicate in _find_duplicate_rows(data, schema):
            prev_idx = duplicate["duplicate_of"]
            idx = duplicate["index"]
            flagged_rows.append({
                "index": idx,
                "flag": "suspect",
                "note": f"Possible duplicate of row {prev_idx}",
                "confidence": None
            })

        # Check for low confidence items (if confidence field exists)
        for idx, row in enumerate(data):
            conf = _extract_row_confidence(row)
            if conf is not None and conf < confidence_threshold:
                # Don't double-flag
                if not any(f["index"] == idx for f in flagged_rows):
                    flagged_rows.append({
                        "index": idx,
                        "flag": "warning",
                        "note": f"Low confidence: {conf:.2f}",
                        "confidence": conf
                    })

        if len(flagged_rows) > 0:
            dupe_count = sum(1 for f in flagged_rows if "duplicate" in f["note"].lower())
            if dupe_count > 0:
                issues.append({
                    "severity": "high" if dupe_count > 3 else "medium",
                    "description": f"Found {dupe_count} potential duplicates",
                    "affected_rows": [f["index"] for f in flagged_rows if "duplicate" in f["note"].lower()],
                    "check_type": "anomalies"
                })

        return {
            "issues": issues,
            "flagged_rows": flagged_rows
        }

    return anomaly_node


def create_synthesizer_node(llm):
    """Combine findings into final summary."""

    def synthesizer_node(state: AuditState) -> Dict:
        issues = state.get("issues", [])
        flagged_rows = state.get("flagged_rows", [])
        completeness = state.get("completeness_score", 1.0)
        consistency = state.get("consistency_score", 1.0)
        data = state.get("data", [])

        # Count by severity
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for issue in issues:
            sev = issue.get("severity", "medium")
            severity_counts[sev] = severity_counts.get(sev, 0) + 1

        # Count by flag
        flag_counts = {"ok": 0, "warning": 0, "suspect": 0}
        flagged_indices = set(f["index"] for f in flagged_rows)
        for idx in range(len(data)):
            if idx in flagged_indices:
                flag = next((f["flag"] for f in flagged_rows if f["index"] == idx), "warning")
                flag_counts[flag] = flag_counts.get(flag, 0) + 1
            else:
                flag_counts["ok"] += 1

        # Build summary
        summary_parts = []
        summary_parts.append(f"Audited {len(data)} rows.")
        summary_parts.append(f"Completeness: {completeness*100:.0f}%, Consistency: {consistency*100:.0f}%.")

        if sum(severity_counts.values()) > 0:
            issue_str = ", ".join(f"{v} {k}" for k, v in severity_counts.items() if v > 0)
            summary_parts.append(f"Issues: {issue_str}.")
        else:
            summary_parts.append("No significant issues found.")

        if flag_counts["warning"] + flag_counts["suspect"] > 0:
            summary_parts.append(
                f"Flagged rows: {flag_counts['warning']} warnings, {flag_counts['suspect']} suspect."
            )

        return {
            "summary": " ".join(summary_parts),
            "status": "complete"
        }

    return synthesizer_node


# ────────────────────────────────────────────────────────────────────────
# Graph Construction
# ────────────────────────────────────────────────────────────────────────

def create_audit_graph(llm, checks: List[str] = None):
    """Create the audit graph with specified checks.

    Args:
        llm: LangChain LLM instance
        checks: List of checks to perform. Options:
                "completeness", "consistency", "gaps", "anomalies"

    Returns:
        Compiled LangGraph
    """
    checks = _normalize_audit_checks(checks)

    workflow = StateGraph(AuditState)

    # Create nodes
    completeness = create_completeness_node(llm)
    consistency = create_consistency_node(llm)
    gaps = create_gaps_node(llm)
    anomaly = create_anomaly_node(llm)
    synthesizer = create_synthesizer_node(llm)

    # Add nodes
    workflow.add_node("completeness", completeness)
    workflow.add_node("consistency", consistency)
    workflow.add_node("gaps", gaps)
    workflow.add_node("anomaly", anomaly)
    workflow.add_node("synthesizer", synthesizer)

    # Set up edges - linear pipeline
    workflow.set_entry_point("completeness")
    workflow.add_edge("completeness", "consistency")
    workflow.add_edge("consistency", "gaps")
    workflow.add_edge("gaps", "anomaly")
    workflow.add_edge("anomaly", "synthesizer")
    workflow.add_edge("synthesizer", END)

    return workflow.compile()


# ────────────────────────────────────────────────────────────────────────
# Execution Functions
# ────────────────────────────────────────────────────────────────────────

def run_audit(
    graph,
    data: List[Dict],
    query: str,
    schema: Dict[str, str],
    known_universe: Optional[List[str]] = None,
    confidence_threshold: float = 0.8,
    checks: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Run the audit graph on data.

    Args:
        graph: Compiled audit graph
        data: List of dictionaries (rows to audit)
        query: Original enumeration query
        schema: Schema dictionary
        known_universe: Optional list of expected items
        confidence_threshold: Flag items below this confidence
        checks: List of checks to perform

    Returns:
        Dictionary with audit results
    """
    checks = _normalize_audit_checks(checks)
    dataset_context = _build_audit_dataset_context(
        data=data,
        schema=schema,
        confidence_threshold=confidence_threshold,
    )

    initial_state = {
        "data": data,
        "query": query,
        "schema": schema,
        "known_universe": known_universe,
        "checks": checks,
        "confidence_threshold": confidence_threshold,
        "dataset_context": dataset_context,
        "issues": [],
        "flagged_rows": [],
        "recommendations": [],
        "completeness_score": 1.0,
        "consistency_score": 1.0,
        "summary": "",
        "status": "checking",
        "errors": []
    }

    try:
        final_state = graph.invoke(initial_state)
        return {
            "summary": final_state.get("summary", ""),
            "issues": final_state.get("issues", []),
            "recommendations": final_state.get("recommendations", []),
            "completeness_score": final_state.get("completeness_score", 1.0),
            "consistency_score": final_state.get("consistency_score", 1.0),
            "flagged_rows": final_state.get("flagged_rows", []),
            "status": final_state.get("status", "complete")
        }
    except Exception as e:
        logger.error(f"Audit graph failed: {e}")
        return {
            "summary": f"Audit failed: {str(e)}",
            "issues": [],
            "recommendations": [],
            "completeness_score": 0.0,
            "consistency_score": 0.0,
            "flagged_rows": [],
            "status": "failed"
        }
