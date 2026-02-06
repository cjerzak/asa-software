# audit_graph.py
#
# LangGraph pipeline for auditing enumeration results.
# Performs completeness, consistency, gap, and anomaly checks.
#
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Annotated, Dict, List, Optional, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph

from state_utils import add_to_list, hash_result, merge_dicts, parse_llm_json

logger = logging.getLogger(__name__)


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
Data ({len(data)} rows): {json.dumps(data[:20], default=str)}

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

        issues = []
        recommendations = []

        # Use LLM for gap analysis
        prompt = f"""Analyze this data for systematic gaps.

Query: {query}
Schema: {json.dumps(schema)}
Data ({len(data)} rows): {json.dumps(data[:30], default=str)}

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
        seen_hashes = {}
        audit_key_fields = list(schema.keys())[:3]
        for idx, row in enumerate(data):
            row_hash = hash_result(row, schema, key_fields=audit_key_fields)

            if row_hash in seen_hashes:
                prev_idx = seen_hashes[row_hash]
                flagged_rows.append({
                    "index": idx,
                    "flag": "suspect",
                    "note": f"Possible duplicate of row {prev_idx}",
                    "confidence": None
                })
            else:
                seen_hashes[row_hash] = idx

        # Check for low confidence items (if confidence field exists)
        for idx, row in enumerate(data):
            conf = row.get("confidence") or row.get("_confidence")
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
    if checks is None:
        checks = ["completeness", "consistency", "gaps", "anomalies"]

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
    if checks is None:
        checks = ["completeness", "consistency", "gaps", "anomalies"]

    initial_state = {
        "data": data,
        "query": query,
        "schema": schema,
        "known_universe": known_universe,
        "checks": checks,
        "confidence_threshold": confidence_threshold,
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
