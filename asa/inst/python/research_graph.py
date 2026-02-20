# research_graph.py
#
# LangGraph multi-agent orchestration for open-ended research tasks.
# Simplified architecture: Planner -> Searcher -> Extractor -> Stopper
#
import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Annotated, Dict, List, Optional, Sequence, TypedDict

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
)
from langgraph.errors import GraphRecursionError
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.managed import RemainingSteps
from langgraph.prebuilt import ToolNode, tools_condition

from state_utils import (
    build_node_trace_entry,
    _token_usage_dict_from_message,
    _token_usage_from_message,
    add_to_list,
    hash_result,
    merge_dicts,
    parse_llm_json,
    should_stop_for_recursion,
)
from outcome_gate import evaluate_research_outcome
from wikidata_tool import get_entity_template, get_known_entity_types, query_known_entity

# Optional strict temporal verification (local module)
try:
    from date_extractor import verify_date_constraint, _parse_date_string
except Exception:  # pragma: no cover
    verify_date_constraint = None
    _parse_date_string = None

logger = logging.getLogger(__name__)
RECURSION_STOP_BUFFER = 2


# ────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────
@dataclass
class ResearchConfig:
    """Configuration for research task execution."""
    max_workers: int = 4
    max_rounds: int = 8
    budget_queries: int = 50
    budget_tokens: int = 200000
    budget_time_sec: int = 300
    target_items: Optional[int] = None
    plateau_rounds: int = 2
    novelty_min: float = 0.05
    novelty_window: int = 20
    use_wikidata: bool = True
    use_web: bool = True
    use_wikipedia: bool = True
    # Optional capability: allow reading full webpages (disabled by default)
    allow_read_webpages: bool = False
    # Maximum tool calls per search round (higher when webpage reading enabled)
    max_tool_calls_per_round: Optional[int] = None
    # Temporal filtering parameters
    time_filter: Optional[str] = None      # DDG time filter: "d", "w", "m", "y"
    date_after: Optional[str] = None       # ISO 8601: "2020-01-01"
    date_before: Optional[str] = None      # ISO 8601: "2024-01-01"
    temporal_strictness: str = "best_effort"  # "best_effort" | "strict"
    use_wayback: bool = False              # Use Wayback Machine for pre-date guarantees


# ────────────────────────────────────────────────────────────────────────
# State Definitions
# ────────────────────────────────────────────────────────────────────────
class ResultRow(TypedDict):
    """A single result row with provenance."""
    row_id: str
    fields: Dict[str, Any]
    source_url: str
    confidence: float
    worker_id: str
    extraction_timestamp: float


class ResearchState(TypedDict):
    """Main state for research graph."""
    # Input
    query: str
    schema: Dict[str, str]
    config: Dict[str, Any]

    # Planning
    plan: Dict[str, Any]
    entity_type: str
    wikidata_type: Optional[str]

    # Results
    results: Annotated[List[ResultRow], add_to_list]
    new_results: List[ResultRow]
    seen_hashes: Annotated[Dict[str, bool], merge_dicts]
    novelty_history: Annotated[List[float], add_to_list]

    # Metrics & Control
    round_number: int
    queries_used: int
    tokens_used: int
    input_tokens: int
    output_tokens: int
    token_trace: Annotated[list, add_to_list]
    start_time: float
    status: str  # "planning", "searching", "complete", "failed"
    stop_reason: Optional[str]
    completion_gate: Annotated[Dict[str, Any], merge_dicts]
    errors: Annotated[List[Dict], add_to_list]
    remaining_steps: RemainingSteps


# ────────────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────────────
def _hash_result(fields: Dict[str, Any], schema: Dict[str, str]) -> str:
    """Create a hash for deduplication based on ALL schema fields."""
    return hash_result(fields, schema)


def _fuzzy_match_name(name1: str, name2: str) -> float:
    """Simple fuzzy matching for names. Returns similarity score 0-1."""
    if not name1 or not name2:
        return 0.0
    name1 = name1.lower().strip()
    name2 = name2.lower().strip()
    if name1 == name2:
        return 1.0
    if name1 in name2 or name2 in name1:
        return 0.8
    words1 = set(name1.split())
    words2 = set(name2.split())
    if not words1 or not words2:
        return 0.0
    overlap = len(words1 & words2)
    total = len(words1 | words2)
    return overlap / total if total > 0 else 0.0


def _configured_recursion_limit_from_state(state: Any) -> Optional[int]:
    """Best-effort read of per-run recursion_limit from state.config."""
    config = None
    try:
        if hasattr(state, "get"):
            config = state.get("config")
        elif hasattr(state, "__getitem__"):
            config = state["config"]
    except Exception:
        config = None

    if config is None:
        return None

    recursion_limit = None
    try:
        if isinstance(config, dict):
            recursion_limit = config.get("recursion_limit")
        elif hasattr(config, "get"):
            recursion_limit = config.get("recursion_limit")
    except Exception:
        recursion_limit = None

    if recursion_limit is None:
        return None

    try:
        return int(recursion_limit)
    except Exception:
        return None


def _effective_recursion_stop_buffer(state: Any) -> int:
    """Use a tighter buffer for very small limits so one research round can run."""
    configured_limit = _configured_recursion_limit_from_state(state)
    if configured_limit is not None and configured_limit <= 4:
        return 1
    return RECURSION_STOP_BUFFER


def _with_node_timing(
    payload: Dict[str, Any],
    *,
    node: str,
    started_at: float,
    usage: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """Attach a normalized per-node timing entry to payload.token_trace."""
    out = dict(payload or {})
    token_trace = out.get("token_trace")
    if not isinstance(token_trace, list):
        token_trace = []
    token_trace.append(build_node_trace_entry(node, usage=usage, started_at=started_at))
    out["token_trace"] = token_trace
    return out


def _add_int(existing: Optional[int], new: Optional[int]) -> int:
    """Reducer for summing integer counters (LangGraph state annotation)."""
    try:
        existing_val = int(existing or 0)
    except Exception:
        existing_val = 0
    try:
        new_val = int(new or 0)
    except Exception:
        new_val = 0
    return existing_val + new_val


class _ToolLoopState(TypedDict):
    """Internal state schema for a minimal tool-calling loop."""
    messages: Annotated[list, add_messages]
    queries_used: Annotated[int, _add_int]
    tokens_used: Annotated[int, _add_int]
    input_tokens: Annotated[int, _add_int]
    output_tokens: Annotated[int, _add_int]


def _coerce_message_to_base(message: Any) -> BaseMessage:
    """Coerce arbitrary LLM responses into a LangChain BaseMessage."""
    if isinstance(message, BaseMessage):
        return message

    content = ""
    try:
        content = message.get("content", "") if isinstance(message, dict) else getattr(message, "content", "")
    except Exception:
        content = ""

    tool_calls = None
    try:
        tool_calls = message.get("tool_calls") if isinstance(message, dict) else getattr(message, "tool_calls", None)
    except Exception:
        tool_calls = None

    if tool_calls:
        try:
            return AIMessage(content=str(content), tool_calls=tool_calls)
        except Exception:
            return AIMessage(content=str(content))

    return AIMessage(content=str(content))


def _tool_outputs_from_messages(messages: Sequence[Any]) -> List[str]:
    """Extract tool output text blocks from a message sequence."""
    outputs: List[str] = []
    for msg in messages or []:
        try:
            if type(msg).__name__ == "ToolMessage":
                content = getattr(msg, "content", None)
                if content is not None:
                    outputs.append(str(content))
                continue
            if isinstance(msg, dict) and str(msg.get("role") or "").lower() == "tool":
                content = msg.get("content")
                if content is not None:
                    outputs.append(str(content))
        except Exception:
            continue
    return outputs


def _run_tool_loop(
    llm: Any,
    tools: Sequence[Any],
    *,
    initial_messages: Sequence[Any],
    max_tool_calls: int,
    recursion_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Run a minimal LangGraph tool loop for up to max_tool_calls model steps."""
    try:
        max_calls = int(max_tool_calls)
    except Exception:
        max_calls = 0

    if max_calls <= 0:
        return {
            "messages": list(initial_messages or []),
            "queries_used": 0,
            "tokens_used": 0,
            "input_tokens": 0,
            "output_tokens": 0,
        }

    try:
        effective_recursion = int(recursion_limit) if recursion_limit is not None else max(4, 2 * max_calls + 2)
    except Exception:
        effective_recursion = max(4, 2 * max_calls + 2)

    tools_list = list(tools or [])
    model_with_tools = llm.bind_tools(tools_list)
    tool_node = ToolNode(tools_list)

    def agent_node(state: _ToolLoopState) -> Dict[str, Any]:
        messages = state.get("messages") or []
        raw = model_with_tools.invoke(messages)
        usage = _token_usage_dict_from_message(raw)
        response = _coerce_message_to_base(raw)
        return {
            "messages": [response],
            "queries_used": 1,
            "tokens_used": usage["total_tokens"],
            "input_tokens": usage["input_tokens"],
            "output_tokens": usage["output_tokens"],
        }

    def after_tools(state: _ToolLoopState) -> str:
        try:
            used = int(state.get("queries_used", 0) or 0)
        except Exception:
            used = 0
        if used >= max_calls:
            return "__end__"
        return "agent"

    workflow = StateGraph(_ToolLoopState)
    workflow.add_node("agent", agent_node)
    workflow.add_node("tools", tool_node)
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        tools_condition,
        {
            "tools": "tools",
            "__end__": END,
        },
    )
    workflow.add_conditional_edges(
        "tools",
        after_tools,
        {
            "agent": "agent",
            "__end__": END,
        },
    )

    compiled = workflow.compile()
    initial_state: Dict[str, Any] = {
        "messages": list(initial_messages or []),
        "queries_used": 0,
        "tokens_used": 0,
        "input_tokens": 0,
        "output_tokens": 0,
    }
    config = {"recursion_limit": effective_recursion}

    last_state: Optional[Dict[str, Any]] = None
    try:
        for chunk in compiled.stream(initial_state, config=config, stream_mode="values"):
            if isinstance(chunk, dict):
                last_state = chunk
        if last_state is None:
            invoked = compiled.invoke(initial_state, config=config)
            if isinstance(invoked, dict):
                last_state = invoked
    except GraphRecursionError:
        pass

    state = last_state if isinstance(last_state, dict) else initial_state
    return {
        "messages": list(state.get("messages") or []),
        "queries_used": int(state.get("queries_used", 0) or 0),
        "tokens_used": int(state.get("tokens_used", 0) or 0),
        "input_tokens": int(state.get("input_tokens", 0) or 0),
        "output_tokens": int(state.get("output_tokens", 0) or 0),
    }


def _to_snake_case(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and (name[i - 1].islower() or (i + 1 < len(name) and name[i + 1].islower())):
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def _normalize_wikidata_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Wikidata dict keys to be friendlier for schema matching."""
    normalized: Dict[str, Any] = {}
    for k, v in (item or {}).items():
        normalized[k] = v
        snake = _to_snake_case(k)
        if snake not in normalized:
            normalized[snake] = v

    # Common aliases
    if "termStart" in normalized and "term_start" not in normalized:
        normalized["term_start"] = normalized.get("termStart")
    if "termEnd" in normalized and "term_end" not in normalized:
        normalized["term_end"] = normalized.get("termEnd")

    return normalized


def _parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse a date string into a datetime object.

    Delegates to date_extractor._parse_date_string for multi-format parsing,
    falling back to simple ISO-only parsing if that module is unavailable.
    """
    if not date_str:
        return None
    try:
        if _parse_date_string is not None:
            iso_str = _parse_date_string(date_str)
            if iso_str:
                return datetime.strptime(iso_str, "%Y-%m-%d")
            return None
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def _within_date_range(date_str: Optional[str], date_after: Optional[str], date_before: Optional[str]) -> Optional[bool]:
    """Return True/False if determinable, else None."""
    dt = _parse_iso_date(date_str)
    if dt is None:
        return None
    after = _parse_iso_date(date_after)
    before = _parse_iso_date(date_before)
    if after and dt < after:
        return False
    if before and dt >= before:
        return False
    return True


def _schema_extraction_example(schema_keys: List[str]) -> str:
    """Build a neutral schema-driven JSON example for extraction prompts."""
    if not schema_keys:
        return '[{"field_1": "value", "field_2": null, "source_url": "https://example.com/source"}]'

    row_a: Dict[str, Any] = {}
    row_b: Dict[str, Any] = {}
    for idx, key in enumerate(schema_keys):
        if idx == 0:
            row_a[key] = "Example Value A"
            row_b[key] = "Example Value B"
        elif idx == 1:
            row_a[key] = "Example Attribute"
            row_b[key] = None
        else:
            row_a[key] = None
            row_b[key] = None
    row_a["source_url"] = "https://example.com/source-a"
    row_b["source_url"] = "https://example.com/source-b"
    return json.dumps([row_a, row_b], ensure_ascii=False, indent=2)


# ────────────────────────────────────────────────────────────────────────
# Planner Node
# ────────────────────────────────────────────────────────────────────────
def _planner_known_entity_lines() -> List[str]:
    lines: List[str] = []
    for entity_type in get_known_entity_types():
        template = get_entity_template(entity_type) or {}
        description = str(template.get("description") or entity_type)
        lines.append(f"- {entity_type}: {description}")
    if not lines:
        lines.append("- none (no templates configured)")
    return lines


def _planner_wikidata_choices() -> str:
    known_types = get_known_entity_types()
    if not known_types:
        return "null"
    return "|".join(known_types + ["null"])


def _build_planner_prompt(query: str) -> str:
    known_types_text = "\n".join(_planner_known_entity_lines())
    wikidata_choices = _planner_wikidata_choices()
    return (
        "You are a research planner. Analyze the query and identify:\n"
        "1. The entity type being requested\n"
        "2. Whether this matches a known Wikidata entity type\n\n"
        "Known Wikidata entity types:\n"
        f"{known_types_text}\n\n"
        f"Query: {query}\n\n"
        "Respond in JSON format ONLY (no markdown):\n"
        "{\"entity_type\": \"description of entity type\", "
        f"\"wikidata_type\": \"{wikidata_choices}\", "
        "\"search_queries\": [\"query1\", \"query2\"]}"
    )


def create_planner_node(llm):
    """Create the planner node."""

    def planner_node(state: ResearchState) -> Dict:
        """Analyze query and create research plan."""
        node_started_at = time.perf_counter()
        query = state.get("query", "")
        logger.info(f"Planner analyzing: {query}")

        prompt = _build_planner_prompt(query)
        messages = [HumanMessage(content=prompt)]

        try:
            response = llm.invoke(messages)
            plan = parse_llm_json(response.content)
            if not isinstance(plan, dict):
                plan = {}
            logger.info(f"Plan: entity={plan.get('entity_type')}, wikidata={plan.get('wikidata_type')}")

            usage = _token_usage_dict_from_message(response)
            return _with_node_timing({
                "plan": plan,
                "entity_type": plan.get("entity_type", "unknown"),
                "wikidata_type": plan.get("wikidata_type"),
                "status": "searching",
                "queries_used": 1,
                "tokens_used": state.get("tokens_used", 0) + usage["total_tokens"],
                "input_tokens": state.get("input_tokens", 0) + usage["input_tokens"],
                "output_tokens": state.get("output_tokens", 0) + usage["output_tokens"],
            }, node="planner", started_at=node_started_at, usage=usage)

        except Exception as e:
            logger.error(f"Planner error: {e}")
            return _with_node_timing({
                "status": "failed",
                "stop_reason": f"Planning failed: {str(e)}",
                "errors": [{"stage": "planner", "error": str(e)}]
            }, node="planner", started_at=node_started_at)

    return planner_node


# ────────────────────────────────────────────────────────────────────────
# Searcher Node
# ────────────────────────────────────────────────────────────────────────
def create_searcher_node(llm, tools, wikidata_tool=None, research_config: ResearchConfig = None):
    """Create the searcher node that executes searches."""

    def searcher_node(state: ResearchState) -> Dict:
        """Execute search based on plan."""
        node_started_at = time.perf_counter()
        # If this is the last allowed step, stop before doing any more work. For
        # tiny limits (<=4), we use a smaller buffer so at least one search round
        # can run before graceful completion.
        stop_buffer = _effective_recursion_stop_buffer(state)
        if should_stop_for_recursion(state, buffer=stop_buffer):
            gate = evaluate_research_outcome(
                round_number=int(state.get("round_number", 0) or 0),
                queries_used=int(state.get("queries_used", 0) or 0),
                tokens_used=int(state.get("tokens_used", 0) or 0),
                elapsed_sec=max(0.0, time.time() - float(state.get("start_time", 0.0) or 0.0)),
                items_found=int(len(state.get("seen_hashes", {}) or {})),
                novelty_history=state.get("novelty_history", []) if isinstance(state.get("novelty_history", []), list) else [],
                max_rounds=(research_config.max_rounds if research_config else None),
                budget_queries=(research_config.budget_queries if research_config else None),
                budget_tokens=(research_config.budget_tokens if research_config else None),
                budget_time_sec=(research_config.budget_time_sec if research_config else None),
                target_items=(research_config.target_items if research_config else None),
                plateau_rounds=(research_config.plateau_rounds if research_config else None),
                novelty_min=(research_config.novelty_min if research_config else None),
                recursion_stop=True,
            )
            return _with_node_timing(
                {
                    "status": "complete",
                    "stop_reason": "recursion_limit",
                    "completion_gate": gate,
                },
                node="searcher",
                started_at=node_started_at,
            )

        wikidata_type = state.get("wikidata_type")
        query = state.get("query", "")
        schema = state.get("schema", {})
        config = state.get("config", {})
        seen_hashes = state.get("seen_hashes", {})
        plan = state.get("plan", {}) or {}

        # Extract temporal config
        time_filter = config.get("time_filter") or (research_config.time_filter if research_config else None)
        date_after = config.get("date_after") or (research_config.date_after if research_config else None)
        date_before = config.get("date_before") or (research_config.date_before if research_config else None)
        temporal_strictness = config.get("temporal_strictness") or (research_config.temporal_strictness if research_config else "best_effort")
        use_wayback = bool(config.get("use_wayback") if config.get("use_wayback") is not None else (research_config.use_wayback if research_config else False))
        allow_read_webpages_raw = (
            config.get("allow_read_webpages")
            if config.get("allow_read_webpages") is not None
            else (research_config.allow_read_webpages if research_config else False)
        )
        # Support "auto" mode: enable webpage reading when extraction yield is low
        auto_webpage_mode = (isinstance(allow_read_webpages_raw, str)
                             and allow_read_webpages_raw.lower() == "auto")
        if auto_webpage_mode:
            # Check if yield has been low for 2+ consecutive rounds
            novelty_history = state.get("novelty_history", [])
            existing_results = state.get("results", [])
            round_number = state.get("round_number", 0)
            low_yield = (
                round_number >= 2
                and len(novelty_history) >= 2
                and all(r < 0.2 for r in novelty_history[-2:])
            ) or (round_number >= 2 and len(existing_results) < 2)
            allow_read_webpages = low_yield
            if low_yield:
                logger.info("Auto webpage mode: enabling OpenWebpage due to low extraction yield")
        else:
            allow_read_webpages = bool(allow_read_webpages_raw)
        target_items = config.get("target_items")
        if target_items is None and research_config:
            target_items = research_config.target_items

        results = []
        queries_used = state.get("queries_used", 0)
        tokens_used = state.get("tokens_used", 0)
        input_tokens = state.get("input_tokens", 0)
        output_tokens = state.get("output_tokens", 0)
        start_tokens_used = tokens_used
        start_input_tokens = input_tokens
        start_output_tokens = output_tokens
        local_token_trace = []
        total_unique = len(seen_hashes)
        needs_more = target_items is not None and total_unique < target_items

        # Try Wikidata first if we have a matching type
        wikidata_type_norm = str(wikidata_type or "").strip()
        known_wikidata_types = set(get_known_entity_types())
        can_query_wikidata = (
            bool(wikidata_type_norm)
            and wikidata_type_norm.lower() not in {"null", "none"}
            and wikidata_type_norm in known_wikidata_types
        )
        if can_query_wikidata and wikidata_tool and config.get("use_wikidata", True):
            logger.info(f"Querying Wikidata for: {wikidata_type_norm}")
            try:
                wd_rows = query_known_entity(
                    wikidata_type_norm,
                    config=wikidata_tool.config,
                    date_after=date_after,
                    date_before=date_before,
                )

                for item in wd_rows:
                    if not isinstance(item, dict):
                        continue
                    normalized = _normalize_wikidata_item(item)
                    fields = {k: normalized.get(k) for k in schema.keys()}
                    if not any(v is not None and v != "" for v in fields.values()):
                        continue

                    wikidata_id = normalized.get("wikidata_id") or normalized.get("wikidataId") or normalized.get("wikidata")
                    source_url = f"https://www.wikidata.org/wiki/{wikidata_id}" if wikidata_id else "https://www.wikidata.org"

                    results.append(ResultRow(
                        row_id=f"wd_{len(results)}",
                        fields=fields,
                        source_url=source_url,
                        confidence=0.95,
                        worker_id="wikidata",
                        extraction_timestamp=time.time()
                    ))

                queries_used += 1
                total_unique = len(seen_hashes)
                needs_more = target_items is not None and (total_unique + len(results)) < target_items
                logger.info(f"Wikidata returned {len(results)} results")

            except Exception as e:
                logger.error(f"Wikidata error: {e}")
        elif wikidata_type and wikidata_tool and config.get("use_wikidata", True):
            logger.info(
                "Skipping Wikidata query for unsupported type: %s",
                wikidata_type,
            )

        # If no Wikidata results (or we still need more), try web search
        if config.get("use_web", True) and (len(results) == 0 or needs_more):
            logger.info("Falling back to web search")
            try:
                planned_queries = plan.get("search_queries")
                if isinstance(planned_queries, list) and planned_queries:
                    planned = "\n".join(f"- {q}" for q in planned_queries[:8] if isinstance(q, str) and q.strip())
                    if planned.strip():
                        query_hint = f"\nPlanned sub-queries:\n{planned}\n"
                    else:
                        query_hint = ""
                else:
                    query_hint = ""

                temporal_hint = ""
                if date_after or date_before:
                    temporal_hint = f"\nTemporal constraints: after={date_after or 'N/A'}, before={date_before or 'N/A'} (strictness={temporal_strictness})\n"

                # Apply temporal filter to search tools and restore after run
                search_tools = []
                original_times = []
                for tool in tools:
                    # Hide optional webpage-reading tool unless explicitly enabled.
                    tool_name = getattr(tool, "name", "") or ""
                    if (not allow_read_webpages) and tool_name == "OpenWebpage":
                        continue
                    # Check if it's a DDG search tool and apply time filter
                    if hasattr(tool, 'api_wrapper') and time_filter:
                        try:
                            original_times.append((tool, tool.api_wrapper.time))
                            tool.api_wrapper.time = time_filter
                            logger.info(f"Applied time filter '{time_filter}' to search tool")
                        except Exception as e:
                            logger.warning(f"Could not apply time filter: {e}")
                    search_tools.append(tool)

                # Use LLM with tools
                webpage_hint = ""
                if allow_read_webpages:
                    webpage_hint = (
                        "\nYou may open and read a few of the most relevant result URLs "
                        "using the OpenWebpage tool to extract accurate details. "
                        "When calling OpenWebpage, include a focused 'query' describing what you need.\n"
                    )

                # Build context about already-found entities for subsequent rounds
                already_found_hint = ""
                round_number = state.get("round_number", 0)
                if round_number > 0:
                    existing_results = state.get("results", [])
                    if existing_results:
                        # Sample a few names to include in prompt
                        sample_names = []
                        first_field = list(schema.keys())[0] if schema else "name"
                        for r in existing_results[:10]:
                            fields = r.get("fields", {}) if isinstance(r, dict) else {}
                            val = fields.get(first_field, "")
                            if val:
                                sample_names.append(str(val))
                        if sample_names:
                            names_str = ", ".join(sample_names[:8])
                            already_found_hint = (
                                f"\nAlready found {len(existing_results)} entities "
                                f"(e.g., {names_str}). "
                                "Search for entities NOT in this list. "
                                "Focus on gaps and underrepresented categories.\n"
                            )

                search_prompt = f"""Search for: {query}
{query_hint}{temporal_hint}{webpage_hint}{already_found_hint}
Extract entities with these fields: {list(schema.keys())}
Use the Search tool to find information."""

                max_tool_calls = (
                    (research_config.max_tool_calls_per_round if research_config else None)
                    or (5 if allow_read_webpages else 3)
                )
                messages = [HumanMessage(content=search_prompt)]
                loop_out = _run_tool_loop(
                    llm=llm,
                    tools=search_tools,
                    initial_messages=messages,
                    max_tool_calls=max_tool_calls,
                )
                messages = loop_out.get("messages", messages)
                queries_used += int(loop_out.get("queries_used", 0) or 0)
                tokens_used += int(loop_out.get("tokens_used", 0) or 0)
                input_tokens += int(loop_out.get("input_tokens", 0) or 0)
                output_tokens += int(loop_out.get("output_tokens", 0) or 0)

                tool_outputs = _tool_outputs_from_messages(messages)

                # Extract structured entities from tool output
                if tool_outputs and schema:
                    schema_keys = list(schema.keys())
                    schema_example = _schema_extraction_example(schema_keys)
                    extraction_prompt = (
                        "You are extracting ALL structured entities from search results and opened webpages.\n\n"
                        "CRITICAL INSTRUCTIONS:\n"
                        "- Extract EVERY entity mentioned, even if only partially described.\n"
                        "- Use null for any field you cannot determine -- partial records ARE valuable.\n"
                        "- Do NOT skip entities just because some fields are missing.\n"
                        "- Be EXHAUSTIVE: if a source mentions 10 entities, extract all 10.\n"
                        "- Include entities from ALL search results, not just the first few.\n\n"
                        f"Query: {query}\n"
                        f"Temporal constraints: after={date_after or 'N/A'}, before={date_before or 'N/A'} (strictness={temporal_strictness}).\n"
                        "If strictness is 'strict', ONLY include entities clearly within date constraints.\n\n"
                        f"Required fields: {schema_keys}\n"
                        "Return ONLY a JSON array. Each item MUST have these fields (use null for unknown).\n"
                        "You MAY also include an optional 'source_url' field.\n\n"
                        "Example:\n"
                        f"{schema_example}\n\n"
                        "Tool outputs:\n"
                        + "\n\n".join(tool_outputs)
                    )

                    extraction_messages = [HumanMessage(content=extraction_prompt)]
                    extraction_response = llm.invoke(extraction_messages)
                    _ext_usage = _token_usage_dict_from_message(extraction_response)
                    tokens_used += _ext_usage["total_tokens"]
                    input_tokens += _ext_usage["input_tokens"]
                    output_tokens += _ext_usage["output_tokens"]
                    queries_used += 1

                    parsed = parse_llm_json(extraction_response.content)
                    rows = []
                    if isinstance(parsed, list):
                        rows = parsed
                    elif isinstance(parsed, dict) and isinstance(parsed.get("results"), list):
                        rows = parsed.get("results", [])

                    for item in rows:
                        if not isinstance(item, dict):
                            continue
                        fields = {k: item.get(k) for k in schema.keys()}
                        if not any(v is not None and v != "" for v in fields.values()):
                            continue
                        source_url = (
                            item.get("source_url")
                            or item.get("url")
                            or item.get("source")
                            or "web_search"
                        )
                        results.append(ResultRow(
                            row_id=f"web_{len(results)}",
                            fields=fields,
                            source_url=source_url,
                            confidence=0.6,
                            worker_id="web_search",
                            extraction_timestamp=time.time()
                        ))

            except Exception as e:
                logger.error(f"Web search error: {e}")
            finally:
                for tool, previous in original_times:
                    try:
                        tool.api_wrapper.time = previous
                    except Exception as e:
                        logger.warning(f"Could not restore time filter: {e}")

        # Strict temporal verification (best-effort): filter web results using date metadata.
        if temporal_strictness == "strict" and (date_after or date_before):
            if verify_date_constraint is None:
                logger.warning("Strict temporal filtering requested, but date_extractor is unavailable.")
            else:
                web_rows = [r for r in results if isinstance(r, dict) and r.get("worker_id") == "web_search"]
                urls = sorted({r.get("source_url") for r in web_rows if isinstance(r.get("source_url"), str) and r.get("source_url", "").startswith("http")})

                verified: Dict[str, Dict[str, Any]] = {}
                if urls:
                    max_workers = int(config.get("max_workers") or (research_config.max_workers if research_config else 4) or 4)
                    max_workers = max(1, min(max_workers, 16))
                    with ThreadPoolExecutor(max_workers=max_workers) as ex:
                        futures = {
                            ex.submit(verify_date_constraint, u, date_after=date_after, date_before=date_before): u
                            for u in urls
                        }
                        for fut in as_completed(futures):
                            u = futures[fut]
                            try:
                                verified[u] = fut.result()
                            except Exception as e:
                                verified[u] = {"url": u, "passes": None, "reason": f"verify_error:{e}"}

                def _row_passes(row: Dict[str, Any]) -> bool:
                    url = row.get("source_url")
                    if not isinstance(url, str) or not url.startswith("http"):
                        return False
                    v = verified.get(url)
                    if not isinstance(v, dict):
                        return False
                    if v.get("passes") is True:
                        return True
                    # Optional Wayback fallback: accept a snapshot within range
                    if use_wayback and (v.get("passes") is None):
                        try:
                            from wayback_tool import find_snapshots_in_range
                            snaps = find_snapshots_in_range(url, after_date=date_after, before_date=date_before, limit=5)
                            if snaps:
                                snap = snaps[0]
                                snap_date = snap.get("date")
                                within = _within_date_range(snap_date, date_after, date_before)
                                if within:
                                    row["source_url"] = snap.get("wayback_url") or row.get("source_url")
                                    row["confidence"] = min(float(row.get("confidence", 0.6)) + 0.1, 0.9)
                                    return True
                        except Exception:
                            return False
                    return False

                filtered = []
                for r in results:
                    if isinstance(r, dict) and r.get("worker_id") == "web_search" and (date_after or date_before):
                        if _row_passes(r):
                            filtered.append(r)
                    else:
                        filtered.append(r)
                dropped = len(results) - len(filtered)
                if dropped > 0:
                    logger.info(f"Strict temporal filtering dropped {dropped} web results")
                results = filtered

        node_usage = {
            "input_tokens": max(0, int(input_tokens) - int(start_input_tokens)),
            "output_tokens": max(0, int(output_tokens) - int(start_output_tokens)),
            "total_tokens": max(0, int(tokens_used) - int(start_tokens_used)),
        }
        return _with_node_timing({
            "new_results": results,
            "queries_used": queries_used,
            "tokens_used": tokens_used,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "token_trace": local_token_trace,
            "round_number": state.get("round_number", 0) + 1
        }, node="searcher", started_at=node_started_at, usage=node_usage)

    return searcher_node


# ────────────────────────────────────────────────────────────────────────
# Deduplicator Node
# ────────────────────────────────────────────────────────────────────────
def create_deduper_node():
    """Create deduplication node."""

    def deduper_node(state: ResearchState) -> Dict:
        """Deduplicate results."""
        node_started_at = time.perf_counter()
        # If this is the last allowed step, still dedupe, but mark complete so the graph can END.
        # Keep a small buffer to avoid routing to nodes we cannot execute.
        force_stop = should_stop_for_recursion(
            state,
            buffer=_effective_recursion_stop_buffer(state)
        )

        results = state.get("new_results")
        if results is None:
            results = state.get("results", [])
        schema = state.get("schema", {})
        seen_hashes = state.get("seen_hashes", {})

        unique_results = []
        new_hashes = {}

        # Use first schema field for fuzzy matching (instead of hardcoded "name")
        fuzzy_field = list(schema.keys())[0] if schema else "name"

        for result in results:
            fields = result.get("fields", {})
            result_hash = _hash_result(fields, schema)

            if result_hash not in seen_hashes and result_hash not in new_hashes:
                # Fuzzy check on first schema field
                is_dup = False
                name = fields.get(fuzzy_field, "")
                if name:
                    for existing in unique_results:
                        if _fuzzy_match_name(name, existing.get("fields", {}).get(fuzzy_field, "")) > 0.85:
                            is_dup = True
                            break

                if not is_dup:
                    unique_results.append(result)
                    new_hashes[result_hash] = True

        logger.info(f"Dedup: {len(results)} -> {len(unique_results)}")

        # Per-round novelty: fraction of THIS batch that was novel.
        # Previously used cumulative denominator which caused premature stopping
        # (e.g., 5 new out of 105 total = 0.048, even though 100% of batch was new).
        batch_size = len(results)
        novelty_rate = len(new_hashes) / max(1, batch_size)

        out = {
            "results": unique_results,
            "seen_hashes": new_hashes,
            "novelty_history": [novelty_rate],
            "new_results": []
        }
        if force_stop:
            gate = evaluate_research_outcome(
                round_number=int(state.get("round_number", 0) or 0),
                queries_used=int(state.get("queries_used", 0) or 0),
                tokens_used=int(state.get("tokens_used", 0) or 0),
                elapsed_sec=max(0.0, time.time() - float(state.get("start_time", 0.0) or 0.0)),
                items_found=int(len(seen_hashes) + len(new_hashes)),
                novelty_history=state.get("novelty_history", []) if isinstance(state.get("novelty_history", []), list) else [],
                target_items=(state.get("config", {}) or {}).get("target_items"),
                recursion_stop=True,
            )
            out["status"] = "complete"
            out["stop_reason"] = "recursion_limit"
            out["completion_gate"] = gate
        return _with_node_timing(out, node="deduper", started_at=node_started_at)

    return deduper_node


# ────────────────────────────────────────────────────────────────────────
# Stopper Node
# ────────────────────────────────────────────────────────────────────────
def create_stopper_node(config: ResearchConfig):
    """Create stopping criteria node."""

    def stopper_node(state: ResearchState) -> Dict:
        """Evaluate if we should stop."""
        node_started_at = time.perf_counter()
        round_num = state.get("round_number", 0)
        queries = state.get("queries_used", 0)
        tokens = state.get("tokens_used", 0)
        start_time = state.get("start_time", 0.0) or 0.0
        elapsed = time.time() - float(start_time) if start_time else 0.0
        seen_hashes = state.get("seen_hashes", {})
        total_unique = len(seen_hashes)
        novelty_history = state.get("novelty_history", [])
        gate = evaluate_research_outcome(
            round_number=int(round_num),
            queries_used=int(queries),
            tokens_used=int(tokens),
            elapsed_sec=float(elapsed),
            items_found=int(total_unique),
            novelty_history=novelty_history if isinstance(novelty_history, list) else [],
            max_rounds=config.max_rounds,
            budget_queries=config.budget_queries,
            budget_tokens=config.budget_tokens,
            budget_time_sec=config.budget_time_sec,
            target_items=config.target_items,
            plateau_rounds=config.plateau_rounds,
            novelty_min=config.novelty_min,
            recursion_stop=should_stop_for_recursion(
                state,
                buffer=_effective_recursion_stop_buffer(state),
            ),
        )

        if gate.get("should_stop"):
            return _with_node_timing(
                {
                    "status": "complete",
                    "stop_reason": gate.get("stop_reason"),
                    "completion_gate": gate,
                },
                node="stopper",
                started_at=node_started_at,
            )

        return _with_node_timing(
            {"status": "searching", "completion_gate": gate},
            node="stopper",
            started_at=node_started_at,
        )

    return stopper_node


# ────────────────────────────────────────────────────────────────────────
# Graph Construction
# ────────────────────────────────────────────────────────────────────────
def should_continue(state: ResearchState) -> str:
    """Determine next step."""
    status = state.get("status", "")
    if status in ["complete", "failed"]:
        return "end"
    # Stop before LangGraph raises GraphRecursionError. Use a small buffer to be
    # robust across LangGraph versions / semantics for RemainingSteps.
    if should_stop_for_recursion(state, buffer=_effective_recursion_stop_buffer(state)):
        return "end"
    if status == "searching":
        return "search"
    return "end"


def create_research_graph(
    llm,
    tools: List,
    config: ResearchConfig = None,
    checkpointer=None,
    wikidata_tool=None
):
    """Create the research orchestration graph."""
    config = config or ResearchConfig()

    # Create nodes
    planner = create_planner_node(llm)
    searcher = create_searcher_node(llm, tools, wikidata_tool, research_config=config)
    deduper = create_deduper_node()
    stopper = create_stopper_node(config)

    # Build graph
    workflow = StateGraph(ResearchState)

    workflow.add_node("planner", planner)
    workflow.add_node("searcher", searcher)
    workflow.add_node("deduper", deduper)
    workflow.add_node("stopper", stopper)

    workflow.set_entry_point("planner")

    # Edges
    workflow.add_conditional_edges(
        "planner",
        lambda s: "end" if s.get("status") == "failed" else "search",
        {"search": "searcher", "end": END}
    )
    workflow.add_conditional_edges(
        "searcher",
        lambda s: "end" if s.get("status") in ["complete", "failed"] else "dedupe",
        {"dedupe": "deduper", "end": END}
    )
    workflow.add_conditional_edges(
        "deduper",
        lambda s: "end" if s.get("status") in ["complete", "failed"] else "stopper",
        {"stopper": "stopper", "end": END}
    )
    workflow.add_conditional_edges(
        "stopper",
        should_continue,
        {"search": "searcher", "end": END}
    )

    if checkpointer:
        return workflow.compile(checkpointer=checkpointer)
    return workflow.compile()


def _resolve_thread_id(query: str, thread_id: Optional[str] = None) -> str:
    """Resolve thread id, generating a deterministic short id when absent."""
    if thread_id:
        return thread_id
    return hashlib.md5(f"{query}_{time.time()}".encode()).hexdigest()[:16]


def _build_initial_state(
    query: str,
    schema: Dict[str, str],
    config_dict: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build the initial research state shared by sync and stream execution."""
    return {
        "query": query,
        "schema": schema,
        "config": config_dict or {},
        "plan": {},
        "entity_type": "",
        "wikidata_type": None,
        "results": [],
        "new_results": [],
        "seen_hashes": {},
        "novelty_history": [],
        "round_number": 0,
        "queries_used": 0,
        "tokens_used": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "token_trace": [],
        "start_time": time.time(),
        "status": "planning",
        "stop_reason": None,
        "completion_gate": {},
        "errors": []
    }


def _resolve_invoke_recursion_limit(config_dict: Optional[Dict[str, Any]]) -> Optional[int]:
    """Extract and validate recursion_limit for graph invoke/stream config."""
    if config_dict is None:
        return None

    # Common typo guard: fail fast instead of silently falling back to defaults.
    config_keys = []
    try:
        if isinstance(config_dict, dict):
            config_keys = list(config_dict.keys())
        elif hasattr(config_dict, "keys"):
            config_keys = list(config_dict.keys())
    except Exception:
        # Ignore key introspection failures; continue with normal parsing.
        config_keys = []
    if "recusion_limit" in config_keys and "recursion_limit" not in config_keys:
        raise ValueError(
            "Unknown config key 'recusion_limit'. "
            "Did you mean 'recursion_limit'?"
        )

    recursion_limit = None
    try:
        if isinstance(config_dict, dict):
            recursion_limit = config_dict.get("recursion_limit")
        elif hasattr(config_dict, "get"):
            recursion_limit = config_dict.get("recursion_limit")
    except Exception:
        recursion_limit = None

    if recursion_limit is None:
        return None

    try:
        resolved = int(recursion_limit)
    except Exception:
        raise ValueError(
            "recursion_limit must be an integer between 4 and 500; "
            f"got {recursion_limit!r}"
        )

    if resolved < 4 or resolved > 500:
        raise ValueError(
            "recursion_limit must be between 4 and 500; "
            f"got {resolved}"
        )

    return resolved


def _build_langgraph_run_config(
    thread_id: str,
    config_dict: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build LangGraph runtime config, including recursion limit when provided."""
    config = {"configurable": {"thread_id": thread_id}}
    recursion_limit = _resolve_invoke_recursion_limit(config_dict)
    if recursion_limit is not None:
        config["recursion_limit"] = recursion_limit
    return config


def _build_research_result(
    state: Dict[str, Any],
    elapsed: float,
    status_fallback: str = "unknown"
) -> Dict[str, Any]:
    """Build normalized research results from a completed state."""
    state = state or {}
    results = state.get("results", [])
    provenance = []
    for idx, row in enumerate(results):
        if not isinstance(row, dict):
            continue
        row_id = row.get("row_id") or f"row_{idx}"
        confidence_raw = row.get("confidence")
        try:
            confidence = float(confidence_raw) if confidence_raw is not None else None
        except Exception:
            confidence = None
        provenance.append({
            "row_id": row_id,
            "source_url": row.get("source_url") or "",
            "confidence": confidence,
            "worker_id": row.get("worker_id") or "unknown",
            "extraction_timestamp": row.get("extraction_timestamp"),
        })
    completion_gate = state.get("completion_gate")
    if not isinstance(completion_gate, dict):
        completion_gate = {}
    verification_status = completion_gate.get("completion_status")

    return {
        "results": results,
        "provenance": provenance,
        "metrics": {
            "round_number": state.get("round_number", 0),
            "queries_used": state.get("queries_used", 0),
            "tokens_used": state.get("tokens_used", 0),
            "input_tokens": state.get("input_tokens", 0),
            "output_tokens": state.get("output_tokens", 0),
            "token_trace": state.get("token_trace", []),
            "time_elapsed": elapsed,
            "items_found": len(results)
        },
        "status": state.get("status", status_fallback),
        "verification_status": verification_status,
        "stop_reason": state.get("stop_reason"),
        "completion_gate": completion_gate,
        "errors": state.get("errors", []),
        "plan": state.get("plan", {})
    }


def _build_research_error_result(
    error: Exception,
    elapsed: float,
    state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build normalized failed research results."""
    state = state or {}
    results = state.get("results", [])
    provenance = []
    for idx, row in enumerate(results):
        if not isinstance(row, dict):
            continue
        provenance.append({
            "row_id": row.get("row_id") or f"row_{idx}",
            "source_url": row.get("source_url") or "",
            "confidence": row.get("confidence"),
            "worker_id": row.get("worker_id") or "unknown",
            "extraction_timestamp": row.get("extraction_timestamp"),
        })
    completion_gate = state.get("completion_gate")
    if not isinstance(completion_gate, dict):
        completion_gate = {}

    return {
        "results": results,
        "provenance": provenance,
        "metrics": {"time_elapsed": elapsed},
        "status": "failed",
        "verification_status": completion_gate.get("completion_status"),
        "stop_reason": f"execution_error: {str(error)}",
        "completion_gate": completion_gate,
        "errors": [{"stage": "execution", "error": str(error)}],
        "plan": state.get("plan", {})
    }


def run_research(
    graph,
    query: str,
    schema: Dict[str, str],
    config_dict: Dict[str, Any] = None,
    thread_id: str = None
) -> Dict[str, Any]:
    """Execute research graph and return results."""
    thread_id = _resolve_thread_id(query, thread_id)
    initial_state = _build_initial_state(query, schema, config_dict)

    config = _build_langgraph_run_config(thread_id, config_dict)
    start_time = time.time()

    try:
        final_state = graph.invoke(initial_state, config)
        elapsed = time.time() - start_time

        return _build_research_result(final_state, elapsed, status_fallback="unknown")

    except Exception as e:
        logger.error(f"Research error: {e}")
        return _build_research_error_result(e, time.time() - start_time)


def stream_research(
    graph,
    query: str,
    schema: Dict[str, str],
    config_dict: Dict[str, Any] = None,
    thread_id: str = None
):
    """Stream research progress and return final state.

    Yields progress events during execution, with the final 'complete' event
    containing the full result (same format as run_research).
    """
    thread_id = _resolve_thread_id(query, thread_id)
    initial_state = _build_initial_state(query, schema, config_dict)

    config = _build_langgraph_run_config(thread_id, config_dict)
    start_time = time.time()

    # Accumulate state updates (stream_mode="updates" gives partial updates per node)
    accumulated_state = dict(initial_state)

    # Keys that use list-append reducers in ResearchState (Annotated[..., add_to_list])
    _LIST_REDUCER_KEYS = {"results", "novelty_history", "errors"}
    # Keys that use dict-merge reducers in ResearchState (Annotated[..., merge_dicts])
    _DICT_REDUCER_KEYS = {"seen_hashes", "completion_gate"}

    try:
        for event in graph.stream(initial_state, config, stream_mode="updates"):
            node_name = list(event.keys())[0] if event else "unknown"
            node_state = event.get(node_name, {})

            # Merge node updates into accumulated state, respecting reducer semantics
            for key, value in node_state.items():
                if key in _LIST_REDUCER_KEYS:
                    accumulated_state.setdefault(key, []).extend(
                        value if isinstance(value, list) else [value]
                    )
                elif key in _DICT_REDUCER_KEYS:
                    accumulated_state.setdefault(key, {}).update(
                        value if isinstance(value, dict) else {}
                    )
                else:
                    accumulated_state[key] = value

            yield {
                "event_type": "node_update",
                "node": node_name,
                "status": node_state.get("status", "running"),
                "items_found": len(accumulated_state.get("results", [])),
                "elapsed": time.time() - start_time
            }

        # Build final result in same format as run_research()
        elapsed = time.time() - start_time
        final_result = _build_research_result(
            accumulated_state,
            elapsed,
            status_fallback="complete"
        )

        yield {
            "event_type": "complete",
            "elapsed": elapsed,
            "final_result": final_result
        }

    except Exception as e:
        elapsed = time.time() - start_time
        error_result = _build_research_error_result(e, elapsed, state=accumulated_state)
        yield {
            "event_type": "error",
            "error": str(e),
            "elapsed": elapsed,
            "final_result": error_result
        }
