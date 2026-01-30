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
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from state_utils import add_to_list, merge_dicts, parse_llm_json
from wikidata_tool import query_known_entity

# Optional strict temporal verification (local module)
try:
    from date_extractor import verify_date_constraint
except Exception:  # pragma: no cover
    verify_date_constraint = None

logger = logging.getLogger(__name__)


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
    start_time: float
    status: str  # "planning", "searching", "complete", "failed"
    stop_reason: Optional[str]
    errors: Annotated[List[Dict], add_to_list]


# ────────────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────────────
def _hash_result(fields: Dict[str, Any], schema: Dict[str, str]) -> str:
    """Create a hash for deduplication based on key fields."""
    key_fields = list(schema.keys())[:3]
    key_values = []
    for field_name in key_fields:
        val = fields.get(field_name, "")
        if isinstance(val, str):
            val = val.lower().strip()
        key_values.append(str(val))
    key_string = "|".join(key_values)
    return hashlib.md5(key_string.encode()).hexdigest()


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


def _token_usage_from_message(message: Any) -> int:
    """Best-effort extraction of token usage from LangChain message objects."""
    if message is None:
        return 0

    # Newer LangChain: AIMessage.usage_metadata = {"input_tokens":..., "output_tokens":..., "total_tokens":...}
    usage = getattr(message, "usage_metadata", None)
    if isinstance(usage, dict):
        total = usage.get("total_tokens")
        if isinstance(total, (int, float)):
            return int(total)
        inp = usage.get("input_tokens")
        out = usage.get("output_tokens")
        if isinstance(inp, (int, float)) and isinstance(out, (int, float)):
            return int(inp + out)

    # Some providers: response_metadata contains usage/token_usage
    resp_meta = getattr(message, "response_metadata", None)
    if isinstance(resp_meta, dict):
        token_usage = resp_meta.get("token_usage") or resp_meta.get("usage") or {}
        if isinstance(token_usage, dict):
            total = token_usage.get("total_tokens")
            if isinstance(total, (int, float)):
                return int(total)
            inp = token_usage.get("prompt_tokens")
            out = token_usage.get("completion_tokens")
            if isinstance(inp, (int, float)) and isinstance(out, (int, float)):
                return int(inp + out)

    return 0


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
    if not date_str:
        return None
    try:
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


# ────────────────────────────────────────────────────────────────────────
# Planner Node
# ────────────────────────────────────────────────────────────────────────
PLANNER_PROMPT = """You are a research planner. Analyze the query and identify:
1. The entity type being requested
2. Whether this matches a known Wikidata entity type

Known Wikidata entity types:
- us_senators: Current United States Senators
- us_representatives: Current US Representatives
- countries: All countries in the world
- fortune500: Fortune 500 companies
- us_states: US States and territories

Query: {query}

Respond in JSON format ONLY (no markdown):
{{"entity_type": "description of entity type", "wikidata_type": "us_senators|us_representatives|countries|fortune500|us_states|null", "search_queries": ["query1", "query2"]}}
"""


def create_planner_node(llm):
    """Create the planner node."""

    def planner_node(state: ResearchState) -> Dict:
        """Analyze query and create research plan."""
        query = state.get("query", "")
        logger.info(f"Planner analyzing: {query}")

        prompt = PLANNER_PROMPT.format(query=query)
        messages = [HumanMessage(content=prompt)]

        try:
            response = llm.invoke(messages)
            plan = parse_llm_json(response.content)
            if not isinstance(plan, dict):
                plan = {}
            logger.info(f"Plan: entity={plan.get('entity_type')}, wikidata={plan.get('wikidata_type')}")

            return {
                "plan": plan,
                "entity_type": plan.get("entity_type", "unknown"),
                "wikidata_type": plan.get("wikidata_type"),
                "status": "searching",
                "queries_used": 1,
                "tokens_used": state.get("tokens_used", 0) + _token_usage_from_message(response),
            }

        except Exception as e:
            logger.error(f"Planner error: {e}")
            return {
                "status": "failed",
                "stop_reason": f"Planning failed: {str(e)}",
                "errors": [{"stage": "planner", "error": str(e)}]
            }

    return planner_node


# ────────────────────────────────────────────────────────────────────────
# Searcher Node
# ────────────────────────────────────────────────────────────────────────
def create_searcher_node(llm, tools, wikidata_tool=None, research_config: ResearchConfig = None):
    """Create the searcher node that executes searches."""

    def searcher_node(state: ResearchState) -> Dict:
        """Execute search based on plan."""
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
        target_items = config.get("target_items")
        if target_items is None and research_config:
            target_items = research_config.target_items

        results = []
        queries_used = state.get("queries_used", 0)
        tokens_used = state.get("tokens_used", 0)
        total_unique = len(seen_hashes)
        needs_more = target_items is not None and total_unique < target_items

        # Try Wikidata first if we have a matching type
        if wikidata_type and wikidata_tool and config.get("use_wikidata", True):
            logger.info(f"Querying Wikidata for: {wikidata_type}")
            try:
                wd_rows = query_known_entity(
                    wikidata_type,
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
                model_with_tools = llm.bind_tools(search_tools)
                tool_node = ToolNode(search_tools)

                search_prompt = f"""Search for: {query}
{query_hint}{temporal_hint}
Extract entities with these fields: {list(schema.keys())}
Use the Search tool to find information."""

                messages = [HumanMessage(content=search_prompt)]
                tool_outputs = []

                for _ in range(3):  # Max 3 tool calls
                    response = model_with_tools.invoke(messages)
                    tokens_used += _token_usage_from_message(response)
                    messages.append(response)
                    queries_used += 1

                    if hasattr(response, 'tool_calls') and response.tool_calls:
                        tool_result = tool_node.invoke({"messages": messages})
                        for msg in tool_result.get("messages", []):
                            messages.append(msg)
                            if hasattr(msg, "content"):
                                tool_outputs.append(str(msg.content))
                    else:
                        break

                # Extract structured entities from tool output
                if tool_outputs and schema:
                    extraction_prompt = (
                        "You are extracting structured entities from web search results.\n"
                        f"Query: {query}\n"
                        f"Temporal constraints: after={date_after or 'N/A'}, before={date_before or 'N/A'} (strictness={temporal_strictness}).\n"
                        "If strictness is 'strict', ONLY include entities supported by sources that clearly satisfy the date constraints; otherwise omit.\n"
                        f"Required fields: {list(schema.keys())}\n"
                        "Return ONLY a JSON array where each item has exactly those keys.\n"
                        "Use null for missing values. No markdown, no extra text.\n\n"
                        "Search results:\n"
                        + "\n\n".join(tool_outputs)
                    )

                    extraction_messages = [HumanMessage(content=extraction_prompt)]
                    extraction_response = llm.invoke(extraction_messages)
                    tokens_used += _token_usage_from_message(extraction_response)
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

        return {
            "new_results": results,
            "queries_used": queries_used,
            "tokens_used": tokens_used,
            "round_number": state.get("round_number", 0) + 1
        }

    return searcher_node


# ────────────────────────────────────────────────────────────────────────
# Deduplicator Node
# ────────────────────────────────────────────────────────────────────────
def create_deduper_node():
    """Create deduplication node."""

    def deduper_node(state: ResearchState) -> Dict:
        """Deduplicate results."""
        results = state.get("new_results")
        if results is None:
            results = state.get("results", [])
        schema = state.get("schema", {})
        seen_hashes = state.get("seen_hashes", {})

        unique_results = []
        new_hashes = {}

        for result in results:
            fields = result.get("fields", {})
            result_hash = _hash_result(fields, schema)

            if result_hash not in seen_hashes and result_hash not in new_hashes:
                # Fuzzy check
                is_dup = False
                name = fields.get("name", "")
                if name:
                    for existing in unique_results:
                        if _fuzzy_match_name(name, existing.get("fields", {}).get("name", "")) > 0.85:
                            is_dup = True
                            break

                if not is_dup:
                    unique_results.append(result)
                    new_hashes[result_hash] = True

        logger.info(f"Dedup: {len(results)} -> {len(unique_results)}")

        total_unique = len(seen_hashes) + len(new_hashes)
        novelty_rate = len(new_hashes) / max(1, total_unique)

        return {
            "results": unique_results,
            "seen_hashes": new_hashes,
            "novelty_history": [novelty_rate],
            "new_results": []
        }

    return deduper_node


# ────────────────────────────────────────────────────────────────────────
# Stopper Node
# ────────────────────────────────────────────────────────────────────────
def create_stopper_node(config: ResearchConfig):
    """Create stopping criteria node."""

    def stopper_node(state: ResearchState) -> Dict:
        """Evaluate if we should stop."""
        round_num = state.get("round_number", 0)
        queries = state.get("queries_used", 0)
        tokens = state.get("tokens_used", 0)
        start_time = state.get("start_time", 0.0) or 0.0
        elapsed = time.time() - float(start_time) if start_time else 0.0
        seen_hashes = state.get("seen_hashes", {})
        total_unique = len(seen_hashes)

        # Check limits
        if config.budget_time_sec and elapsed >= config.budget_time_sec:
            return {"status": "complete", "stop_reason": "budget_time"}

        if queries >= config.budget_queries:
            return {"status": "complete", "stop_reason": "budget_queries"}

        if config.budget_tokens and tokens >= config.budget_tokens:
            return {"status": "complete", "stop_reason": "budget_tokens"}

        if round_num >= config.max_rounds:
            return {"status": "complete", "stop_reason": "max_rounds"}

        if config.target_items and total_unique >= config.target_items:
            return {"status": "complete", "stop_reason": "target_reached"}

        novelty_history = state.get("novelty_history", [])
        if config.plateau_rounds and config.novelty_min is not None:
            if len(novelty_history) >= config.plateau_rounds:
                recent = novelty_history[-config.plateau_rounds:]
                if all(rate < config.novelty_min for rate in recent):
                    return {"status": "complete", "stop_reason": "novelty_plateau"}

        return {"status": "searching"}

    return stopper_node


# ────────────────────────────────────────────────────────────────────────
# Graph Construction
# ────────────────────────────────────────────────────────────────────────
def should_continue(state: ResearchState) -> str:
    """Determine next step."""
    status = state.get("status", "")
    if status in ["complete", "failed"]:
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
    workflow.add_edge("searcher", "deduper")
    workflow.add_edge("deduper", "stopper")
    workflow.add_conditional_edges(
        "stopper",
        should_continue,
        {"search": "searcher", "end": END}
    )

    if checkpointer:
        return workflow.compile(checkpointer=checkpointer)
    return workflow.compile()


def run_research(
    graph,
    query: str,
    schema: Dict[str, str],
    config_dict: Dict[str, Any] = None,
    thread_id: str = None
) -> Dict[str, Any]:
    """Execute research graph and return results."""
    if thread_id is None:
        thread_id = hashlib.md5(f"{query}_{time.time()}".encode()).hexdigest()[:16]

    initial_state = {
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
        "start_time": time.time(),
        "status": "planning",
        "stop_reason": None,
        "errors": []
    }

    config = {"configurable": {"thread_id": thread_id}}
    start_time = time.time()

    try:
        final_state = graph.invoke(initial_state, config)
        elapsed = time.time() - start_time

        return {
            "results": final_state.get("results", []),
            "provenance": [],
            "metrics": {
                "round_number": final_state.get("round_number", 0),
                "queries_used": final_state.get("queries_used", 0),
                "tokens_used": final_state.get("tokens_used", 0),
                "time_elapsed": elapsed,
                "items_found": len(final_state.get("results", []))
            },
            "status": final_state.get("status", "unknown"),
            "stop_reason": final_state.get("stop_reason"),
            "errors": final_state.get("errors", []),
            "plan": final_state.get("plan", {})
        }

    except Exception as e:
        logger.error(f"Research error: {e}")
        return {
            "results": [],
            "provenance": [],
            "metrics": {"time_elapsed": time.time() - start_time},
            "status": "failed",
            "stop_reason": f"execution_error: {str(e)}",
            "errors": [{"stage": "execution", "error": str(e)}],
            "plan": {}
        }


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
    if thread_id is None:
        thread_id = hashlib.md5(f"{query}_{time.time()}".encode()).hexdigest()[:16]

    initial_state = {
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
        "start_time": time.time(),
        "status": "planning",
        "stop_reason": None,
        "errors": []
    }

    config = {"configurable": {"thread_id": thread_id}}
    start_time = time.time()

    # Accumulate state updates (stream_mode="updates" gives partial updates per node)
    accumulated_state = dict(initial_state)

    try:
        for event in graph.stream(initial_state, config, stream_mode="updates"):
            node_name = list(event.keys())[0] if event else "unknown"
            node_state = event.get(node_name, {})

            # Merge node updates into accumulated state
            for key, value in node_state.items():
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
        final_result = {
            "results": accumulated_state.get("results", []),
            "provenance": [],
            "metrics": {
                "round_number": accumulated_state.get("round_number", 0),
                "queries_used": accumulated_state.get("queries_used", 0),
                "tokens_used": accumulated_state.get("tokens_used", 0),
                "time_elapsed": elapsed,
                "items_found": len(accumulated_state.get("results", []))
            },
            "status": accumulated_state.get("status", "complete"),
            "stop_reason": accumulated_state.get("stop_reason"),
            "errors": accumulated_state.get("errors", []),
            "plan": accumulated_state.get("plan", {})
        }

        yield {
            "event_type": "complete",
            "elapsed": elapsed,
            "final_result": final_result
        }

    except Exception as e:
        elapsed = time.time() - start_time
        error_result = {
            "results": accumulated_state.get("results", []),
            "provenance": [],
            "metrics": {"time_elapsed": elapsed},
            "status": "failed",
            "stop_reason": f"execution_error: {str(e)}",
            "errors": [{"stage": "execution", "error": str(e)}],
            "plan": accumulated_state.get("plan", {})
        }
        yield {
            "event_type": "error",
            "error": str(e),
            "elapsed": elapsed,
            "final_result": error_result
        }
