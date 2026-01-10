# wikidata_tool.py
#
# LangChain-compatible Wikidata SPARQL tool for authoritative entity enumeration.
# Provides structured queries for known entity types (senators, countries, etc.)
#
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import requests
from langchain_core.tools import BaseTool
from pydantic import Field

from state_utils import parse_date_filters
from http_utils import make_request, DEFAULT_USER_AGENT

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────
WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
DEFAULT_TIMEOUT = 60.0
DEFAULT_USER_AGENT = "ASA-Research-Agent/1.0 (https://github.com/cjerzak/asa-software)"


@dataclass
class WikidataConfig:
    """Configuration for Wikidata queries."""
    timeout: float = DEFAULT_TIMEOUT
    max_results: int = 500
    retry_count: int = 3
    retry_delay: float = 2.0
    # Temporal filtering
    date_after: Optional[str] = None   # ISO 8601: "2020-01-01"
    date_before: Optional[str] = None  # ISO 8601: "2024-01-01"


def _build_temporal_filter(date_after: Optional[str], date_before: Optional[str]) -> str:
    """Build SPARQL FILTER clause for temporal constraints.

    This generates filters that work with Wikidata's temporal qualifiers:
    - P580 (start time): When something began
    - P582 (end time): When something ended

    For entities that are "current" (no end date), we treat them as still valid.

    Args:
        date_after: Only include entities valid AFTER this date (ISO 8601)
        date_before: Only include entities valid BEFORE this date (ISO 8601)

    Returns:
        SPARQL FILTER clause string (or empty string if no constraints)
    """
    filters = []

    if date_after:
        # Entity must be valid after this date
        # Either: no end date (still valid), OR end date is after our threshold
        filters.append(f'''
  FILTER(
    !BOUND(?termEnd) ||
    ?termEnd >= "{date_after}T00:00:00Z"^^xsd:dateTime
  )''')

    if date_before:
        # Entity must have started before this date
        # Either: no start date (unknown), OR start date is before our threshold
        filters.append(f'''
  FILTER(
    !BOUND(?termStart) ||
    ?termStart < "{date_before}T00:00:00Z"^^xsd:dateTime
  )''')

    return "\n".join(filters)


def _inject_temporal_filter(query: str, date_after: Optional[str], date_before: Optional[str]) -> str:
    """Inject temporal filtering into a SPARQL query.

    Looks for existing OPTIONAL clauses for P580/P582 and adds FILTER clauses.
    If the query doesn't have these optionals, adds them.

    Args:
        query: Original SPARQL query
        date_after: Filter entities valid after this date
        date_before: Filter entities valid before this date

    Returns:
        Modified SPARQL query with temporal filters
    """
    if not date_after and not date_before:
        return query

    temporal_filter = _build_temporal_filter(date_after, date_before)

    if not temporal_filter:
        return query

    # Check if query already has termStart/termEnd variables
    has_term_start = "?termStart" in query or "pq:P580" in query
    has_term_end = "?termEnd" in query or "pq:P582" in query

    # If query uses different variable names, try to adapt
    # Common patterns: ?start, ?end, ?startTime, ?endTime
    if not has_term_start and "?start" in query:
        temporal_filter = temporal_filter.replace("?termStart", "?start")
        has_term_start = True
    if not has_term_end and "?end" in query:
        temporal_filter = temporal_filter.replace("?termEnd", "?end")
        has_term_end = True

    # Insert filter before SERVICE or closing brace
    if "SERVICE wikibase:label" in query:
        # Insert before SERVICE clause
        parts = query.rsplit("SERVICE wikibase:label", 1)
        modified = parts[0] + temporal_filter + "\n  SERVICE wikibase:label" + parts[1]
    elif query.rstrip().endswith("}"):
        # Insert before closing brace
        modified = query.rstrip()[:-1] + temporal_filter + "\n}"
    else:
        modified = query + temporal_filter

    logger.info(f"Injected temporal filter: after={date_after}, before={date_before}")
    return modified


# Known entity type templates for common enumeration queries
ENTITY_TEMPLATES = {
    "us_senators": {
        "description": "Current United States Senators",
        "query": """
SELECT DISTINCT ?person ?personLabel ?state ?stateLabel ?party ?partyLabel
       ?termStart ?termEnd ?website
WHERE {
  ?person wdt:P31 wd:Q5 .  # instance of: human (filter out fictional)
  ?person wdt:P39 wd:Q4416090 .  # position held: United States senator
  ?person p:P39 ?statement .
  ?statement ps:P39 wd:Q4416090 .
  OPTIONAL { ?statement pq:P580 ?termStart . }
  OPTIONAL { ?statement pq:P582 ?termEnd . }
  OPTIONAL { ?statement pq:P768 ?state . }
  OPTIONAL { ?person wdt:P102 ?party . }
  OPTIONAL { ?person wdt:P856 ?website . }
  FILTER NOT EXISTS { ?statement pq:P582 ?end . FILTER(?end < NOW()) }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY ?stateLabel ?personLabel
""",
        "schema": ["name", "state", "party", "term_start", "term_end", "website", "wikidata_id"]
    },
    "us_representatives": {
        "description": "Current United States Representatives",
        "query": """
SELECT DISTINCT ?person ?personLabel ?district ?districtLabel ?state ?stateLabel ?party ?partyLabel
WHERE {
  ?person wdt:P39 wd:Q13218630 .  # position held: member of US House
  ?person p:P39 ?statement .
  ?statement ps:P39 wd:Q13218630 .
  OPTIONAL { ?statement pq:P768 ?district . }
  OPTIONAL { ?district wdt:P131 ?state . }
  OPTIONAL { ?person wdt:P102 ?party . }
  FILTER NOT EXISTS { ?statement pq:P582 ?end . FILTER(?end < NOW()) }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY ?stateLabel ?personLabel
""",
        "schema": ["name", "district", "state", "party", "wikidata_id"]
    },
    "countries": {
        "description": "All countries in the world",
        "query": """
SELECT DISTINCT ?country ?countryLabel ?capital ?capitalLabel ?population ?area ?iso3
WHERE {
  ?country wdt:P31 wd:Q6256 .  # instance of: country
  OPTIONAL { ?country wdt:P36 ?capital . }
  OPTIONAL { ?country wdt:P1082 ?population . }
  OPTIONAL { ?country wdt:P2046 ?area . }
  OPTIONAL { ?country wdt:P298 ?iso3 . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY ?countryLabel
""",
        "schema": ["name", "capital", "population", "area", "iso3", "wikidata_id"]
    },
    "fortune500": {
        "description": "Fortune 500 companies",
        "query": """
SELECT DISTINCT ?company ?companyLabel ?ceo ?ceoLabel ?industry ?industryLabel
       ?founded ?headquarters ?headquartersLabel ?website
WHERE {
  ?company wdt:P361 wd:Q631014 .  # part of: Fortune 500
  OPTIONAL { ?company wdt:P169 ?ceo . }
  OPTIONAL { ?company wdt:P452 ?industry . }
  OPTIONAL { ?company wdt:P571 ?founded . }
  OPTIONAL { ?company wdt:P159 ?headquarters . }
  OPTIONAL { ?company wdt:P856 ?website . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY ?companyLabel
""",
        "schema": ["name", "ceo", "industry", "founded", "headquarters", "website", "wikidata_id"]
    },
    "us_states": {
        "description": "US States and territories",
        "query": """
SELECT DISTINCT ?state ?stateLabel ?capital ?capitalLabel ?population ?admission ?abbreviation
WHERE {
  ?state wdt:P31 wd:Q35657 .  # instance of: US state
  OPTIONAL { ?state wdt:P36 ?capital . }
  OPTIONAL { ?state wdt:P1082 ?population . }
  OPTIONAL { ?state wdt:P571 ?admission . }
  OPTIONAL { ?state wdt:P5087 ?abbreviation . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY ?stateLabel
""",
        "schema": ["name", "capital", "population", "admission_date", "abbreviation", "wikidata_id"]
    }
}


def _extract_wikidata_id(uri: str) -> str:
    """Extract Wikidata ID (Q-number) from URI."""
    if uri and "wikidata.org" in uri:
        return uri.split("/")[-1]
    return uri or ""


def _parse_sparql_results(results: Dict, entity_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """Parse SPARQL JSON results into list of dictionaries."""
    parsed = []
    bindings = results.get("results", {}).get("bindings", [])

    for row in bindings:
        item = {}
        for key, value in row.items():
            val = value.get("value", "")
            # Handle Label fields specially
            if key.endswith("Label"):
                base_key = key[:-5]  # Remove "Label" suffix
                # Prefer the label over raw URI
                if base_key in item and item[base_key].startswith("http"):
                    item[base_key] = val
                elif base_key not in item:
                    item[base_key] = val
            elif key in ["person", "country", "company", "state"]:
                # Extract Wikidata ID for primary entity
                item["wikidata_id"] = _extract_wikidata_id(val)
                if f"{key}Label" not in row:
                    item["name"] = val
            else:
                # Skip raw URIs if we have a label
                if not val.startswith("http://www.wikidata.org"):
                    item[key] = val

        # Normalize common field names
        if "personLabel" in row:
            item["name"] = row["personLabel"].get("value", "")
        elif "countryLabel" in row:
            item["name"] = row["countryLabel"].get("value", "")
        elif "companyLabel" in row:
            item["name"] = row["companyLabel"].get("value", "")
        elif "stateLabel" in row and "name" not in item:
            item["name"] = row["stateLabel"].get("value", "")

        parsed.append(item)

    return parsed


def execute_sparql_query(
    query: str,
    config: Optional[WikidataConfig] = None,
    timeout: Optional[float] = None
) -> List[Dict[str, Any]]:
    """Execute a SPARQL query against Wikidata endpoint.

    Args:
        query: SPARQL query string
        config: Optional WikidataConfig for timeouts/retries
        timeout: Override timeout in seconds

    Returns:
        List of dictionaries with query results

    Raises:
        requests.RequestException: On network errors after retries
    """
    config = config or WikidataConfig()
    timeout = timeout or config.timeout

    logger.info("Executing Wikidata SPARQL query")

    response = make_request(
        url=WIKIDATA_SPARQL_ENDPOINT,
        method="GET",
        params={"query": query, "format": "json"},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
        max_retries=config.retry_count,
        retry_delay=config.retry_delay,
    )

    results = response.json()
    parsed = _parse_sparql_results(results)
    logger.info(f"Wikidata query returned {len(parsed)} results")
    return parsed


def get_known_entity_types() -> List[str]:
    """Return list of known entity types with pre-built queries."""
    return list(ENTITY_TEMPLATES.keys())


def get_entity_template(entity_type: str) -> Optional[Dict]:
    """Get the template for a known entity type."""
    return ENTITY_TEMPLATES.get(entity_type)


def query_known_entity(
    entity_type: str,
    config: Optional[WikidataConfig] = None,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Query Wikidata for a known entity type using pre-built query.

    Args:
        entity_type: One of the known entity types (us_senators, countries, etc.)
        config: Optional configuration
        date_after: Only include entities valid AFTER this date (ISO 8601)
        date_before: Only include entities valid BEFORE this date (ISO 8601)

    Returns:
        List of entity dictionaries

    Raises:
        ValueError: If entity_type is not recognized
    """
    template = ENTITY_TEMPLATES.get(entity_type)
    if not template:
        raise ValueError(
            f"Unknown entity type: {entity_type}. "
            f"Known types: {', '.join(ENTITY_TEMPLATES.keys())}"
        )

    # Use config temporal params if not explicitly provided
    config = config or WikidataConfig()
    date_after = date_after or config.date_after
    date_before = date_before or config.date_before

    # Inject temporal filter into the query if needed
    query = template["query"]
    if date_after or date_before:
        query = _inject_temporal_filter(query, date_after, date_before)
        logger.info(f"Temporal filter applied: after={date_after}, before={date_before}")

    logger.info(f"Querying Wikidata for entity type: {entity_type}")
    return execute_sparql_query(query, config)


def infer_entity_type(query: str) -> Optional[str]:
    """Attempt to infer the entity type from a natural language query.

    Args:
        query: Natural language query string

    Returns:
        Matched entity type or None if no match found
    """
    query_lower = query.lower()

    # Keywords for each entity type
    keyword_map = {
        "us_senators": ["senator", "senators", "senate", "us senate", "united states senator"],
        "us_representatives": ["representative", "representatives", "congress", "house of representatives", "congressman", "congresswoman"],
        "countries": ["countries", "country", "nations", "nation", "sovereign"],
        "fortune500": ["fortune 500", "fortune500", "largest companies", "biggest companies"],
        "us_states": ["us states", "american states", "states of america", "50 states"]
    }

    for entity_type, keywords in keyword_map.items():
        for keyword in keywords:
            if keyword in query_lower:
                logger.info(f"Inferred entity type '{entity_type}' from query: {query}")
                return entity_type

    return None


# ────────────────────────────────────────────────────────────────────────
# LangChain Tool Interface
# ────────────────────────────────────────────────────────────────────────
class WikidataSearchTool(BaseTool):
    """LangChain tool for querying Wikidata SPARQL endpoint.

    This tool provides authoritative enumeration of entities like
    US Senators, countries, Fortune 500 companies, etc.
    """

    name: str = "wikidata_search"
    description: str = """Search Wikidata for authoritative lists of entities.

    Best for enumeration queries like:
    - "all current US senators"
    - "all countries in the world"
    - "Fortune 500 companies"
    - "US states and their capitals"

    For known entity types, provides complete authoritative data with Wikidata IDs.
    For custom queries, can execute raw SPARQL.

    Supports temporal filtering:
    - "entity_type:us_senators after:2020-01-01"
    - "entity_type:fortune500 before:2015-01-01"
    - "entity_type:countries after:2000-01-01 before:2020-01-01"

    Input format:
    - For known types: "entity_type:us_senators" or "entity_type:countries"
    - For inference: just describe what you want, e.g., "all US senators"
    - For raw SPARQL: "sparql:SELECT ..."
    - With temporal: add "after:YYYY-MM-DD" and/or "before:YYYY-MM-DD"
    """

    config: WikidataConfig = Field(default_factory=WikidataConfig)
    # Temporal parameters can be set externally
    date_after: Optional[str] = None
    date_before: Optional[str] = None

    def _parse_temporal_from_query(self, query: str) -> tuple:
        """Extract temporal parameters from query string.

        Uses shared parse_date_filters utility.

        Returns:
            Tuple of (cleaned_query, date_after, date_before)
        """
        cleaned, date_after, date_before = parse_date_filters(query)
        # Use instance defaults if not found in query
        return cleaned, date_after or self.date_after, date_before or self.date_before

    def _run(self, query: str) -> str:
        """Execute Wikidata search and return formatted results."""
        try:
            # Parse temporal parameters from query string
            query, date_after, date_before = self._parse_temporal_from_query(query)

            # Check for explicit entity type prefix
            if query.startswith("entity_type:"):
                entity_type = query.split(":", 1)[1].strip()
                results = query_known_entity(
                    entity_type, self.config,
                    date_after=date_after, date_before=date_before
                )
                return self._format_results(results, entity_type)

            # Check for raw SPARQL prefix
            if query.startswith("sparql:"):
                sparql_query = query.split(":", 1)[1].strip()
                # Inject temporal filter into raw SPARQL if provided
                if date_after or date_before:
                    sparql_query = _inject_temporal_filter(sparql_query, date_after, date_before)
                results = execute_sparql_query(sparql_query, self.config)
                return self._format_results(results)

            # Try to infer entity type from natural language
            entity_type = infer_entity_type(query)
            if entity_type:
                results = query_known_entity(
                    entity_type, self.config,
                    date_after=date_after, date_before=date_before
                )
                return self._format_results(results, entity_type)

            # No match - return guidance
            return (
                f"Could not match query to known entity type. "
                f"Known types: {', '.join(get_known_entity_types())}. "
                f"Use 'entity_type:TYPE' for explicit queries or try rephrasing."
            )

        except Exception as e:
            logger.error(f"Wikidata search error: {e}")
            return f"Error querying Wikidata: {str(e)}"

    def _format_results(
        self,
        results: List[Dict[str, Any]],
        entity_type: Optional[str] = None
    ) -> str:
        """Format results as human-readable string."""
        if not results:
            return "No results found."

        # Get schema if known entity type
        schema = None
        if entity_type and entity_type in ENTITY_TEMPLATES:
            schema = ENTITY_TEMPLATES[entity_type].get("schema", [])

        lines = [f"Found {len(results)} results:"]

        for i, item in enumerate(results[:50], 1):  # Limit display to first 50
            # Build display line with key fields
            parts = []
            if "name" in item:
                parts.append(item["name"])
            if "state" in item:
                parts.append(f"({item['state']})")
            if "party" in item:
                parts.append(f"[{item['party']}]")
            if "wikidata_id" in item:
                parts.append(f"[{item['wikidata_id']}]")

            lines.append(f"{i}. {' '.join(parts)}")

        if len(results) > 50:
            lines.append(f"... and {len(results) - 50} more results.")

        return "\n".join(lines)

    async def _arun(self, query: str) -> str:
        """Async version - just calls sync for now."""
        return self._run(query)


def create_wikidata_tool(
    config: Optional[WikidataConfig] = None,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None
) -> WikidataSearchTool:
    """Factory function to create a WikidataSearchTool instance.

    Args:
        config: Optional WikidataConfig for customization
        date_after: Default temporal filter - only include entities valid after this date
        date_before: Default temporal filter - only include entities valid before this date

    Returns:
        Configured WikidataSearchTool instance
    """
    tool = WikidataSearchTool(config=config or WikidataConfig())
    if date_after:
        tool.date_after = date_after
    if date_before:
        tool.date_before = date_before
    return tool


# ────────────────────────────────────────────────────────────────────────
# Direct query functions for R integration
# ────────────────────────────────────────────────────────────────────────
def wikidata_query_senators() -> List[Dict[str, Any]]:
    """Query all current US Senators."""
    return query_known_entity("us_senators")


def wikidata_query_representatives() -> List[Dict[str, Any]]:
    """Query all current US Representatives."""
    return query_known_entity("us_representatives")


def wikidata_query_countries() -> List[Dict[str, Any]]:
    """Query all countries."""
    return query_known_entity("countries")


def wikidata_query_fortune500() -> List[Dict[str, Any]]:
    """Query Fortune 500 companies."""
    return query_known_entity("fortune500")


def wikidata_query_us_states() -> List[Dict[str, Any]]:
    """Query US states."""
    return query_known_entity("us_states")
