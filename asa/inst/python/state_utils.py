# state_utils.py
#
# Shared utilities for LangGraph state management.
# Provides state reducers, JSON parsing, and query parsing functions.
#
import json
import re
from typing import Any, Dict, List, Optional, Tuple


def remaining_steps_value(state: Any) -> Optional[int]:
    """Safely extract remaining_steps from LangGraph state.

    Returns an int when available, otherwise None.
    """
    val = None
    try:
        if isinstance(state, dict):
            val = state.get("remaining_steps")
        else:
            val = getattr(state, "remaining_steps", None)
    except Exception:
        val = None

    if val is None:
        return None

    try:
        return int(val)
    except Exception:
        try:
            return int(getattr(val, "value"))
        except Exception:
            return None


def should_stop_for_recursion(state: Any, buffer: int = 1) -> bool:
    """Return True when RemainingSteps is at/under buffer."""
    remaining = remaining_steps_value(state)
    return remaining is not None and remaining <= buffer


def add_to_list(existing: List, new: List) -> List:
    """Reducer for appending lists (LangGraph state annotation).

    Args:
        existing: Current list in state (may be None)
        new: New list to append (may be None)

    Returns:
        Combined list
    """
    if existing is None:
        existing = []
    if new is None:
        new = []
    return existing + new


def merge_dicts(existing: Dict, new: Dict) -> Dict:
    """Reducer for merging dictionaries (LangGraph state annotation).

    Args:
        existing: Current dict in state (may be None)
        new: New dict to merge (may be None)

    Returns:
        Merged dictionary (new values override existing)
    """
    if existing is None:
        existing = {}
    if new is None:
        new = {}
    result = existing.copy()
    result.update(new)
    return result


def _scan_json_substring(content: str, start: int) -> Optional[str]:
    """Scan for a JSON object/array starting at index and return substring."""
    stack = []
    in_string = False
    escape_next = False

    for i in range(start, len(content)):
        char = content[i]

        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == '"' and not escape_next:
            in_string = not in_string
            continue

        if not in_string:
            if char in "{[":
                stack.append(char)
            elif char in "}]":
                if not stack:
                    return None
                opening = stack.pop()
                if (opening == "{" and char != "}") or (opening == "[" and char != "]"):
                    return None
                if not stack:
                    return content[start:i + 1]

    return None


def _extract_json_substring(content: str) -> Optional[str]:
    """Extract the first valid JSON object/array substring from text."""
    for i, char in enumerate(content):
        if char in "{[":
            snippet = _scan_json_substring(content, i)
            if snippet:
                return snippet
    return None


def parse_llm_json(content: str) -> Any:
    """Parse JSON from LLM response, handling markdown code blocks.

    Handles common LLM output patterns:
    - Raw JSON
    - JSON wrapped in ```json ... ``` blocks
    - JSON wrapped in ``` ... ``` blocks
    - JSON embedded in text

    Args:
        content: Raw LLM response text

    Returns:
        Parsed object (dict or list), or empty dict if parsing fails
    """
    if not content:
        return {}

    content = content.strip()

    # Remove markdown code blocks
    if content.startswith("```"):
        parts = content.split("```")
        if len(parts) >= 2:
            content = parts[1]
            # Remove language identifier if present
            if content.startswith("json"):
                content = content[4:]
            elif content.startswith("\n"):
                content = content[1:]

    content = content.strip()

    # Try direct parse first
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    # Try scanning for a JSON object or array in text
    extracted = _extract_json_substring(content)
    if extracted:
        try:
            return json.loads(extracted)
        except json.JSONDecodeError:
            pass

    # Try to find JSON object in text (legacy patterns)
    json_patterns = [
        r'```json\s*(.*?)\s*```',
        r'```\s*(.*?)\s*```',
        r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    ]

    for pattern in json_patterns:
        matches = re.findall(pattern, content, re.DOTALL)
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

    return {}


def parse_date_filters(query: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Extract after:/before: date filters from query string.

    Looks for patterns like "after:2020-01-01" and "before:2024-01-01"
    and removes them from the query string.

    Args:
        query: Query string potentially containing date filters

    Returns:
        Tuple of (cleaned_query, date_after, date_before)
        where date_after and date_before are ISO 8601 date strings or None
    """
    date_after: Optional[str] = None
    date_before: Optional[str] = None
    cleaned = query

    # Pattern for after:YYYY-MM-DD
    after_match = re.search(r'\bafter:(\d{4}-\d{2}-\d{2})\b', query)
    if after_match:
        date_after = after_match.group(1)
        cleaned = cleaned.replace(after_match.group(0), "").strip()

    # Pattern for before:YYYY-MM-DD
    before_match = re.search(r'\bbefore:(\d{4}-\d{2}-\d{2})\b', query)
    if before_match:
        date_before = before_match.group(1)
        cleaned = cleaned.replace(before_match.group(0), "").strip()

    # Clean up any double spaces
    cleaned = " ".join(cleaned.split())

    return cleaned, date_after, date_before
