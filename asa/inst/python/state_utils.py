# state_utils.py
#
# Shared utilities for LangGraph state management.
# Provides state reducers, JSON parsing, and query parsing functions.
#
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple


def remaining_steps_value(state: Any) -> Optional[int]:
    """Safely extract remaining_steps from LangGraph state.

    Returns an int when available, otherwise None.
    """
    val = None
    try:
        # Try dict-like access first (works for dict, AddableValuesDict, etc.)
        # Use duck-typing instead of isinstance(dict) for broader compatibility
        if hasattr(state, "get"):
            val = state.get("remaining_steps")
        elif hasattr(state, "__getitem__"):
            try:
                val = state["remaining_steps"]
            except (KeyError, TypeError):
                val = None

        # Fallback to attribute access
        if val is None:
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


def hash_result(row: Dict[str, Any], schema: Dict[str, str],
                key_fields: Optional[List[str]] = None) -> str:
    """Create an MD5 hash for dedup based on selected schema fields.

    Args:
        row: Data row dict.
        schema: Schema dict whose keys define the field universe.
        key_fields: Explicit fields to hash.  Defaults to all schema keys.

    Returns:
        Hex MD5 digest string.
    """
    fields = key_fields if key_fields is not None else list(schema.keys())
    values = [str(row.get(f, "")).lower().strip() for f in fields]
    return hashlib.md5("|".join(values).encode()).hexdigest()


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


def _tokenize_jsonish_schema(text: str) -> List[Tuple[str, str]]:
    """Tokenize a JSON-ish schema string.

    This is intentionally tolerant: it supports non-JSON leaf values like
    `string|null` or `integer`, and only relies on quoted keys.
    """
    tokens: List[Tuple[str, str]] = []
    if not text:
        return tokens

    i = 0
    n = len(text)
    punct = set("{}[]:,")

    while i < n:
        ch = text[i]

        if ch.isspace():
            i += 1
            continue

        if ch in punct:
            tokens.append((ch, ch))
            i += 1
            continue

        if ch == '"':
            # Parse double-quoted string literal; keep the decoded content only.
            i += 1
            buf: List[str] = []
            escape_next = False
            while i < n:
                c = text[i]
                if escape_next:
                    buf.append(c)
                    escape_next = False
                    i += 1
                    continue
                if c == "\\":
                    escape_next = True
                    i += 1
                    continue
                if c == '"':
                    break
                buf.append(c)
                i += 1
            tokens.append(("STRING", "".join(buf)))
            if i < n and text[i] == '"':
                i += 1
            continue

        # OTHER token: read until whitespace or punctuation or string start.
        start = i
        while i < n and (not text[i].isspace()) and (text[i] not in punct) and (text[i] != '"'):
            i += 1
        tokens.append(("OTHER", text[start:i]))

    return tokens


def _parse_jsonish_schema(text: str) -> Any:
    """Parse a JSON-ish schema block into a lightweight schema tree.

    Schema representation:
      - dict: object keys -> subschema
      - list: array, with element subschema at index 0 (may be None/leaf)
      - str: leaf descriptor (e.g., "string|null")
      - None: unknown leaf
    """
    tokens = _tokenize_jsonish_schema(text)
    if not tokens:
        return None

    pos = 0

    def peek_type() -> Optional[str]:
        return tokens[pos][0] if pos < len(tokens) else None

    def consume(expected: Optional[str] = None) -> Optional[Tuple[str, str]]:
        nonlocal pos
        if pos >= len(tokens):
            return None
        tok = tokens[pos]
        if expected is not None and tok[0] != expected:
            return None
        pos += 1
        return tok

    def parse_value() -> Any:
        t = peek_type()
        if t == "{":
            return parse_object()
        if t == "[":
            return parse_array()

        # Leaf: consume until delimiter so callers don't see leftover tokens like `| null`.
        parts: List[str] = []
        while True:
            t2 = peek_type()
            if t2 is None or t2 in {",", "}", "]"}:
                break
            tok = consume()
            if tok is None:
                break
            parts.append(tok[1])
        leaf = "".join(parts).strip()
        return leaf or None

    def parse_object() -> Dict[str, Any]:
        obj: Dict[str, Any] = {}
        consume("{")
        while True:
            t = peek_type()
            if t is None:
                break
            if t == "}":
                consume("}")
                break
            if t == ",":
                consume(",")
                continue

            if t != "STRING":
                # Skip unexpected tokens for robustness.
                consume()
                continue

            key_tok = consume("STRING")
            if key_tok is None:
                break
            key = key_tok[1]

            # Advance to ':' (schema blocks can contain odd tokens).
            while True:
                t2 = peek_type()
                if t2 is None or t2 in {":", ",", "}", "]"}:
                    break
                consume()
            if peek_type() != ":":
                obj[key] = None
                continue
            consume(":")

            obj[key] = parse_value()

        return obj

    def parse_array() -> List[Any]:
        consume("[")

        # Skip leading commas, if any.
        while peek_type() == ",":
            consume(",")

        if peek_type() == "]":
            consume("]")
            return [None]

        elem_schema = parse_value()

        # Skip remaining array contents; we only need the element schema.
        while True:
            t = peek_type()
            if t is None:
                break
            if t == "]":
                consume("]")
                break
            consume()

        return [elem_schema]

    # Parse the first value in the token stream.
    return parse_value()


def infer_required_json_schema(text: str) -> Optional[Any]:
    """Infer a JSON-ish schema tree from free text.

    Heuristic: looks for a schema block following anchors like "exact schema"
    and parses quoted keys into an object/array shape.
    """
    if not text or not isinstance(text, str):
        return None

    # Prefer explicit anchors; avoid generic "schema" which is too noisy.
    anchor_re = re.compile(
        r"(this exact schema|exact schema|json schema|schema\\s*:)",
        re.IGNORECASE,
    )
    matches = list(anchor_re.finditer(text))
    if not matches:
        return None

    # Choose the most recent/last anchor in the text.
    start_at = matches[-1].start()

    # Bound scanning to reduce exposure to huge or adversarial blocks.
    MAX_SCAN_CHARS = 20000
    tail = text[start_at:start_at + MAX_SCAN_CHARS]

    # Find the first object/array block after the anchor.
    m = re.search(r"[\{\[]", tail)
    if not m:
        return None
    start = m.start()

    block = _scan_json_substring(tail, start)
    if not block:
        return None

    # Avoid pathological parsing work.
    MAX_SCHEMA_BLOCK_CHARS = 20000
    if len(block) > MAX_SCHEMA_BLOCK_CHARS:
        return None

    schema = _parse_jsonish_schema(block)
    # Require at least one key to avoid accidentally parsing unrelated braces.
    if isinstance(schema, dict) and len(schema) == 0:
        return None
    return schema


def _message_role(msg: Any) -> str:
    """Best-effort extraction of message role."""
    try:
        if isinstance(msg, dict):
            role = msg.get("role") or msg.get("type") or ""
            return str(role).lower()
        role = getattr(msg, "type", None)
        if isinstance(role, str):
            return role.lower()
    except Exception:
        pass

    try:
        return type(msg).__name__.lower()
    except Exception:
        return ""


def _message_content(msg: Any) -> str:
    """Best-effort extraction of message content as text."""
    try:
        if isinstance(msg, dict):
            content = msg.get("content", "")
            if content is None:
                return ""
            return str(content)
    except Exception:
        pass

    try:
        content = getattr(msg, "content", "")
        if content is None:
            return ""
        return str(content)
    except Exception:
        return ""


def infer_required_json_schema_from_messages(messages: Any) -> Optional[Any]:
    """Infer required JSON schema from a conversation message list.

    Only considers user/system/developer content (skips tool outputs).
    """
    if not messages:
        return None

    parts: List[str] = []
    # Cap total scanned text to avoid adversarial prompts/tool outputs.
    MAX_TOTAL_CHARS = 50000
    total = 0

    for msg in messages:
        role = _message_role(msg)
        if role in {"tool", "toolmessage", "function", "functionmessage"}:
            continue

        # Keep only user/system/dev messages for schema discovery.
        if role not in {"user", "human", "system", "developer"}:
            continue

        text = _message_content(msg)
        if not text:
            continue
        if len(text) > 20000:
            text = text[:20000]
        parts.append(text)
        total += len(text)
        if total >= MAX_TOTAL_CHARS:
            break

    return infer_required_json_schema("\n\n".join(parts))


def _default_leaf_value(descriptor: Any, key: Optional[str] = None) -> Any:
    """Default value for an unknown scalar leaf.

    Policy (best-effort):
    - If null is allowed: use None
    - If it's a string and null is disallowed: use "" (empty string)
    - If it's an enum like complete|partial: prefer "partial" when available
    - Otherwise: use None
    """
    if not isinstance(descriptor, str):
        return None

    lower = descriptor.lower().strip()
    if "null" in lower:
        return None

    # Prefer "partial" for common status enums when available.
    if key == "status" and "partial" in lower:
        return "partial"

    if "|" in descriptor:
        parts = [p.strip() for p in descriptor.split("|") if p.strip()]
        type_words = {"string", "integer", "number", "boolean", "null", "object", "array"}
        if parts and not any(p.lower() in type_words for p in parts):
            # Enum of literals (e.g., complete|partial).
            for p in parts:
                if p.lower() == "partial":
                    return p
            return parts[0]

    if "string" in lower:
        return ""

    return None


def list_missing_required_keys(data: Any, schema: Any, *, max_items: int = 1000) -> List[str]:
    """List missing required keys (and required nesting) for a schema tree.

    Returns dotted paths like "items[0].name". Caps output length via max_items.
    """
    missing: List[str] = []

    def add(path: str) -> None:
        if len(missing) >= max_items:
            return
        missing.append(path)

    def walk(value: Any, sch: Any, prefix: str) -> None:
        if len(missing) >= max_items:
            return

        if isinstance(sch, dict):
            if not isinstance(value, dict):
                add(prefix.rstrip(".") or "$")
                return
            for k, child in sch.items():
                path = f"{prefix}{k}"
                if k not in value:
                    add(path)
                    continue
                v = value.get(k)
                if isinstance(child, dict):
                    if not isinstance(v, dict):
                        add(path)
                        continue
                    walk(v, child, path + ".")
                elif isinstance(child, list):
                    if not isinstance(v, list):
                        add(path)
                        continue
                    walk(v, child, path)
                else:
                    # Leaf: key exists -> ok
                    continue
            return

        if isinstance(sch, list):
            if not isinstance(value, list):
                add(prefix or "$")
                return
            elem_schema = sch[0] if sch else None
            if elem_schema is None:
                return
            for i, item in enumerate(value):
                walk(item, elem_schema, f"{prefix}[{i}].")
            return

        # Leaf: nothing to validate at this level
        return

    walk(data, schema, "")
    return missing


def populate_required_fields(data: Any, schema: Any) -> Any:
    """Populate missing keys in parsed JSON according to a schema tree.

    Adds missing keys with default values:
      - object -> {}
      - array -> []
      - leaf -> None
    """
    if schema is None:
        return data

    if isinstance(schema, dict):
        if not isinstance(data, dict):
            return data

        for key, child_schema in schema.items():
            if key not in data or data.get(key) is None:
                if isinstance(child_schema, dict):
                    data[key] = populate_required_fields({}, child_schema)
                elif isinstance(child_schema, list):
                    # Do not create placeholder elements; keep arrays empty by default.
                    data[key] = []
                else:
                    data[key] = _default_leaf_value(child_schema, key=key)
            else:
                data[key] = populate_required_fields(data[key], child_schema)
        return data

    if isinstance(schema, list):
        if not isinstance(data, list):
            return data
        elem_schema = schema[0] if len(schema) > 0 else None
        if elem_schema is None:
            return data
        for i, item in enumerate(data):
            data[i] = populate_required_fields(item, elem_schema)
        return data

    # Leaf
    return data


def repair_json_output_to_schema(
    output_text: str,
    schema: Any,
    *,
    fallback_on_failure: bool = False,
) -> Optional[str]:
    """Repair an LLM JSON output so required schema keys exist.

    When fallback_on_failure is True, returns a deterministic best-effort JSON
    output matching the schema's top-level shape even when parsing fails.
    """
    if not output_text or not isinstance(output_text, str):
        return None
    if schema is None:
        return None

    parsed = parse_llm_json(output_text)

    if isinstance(schema, dict):
        if not isinstance(parsed, dict):
            if not fallback_on_failure:
                return None
            parsed = {}
    elif isinstance(schema, list):
        if not isinstance(parsed, list):
            if not fallback_on_failure:
                return None
            parsed = []
    else:
        return None

    repaired = populate_required_fields(parsed, schema)
    try:
        return json.dumps(repaired, ensure_ascii=False)
    except Exception:
        return None


def repair_json_output_to_required_schema(output_text: str, prompt: str) -> Optional[str]:
    """Back-compat wrapper: infer schema from prompt then repair output."""
    schema = infer_required_json_schema(prompt)
    if schema is None:
        return None
    return repair_json_output_to_schema(output_text, schema, fallback_on_failure=False)


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
