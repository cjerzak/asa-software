# state_utils.py
#
# Shared utilities for LangGraph state management.
# Provides state reducers, JSON parsing, and query parsing functions.
#
import hashlib
import json
import pathlib
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from shared.message_text_utils import message_text_from_message
except ImportError:
    _module_dir = pathlib.Path(__file__).resolve().parent
    _root_dir = _module_dir.parent
    for _candidate in (str(_root_dir), str(_module_dir)):
        if _candidate not in sys.path:
            sys.path.insert(0, _candidate)
    from shared.message_text_utils import message_text_from_message


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


def _compact_text_preview(value: Any, max_chars: int = 180) -> str:
    """Return a compact single-line preview suitable for diagnostics."""
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars] + "..."


def _exception_summary(exc: Exception, *, max_chars: int = 240) -> Tuple[str, str]:
    """Return compact exception type/message for diagnostics."""
    exc_type = type(exc).__name__
    exc_msg = str(exc or "").strip()
    if len(exc_msg) > max_chars:
        exc_msg = exc_msg[:max_chars] + "..."
    return exc_type, exc_msg


def parse_llm_json(content: str, *, return_diagnostics: bool = False) -> Any:
    """Parse JSON from LLM output with optional structured diagnostics.

    Handles common output patterns:
    - Raw JSON
    - JSON wrapped in fenced code blocks
    - JSON embedded in surrounding text

    Args:
        content: Raw LLM response text
        return_diagnostics: When True, return a diagnostics dict that includes
            parse success/failure metadata and parsed payload.

    Returns:
        By default: parsed object, or `{}` on failure (backward compatibility).
        When `return_diagnostics=True`: diagnostics dict with keys:
          - ok (bool)
          - parsed (Any)
          - error_reason (str|None)
          - exception_type (str|None)
          - exception_message (str|None)
          - strategy (str|None)
          - context (dict)
    """
    diagnostics: Dict[str, Any] = {
        "ok": False,
        "parsed": {},
        "error_reason": None,
        "exception_type": None,
        "exception_message": None,
        "strategy": None,
        "context": {
            "content_length": len(content) if isinstance(content, str) else None,
            "content_preview": _compact_text_preview(content),
            "code_fence_removed": False,
            "attempt_count": 0,
        },
    }

    if not isinstance(content, str):
        diagnostics["error_reason"] = "invalid_input_type"
        diagnostics["context"]["input_type"] = type(content).__name__
        return diagnostics if return_diagnostics else {}

    if not content.strip():
        diagnostics["error_reason"] = "empty_content"
        diagnostics["context"]["normalized_length"] = 0
        return diagnostics if return_diagnostics else {}

    normalized = content.strip()

    # Remove markdown code blocks
    if normalized.startswith("```"):
        parts = normalized.split("```")
        if len(parts) >= 2:
            normalized = parts[1]
            diagnostics["context"]["code_fence_removed"] = True
            # Remove language identifier if present
            if normalized.startswith("json"):
                normalized = normalized[4:]
            elif normalized.startswith("\n"):
                normalized = normalized[1:]
    normalized = normalized.strip()
    diagnostics["context"]["normalized_length"] = len(normalized)

    last_error_type: Optional[str] = None
    last_error_message: Optional[str] = None
    attempts = 0

    def _attempt(candidate: str, strategy: str) -> Optional[Any]:
        nonlocal last_error_type, last_error_message, attempts
        attempts += 1
        try:
            parsed = json.loads(candidate)
            diagnostics["ok"] = True
            diagnostics["parsed"] = parsed
            diagnostics["strategy"] = strategy
            diagnostics["error_reason"] = None
            diagnostics["exception_type"] = None
            diagnostics["exception_message"] = None
            return parsed
        except Exception as exc:
            etype, emsg = _exception_summary(exc)
            last_error_type = etype
            last_error_message = emsg
            return None

    # Try direct parse first
    parsed_direct = _attempt(normalized, "direct_json")
    if parsed_direct is not None:
        diagnostics["context"]["attempt_count"] = attempts
        return diagnostics if return_diagnostics else parsed_direct

    # Try scanning for a JSON object or array in text
    extracted = _extract_json_substring(normalized)
    if extracted:
        diagnostics["context"]["extracted_json_length"] = len(extracted)
        parsed_extracted = _attempt(extracted, "scan_substring")
        if parsed_extracted is not None:
            diagnostics["context"]["attempt_count"] = attempts
            return diagnostics if return_diagnostics else parsed_extracted

    # Try to find JSON object in text (legacy patterns)
    json_patterns = [
        ("fenced_json", r"```json\s*(.*?)\s*```"),
        ("fenced_generic", r"```\s*(.*?)\s*```"),
        ("legacy_brace_extract", r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}"),
    ]

    for strategy, pattern in json_patterns:
        matches = re.findall(pattern, normalized, re.DOTALL)
        for match in matches:
            parsed_match = _attempt(match, strategy)
            if parsed_match is not None:
                diagnostics["context"]["attempt_count"] = attempts
                return diagnostics if return_diagnostics else parsed_match

    diagnostics["context"]["attempt_count"] = attempts
    diagnostics["error_reason"] = (
        "json_decode_failed" if attempts > 0 else "no_json_candidate_found"
    )
    diagnostics["exception_type"] = last_error_type
    diagnostics["exception_message"] = last_error_message
    diagnostics["strategy"] = None
    diagnostics["parsed"] = {}
    return diagnostics if return_diagnostics else {}


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
        r"(this exact schema|exact schema|json schema|schema\s*:)",
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
    return message_text_from_message(msg, list_mode="stringify")


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
    - If "Unknown" is an explicit enum option: use "Unknown"
    - If null is allowed (and no "Unknown" option): use None
    - Prefer "partial" for status enum fields when available
    - If it's a string and null is disallowed: use "" (empty string)
    - Otherwise: use None
    """
    if not isinstance(descriptor, str):
        return None

    text = descriptor.strip()
    if not text:
        return None

    lower = text.lower()

    # Prefer "partial" for common status enums when available.
    if key == "status" and "partial" in lower:
        return "partial"

    # Handle pipe-separated descriptors (enums or type unions).
    if "|" in text:
        parts = [p.strip() for p in text.split("|") if p.strip()]
        type_words = {"string", "integer", "number", "boolean", "null", "object", "array"}

        # Check for "Unknown" sentinel in ANY pipe-separated descriptor.
        for p in parts:
            clean = p.strip().strip("\"'")
            if clean.lower() == "unknown":
                return clean or "Unknown"

        # Pure enum (no type words): check for "partial", then first value.
        if parts and not any(p.lower() in type_words for p in parts):
            for p in parts:
                if p.lower() == "partial":
                    return p
            return parts[0]

        # Mixed type descriptor (e.g., "string|null"): check for null.
        for p in parts:
            clean = p.strip().strip("\"'")
            if clean.lower() == "null":
                return None

    # Non-pipe or fall-through: check for word-boundary "unknown".
    if re.search(r"\bunknown\b", text, flags=re.IGNORECASE):
        return "Unknown"

    # Check for word-boundary "null".
    if re.search(r"\bnull\b", text, flags=re.IGNORECASE):
        return None

    if "array" in lower:
        return []

    if "object" in lower:
        return {}

    if "string" in lower:
        return ""

    return None


def _descriptor_prefers_unknown(descriptor: str) -> bool:
    """Return True if the descriptor explicitly lists 'Unknown' as an option."""
    if "|" in descriptor:
        for part in descriptor.split("|"):
            clean = part.strip().strip("\"'")
            if clean.lower() == "unknown":
                return True
    elif re.search(r"\bunknown\b", descriptor, flags=re.IGNORECASE):
        return True
    return False


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
                current = data[key]
                if (
                    isinstance(current, str)
                    and current.strip() == ""
                    and isinstance(child_schema, str)
                    and _descriptor_prefers_unknown(child_schema)
                ):
                    data[key] = _default_leaf_value(child_schema, key=key)
                else:
                    data[key] = populate_required_fields(current, child_schema)
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
    return_diagnostics: bool = False,
) -> Any:
    """Repair LLM JSON output so required schema keys exist.

    When fallback_on_failure is True, returns a deterministic best-effort JSON
    output matching the schema's top-level shape even when parsing fails.

    Args:
        output_text: Raw JSON-ish output string
        schema: Expected schema tree
        fallback_on_failure: Coerce top-level shape when parsing/type checks fail
        return_diagnostics: When True, return structured diagnostics dict

    Returns:
        By default: repaired JSON string or None.
        When `return_diagnostics=True`: diagnostics dict containing:
          - ok (bool)
          - repaired_text (str|None)
          - error_reason (str|None)
          - exception_type (str|None)
          - exception_message (str|None)
          - parse_diagnostics (dict)
          - context (dict)
    """
    diagnostics: Dict[str, Any] = {
        "ok": False,
        "repaired_text": None,
        "error_reason": None,
        "exception_type": None,
        "exception_message": None,
        "parse_diagnostics": None,
        "context": {
            "output_length": len(output_text) if isinstance(output_text, str) else None,
            "output_preview": _compact_text_preview(output_text),
            "schema_type": type(schema).__name__ if schema is not None else "none",
            "fallback_on_failure": bool(fallback_on_failure),
            "coerced_input_shape": False,
        },
    }

    if not isinstance(output_text, str) or not output_text.strip():
        diagnostics["error_reason"] = "invalid_output_text"
        diagnostics["context"]["input_type"] = type(output_text).__name__
        return diagnostics if return_diagnostics else None
    if schema is None:
        diagnostics["error_reason"] = "schema_missing"
        return diagnostics if return_diagnostics else None

    parse_diag = parse_llm_json(output_text, return_diagnostics=True)
    diagnostics["parse_diagnostics"] = {
        "ok": bool(parse_diag.get("ok", False)) if isinstance(parse_diag, dict) else False,
        "error_reason": parse_diag.get("error_reason") if isinstance(parse_diag, dict) else "parse_diagnostics_missing",
        "exception_type": parse_diag.get("exception_type") if isinstance(parse_diag, dict) else None,
        "exception_message": parse_diag.get("exception_message") if isinstance(parse_diag, dict) else None,
        "context": parse_diag.get("context", {}) if isinstance(parse_diag, dict) else {},
    }
    parsed = parse_diag.get("parsed", {}) if isinstance(parse_diag, dict) else {}

    expected_shape = None
    if isinstance(schema, dict):
        expected_shape = "dict"
        if not isinstance(parsed, dict):
            diagnostics["context"]["parsed_type"] = type(parsed).__name__
            if not fallback_on_failure:
                diagnostics["error_reason"] = "type_mismatch"
                return diagnostics if return_diagnostics else None
            diagnostics["context"]["coerced_input_shape"] = True
            parsed = {}
    elif isinstance(schema, list):
        expected_shape = "list"
        if not isinstance(parsed, list):
            diagnostics["context"]["parsed_type"] = type(parsed).__name__
            if not fallback_on_failure:
                diagnostics["error_reason"] = "type_mismatch"
                return diagnostics if return_diagnostics else None
            diagnostics["context"]["coerced_input_shape"] = True
            parsed = []
    else:
        diagnostics["error_reason"] = "unsupported_schema_type"
        diagnostics["context"]["schema_type"] = type(schema).__name__
        return diagnostics if return_diagnostics else None
    diagnostics["context"]["expected_shape"] = expected_shape
    diagnostics["context"]["parsed_type"] = type(parsed).__name__

    try:
        repaired = populate_required_fields(parsed, schema)
    except Exception as exc:
        exc_type, exc_msg = _exception_summary(exc)
        diagnostics["error_reason"] = "repair_exception"
        diagnostics["exception_type"] = exc_type
        diagnostics["exception_message"] = exc_msg
        return diagnostics if return_diagnostics else None

    try:
        repaired_text = json.dumps(repaired, ensure_ascii=False)
    except Exception as exc:
        exc_type, exc_msg = _exception_summary(exc)
        diagnostics["error_reason"] = "json_serialize_failed"
        diagnostics["exception_type"] = exc_type
        diagnostics["exception_message"] = exc_msg
        return diagnostics if return_diagnostics else None

    diagnostics["ok"] = True
    diagnostics["repaired_text"] = repaired_text
    diagnostics["error_reason"] = None
    diagnostics["exception_type"] = None
    diagnostics["exception_message"] = None
    return diagnostics if return_diagnostics else repaired_text


def repair_json_output_to_required_schema(output_text: str, prompt: str) -> Optional[str]:
    """Back-compat wrapper: infer schema from prompt then repair output."""
    schema = infer_required_json_schema(prompt)
    if schema is None:
        return None
    return repair_json_output_to_schema(output_text, schema, fallback_on_failure=False)


def _coerce_token_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return max(0, int(value))
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return max(0, int(float(text)))
        except Exception:
            return None
    return None


def _usage_from_mapping(payload: Any) -> Optional[Dict[str, int]]:
    if not isinstance(payload, dict):
        return None

    input_keys = (
        "input_tokens",
        "prompt_tokens",
        "inputTokenCount",
        "promptTokenCount",
        "input_token_count",
        "prompt_token_count",
        "prompt_eval_count",
    )
    output_keys = (
        "output_tokens",
        "completion_tokens",
        "outputTokenCount",
        "candidatesTokenCount",
        "output_token_count",
        "completion_token_count",
        "eval_count",
    )
    total_keys = (
        "total_tokens",
        "totalTokenCount",
        "total_token_count",
        "totalTokens",
    )

    def _first(keys: Tuple[str, ...]) -> Tuple[Optional[int], bool]:
        for key in keys:
            if key in payload:
                return _coerce_token_int(payload.get(key)), True
        return None, False

    inp, has_inp = _first(input_keys)
    out, has_out = _first(output_keys)
    total, has_total = _first(total_keys)

    if not (has_inp or has_out or has_total):
        return None

    if total is None and inp is not None and out is not None:
        total = inp + out
    if inp is None and total is not None and out is not None:
        inp = max(0, total - out)
    if out is None and total is not None and inp is not None:
        out = max(0, total - inp)

    inp = 0 if inp is None else inp
    out = 0 if out is None else out
    total = (inp + out) if total is None else total

    return {"input_tokens": inp, "output_tokens": out, "total_tokens": total}


def _token_usage_dict_from_message(message: Any) -> Dict[str, int]:
    """Best-effort extraction of token usage breakdown from LangChain message objects.

    Returns:
        Dict with keys "input_tokens", "output_tokens", "total_tokens" (all ints, default 0).
    """
    zero = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    if message is None:
        return zero

    candidates: List[dict] = []
    seen_ids = set()

    def _push(candidate: Any) -> None:
        if not isinstance(candidate, dict):
            return
        cid = id(candidate)
        if cid in seen_ids:
            return
        seen_ids.add(cid)
        candidates.append(candidate)

    if isinstance(message, dict):
        _push(message)

    _push(getattr(message, "usage_metadata", None))
    _push(getattr(message, "usage", None))
    _push(getattr(message, "token_usage", None))

    response_metadata = getattr(message, "response_metadata", None)
    _push(response_metadata)
    if isinstance(response_metadata, dict):
        _push(response_metadata.get("usage_metadata"))
        _push(response_metadata.get("token_usage"))
        _push(response_metadata.get("usage"))

    additional_kwargs = getattr(message, "additional_kwargs", None)
    _push(additional_kwargs)
    if isinstance(additional_kwargs, dict):
        _push(additional_kwargs.get("usage_metadata"))
        _push(additional_kwargs.get("token_usage"))
        _push(additional_kwargs.get("usage"))
        nested_response_meta = additional_kwargs.get("response_metadata")
        _push(nested_response_meta)
        if isinstance(nested_response_meta, dict):
            _push(nested_response_meta.get("usage_metadata"))
            _push(nested_response_meta.get("token_usage"))
            _push(nested_response_meta.get("usage"))

    for candidate in candidates:
        parsed = _usage_from_mapping(candidate)
        if parsed is not None:
            return parsed

    return zero


def _token_usage_from_message(message: Any) -> int:
    """Best-effort extraction of total token usage from LangChain message objects."""
    return _token_usage_dict_from_message(message)["total_tokens"]


def elapsed_minutes_from_perf_counter(
    started_at: Optional[float],
    ended_at: Optional[float] = None
) -> float:
    """Compute non-negative elapsed wall time in minutes from perf_counter values."""
    if started_at is None:
        return 0.0
    if ended_at is None:
        ended_at = time.perf_counter()
    try:
        elapsed_seconds = float(ended_at) - float(started_at)
    except Exception:
        return 0.0
    if elapsed_seconds < 0:
        elapsed_seconds = 0.0
    return elapsed_seconds / 60.0


def _iso_utc_from_epoch_seconds(epoch_seconds: Any) -> str:
    """Best-effort conversion of epoch seconds to ISO-8601 UTC text."""
    try:
        ts = float(epoch_seconds)
    except Exception:
        ts = time.time()
    if ts < 0:
        ts = 0.0
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def build_node_trace_entry(
    node: str,
    usage: Optional[Dict[str, int]] = None,
    *,
    started_at: Optional[float] = None,
    ended_at: Optional[float] = None,
    elapsed_minutes: Optional[float] = None,
    status: Optional[str] = "ok",
    error_type: Optional[str] = None,
    tool_name: Optional[str] = None,
    started_at_utc: Optional[str] = None,
    ended_at_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a normalized per-node trace entry with token and timing data."""
    parsed_usage = _usage_from_mapping(usage if isinstance(usage, dict) else {}) or {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }

    elapsed_val = elapsed_minutes
    if elapsed_val is None:
        elapsed_val = elapsed_minutes_from_perf_counter(started_at, ended_at)
    try:
        elapsed_val = float(elapsed_val)
    except Exception:
        elapsed_val = 0.0
    if elapsed_val < 0:
        elapsed_val = 0.0

    node_name = str(node).strip() if node is not None else ""
    if not node_name:
        node_name = "unknown"

    status_token = str(status or "ok").strip().lower()
    if status_token not in {"ok", "error", "skipped"}:
        status_token = "ok"

    error_type_token = str(error_type or "").strip() or None
    tool_name_token = str(tool_name or "").strip() or None

    ended_at_iso = str(ended_at_utc or "").strip() or None
    if not ended_at_iso:
        ended_at_iso = _iso_utc_from_epoch_seconds(time.time())

    started_at_iso = str(started_at_utc or "").strip() or None
    if not started_at_iso:
        elapsed_seconds = max(0.0, float(elapsed_val) * 60.0)
        started_epoch = max(0.0, float(time.time()) - elapsed_seconds)
        started_at_iso = _iso_utc_from_epoch_seconds(started_epoch)

    return {
        "node": node_name,
        "input_tokens": int(parsed_usage.get("input_tokens", 0) or 0),
        "output_tokens": int(parsed_usage.get("output_tokens", 0) or 0),
        "total_tokens": int(parsed_usage.get("total_tokens", 0) or 0),
        "elapsed_minutes": elapsed_val,
        "started_at_utc": started_at_iso,
        "ended_at_utc": ended_at_iso,
        "status": status_token,
        "error_type": error_type_token,
        "tool_name": tool_name_token,
    }


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
