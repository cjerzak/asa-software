"""Bridge helpers for schema extraction via langextract.

This module keeps langextract optional at runtime by importing it lazily.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse


_UNKNOWN_MARKERS = {
    "",
    "unknown",
    "n/a",
    "na",
    "none",
    "null",
    "not available",
    "not found",
    "unspecified",
    "tbd",
}


@dataclass
class LangextractRoute:
    """Resolved langextract model/provider route."""

    backend: str
    provider: Optional[str]
    model_id: str


def _normalize_match_text(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _normalize_url(url: Any) -> Optional[str]:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return raw


def _is_unknown_marker(value: Any) -> bool:
    token = str(value or "").strip().lower()
    return token in _UNKNOWN_MARKERS


def _selector_model_name(selector_model: Any) -> str:
    for attr in ("model_name", "model", "model_id", "name"):
        try:
            value = getattr(selector_model, attr, None)
        except Exception:
            value = None
        if value:
            text = str(value).strip()
            if text:
                return text
    return ""


def _selector_model_backend(selector_model: Any) -> str:
    try:
        module_name = str(getattr(selector_model.__class__, "__module__", "") or "").lower()
    except Exception:
        module_name = ""
    class_name = str(getattr(selector_model.__class__, "__name__", "") or "").lower()
    name = _selector_model_name(selector_model).lower()
    haystack = " ".join((module_name, class_name, name))

    if "google" in haystack or "gemini" in haystack:
        return "gemini"
    if "openai" in haystack or "gpt" in haystack:
        return "openai"
    if "anthropic" in haystack or "claude" in haystack:
        return "anthropic"
    if "xai" in haystack or "grok" in haystack:
        return "xai"
    if "bedrock" in haystack or "aws" in haystack:
        return "bedrock"
    if "openrouter" in haystack:
        return "openrouter"
    return "unknown"


def _normalize_model_id_for_backend(*, backend: str, model_name: Any) -> str:
    model_id = str(model_name or "").strip()
    backend_token = str(backend or "").strip().lower()
    if backend_token == "gemini" and model_id.lower().startswith("models/"):
        # langextract expects bare Gemini model IDs (without the SDK prefix).
        model_id = model_id.split("/", 1)[1].strip()
    return model_id


def _resolve_route(
    selector_model: Any,
    *,
    backend_hint: Optional[str] = None,
    model_id_override: Optional[str] = None,
) -> Tuple[Optional[LangextractRoute], Optional[str]]:
    backend = str(backend_hint or "").strip().lower()
    if not backend:
        backend = _selector_model_backend(selector_model)

    raw_model_name = model_id_override if model_id_override is not None else _selector_model_name(selector_model)
    model_name = _normalize_model_id_for_backend(backend=backend, model_name=raw_model_name)

    if backend == "gemini":
        return LangextractRoute(
            backend="gemini",
            provider="gemini",
            model_id=model_name or "gemini-2.5-flash",
        ), None

    if backend == "openai":
        return LangextractRoute(
            backend="openai",
            provider="openai",
            model_id=model_name or "gpt-4.1-mini",
        ), None

    return None, f"unsupported_backend:{backend}"


def _prompt_validation_level(level: str, pv_module: Any) -> Any:
    token = str(level or "warning").strip().lower()
    level_enum = getattr(pv_module, "PromptValidationLevel", None)
    if level_enum is None:
        return None
    if token == "off":
        return getattr(level_enum, "OFF", None)
    if token == "error":
        return getattr(level_enum, "ERROR", None)
    return getattr(level_enum, "WARNING", None)


def _example_value_for_key(key: str) -> str:
    key_l = key.lower()
    if key_l.endswith("_source"):
        return "https://example.com/source"
    if any(token in key_l for token in ("date", "year")):
        return "2024-01-15"
    if any(token in key_l for token in ("email",)):
        return "person@example.com"
    if any(token in key_l for token in ("phone", "tel")):
        return "+1-555-0100"
    if any(token in key_l for token in ("url", "website")):
        return "https://example.com/profile"
    if any(token in key_l for token in ("name", "title", "role", "occupation", "city", "state", "country", "institution")):
        return "Example Value"
    return "Example Value"


def build_langextract_examples_from_schema(
    *,
    allowed_keys: Iterable[str],
) -> List[Any]:
    """Create generic, schema-driven few-shot examples for langextract."""
    keys = [str(k).strip() for k in allowed_keys if str(k).strip()]
    keys = keys[:12]
    if not keys:
        return []

    text_lines: List[str] = []
    extraction_specs: List[Tuple[str, str]] = []
    for key in keys:
        value = _example_value_for_key(key)
        text_lines.append(f"{key}: {value}")
        extraction_specs.append((key, value))

    example_text = ". ".join(text_lines) + "."

    # Created lazily to keep runtime optional when langextract is not installed.
    import langextract as lx  # type: ignore

    extractions = [
        lx.data.Extraction(
            extraction_class=field,
            extraction_text=value,
            attributes={"field": field},
        )
        for field, value in extraction_specs
    ]
    return [
        lx.data.ExampleData(
            text=example_text,
            extractions=extractions,
        )
    ]


def _allowed_key_map(allowed_keys: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in allowed_keys:
        key_s = str(key or "").strip()
        if not key_s:
            continue
        out[_normalize_match_text(key_s)] = key_s
    return out


def normalize_langextract_output_to_allowed_keys(
    *,
    extractions: Iterable[Any],
    allowed_keys: Iterable[str],
    page_url: str,
) -> Dict[str, Any]:
    """Normalize langextract extraction output to ASA payload shape."""
    allowed_key_map = _allowed_key_map(allowed_keys)
    payload: Dict[str, Any] = {}

    for extraction in list(extractions or []):
        try:
            raw_class = str(getattr(extraction, "extraction_class", "") or "").strip()
        except Exception:
            raw_class = ""
        try:
            raw_text = str(getattr(extraction, "extraction_text", "") or "").strip()
        except Exception:
            raw_text = ""

        attrs = getattr(extraction, "attributes", None)
        field_hint = ""
        if isinstance(attrs, dict):
            for attr_key in ("field", "schema_key", "key"):
                raw_hint = attrs.get(attr_key)
                if raw_hint:
                    field_hint = str(raw_hint).strip()
                    break

        candidate_field = raw_class or field_hint
        canonical = allowed_key_map.get(_normalize_match_text(candidate_field))
        if not canonical:
            continue
        if not raw_text or _is_unknown_marker(raw_text):
            continue

        if canonical.endswith("_source"):
            normalized = _normalize_url(raw_text) or _normalize_url(page_url)
            if normalized:
                payload[canonical] = normalized
            continue

        if canonical not in payload:
            payload[canonical] = raw_text

    # Deterministically fill missing sibling *_source fields.
    normalized_page_url = _normalize_url(page_url)
    if normalized_page_url:
        for key in list(payload.keys()):
            if key.endswith("_source"):
                continue
            sibling = f"{key}_source"
            canonical_sibling = allowed_key_map.get(_normalize_match_text(sibling))
            if canonical_sibling and canonical_sibling not in payload:
                payload[canonical_sibling] = normalized_page_url

    return payload


def extract_schema_from_openwebpage_text(
    *,
    page_url: str,
    page_text: str,
    allowed_keys: Iterable[str],
    schema_keys: Iterable[str],
    selector_model: Any,
    backend_hint: Optional[str] = None,
    model_id_override: Optional[str] = None,
    extraction_passes: int = 1,
    max_char_buffer: int = 1000,
    prompt_validation_level: str = "warning",
    max_chars: int = 6000,
) -> Dict[str, Any]:
    """Run langextract and return normalized ASA payload result.

    Returns:
      dict with keys: ok, payload, provider, model_id, error
    """
    route, route_error = _resolve_route(
        selector_model,
        backend_hint=backend_hint,
        model_id_override=model_id_override,
    )
    if route is None:
        return {
            "ok": False,
            "payload": {},
            "provider": None,
            "model_id": None,
            "error": route_error or "route_unresolved",
        }

    clipped_text = str(page_text or "")[: max(512, int(max_chars or 6000))]
    if not clipped_text.strip():
        return {
            "ok": False,
            "payload": {},
            "provider": route.provider,
            "model_id": route.model_id,
            "error": "empty_page_text",
        }

    try:
        import langextract as lx  # type: ignore
        from langextract import prompt_validation as pv  # type: ignore
    except Exception as exc:
        return {
            "ok": False,
            "payload": {},
            "provider": route.provider,
            "model_id": route.model_id,
            "error": f"langextract_unavailable:{exc}",
        }

    try:
        examples = build_langextract_examples_from_schema(allowed_keys=allowed_keys)
        if not examples:
            return {
                "ok": False,
                "payload": {},
                "provider": route.provider,
                "model_id": route.model_id,
                "error": "no_examples",
            }

        keys_preview = ", ".join(sorted(str(k) for k in list(schema_keys or [])[:24]))
        allowed_preview = ", ".join(sorted(str(k) for k in list(allowed_keys or [])[:24]))

        prompt_description = (
            "Extract structured field values from webpage text. "
            "For each extraction, set extraction_class EXACTLY to one field key from the allowed list. "
            "Use exact evidence text for extraction_text; do not guess or emit unknown placeholders. "
            "If no explicit value exists for a field, omit it. "
            f"Allowed field keys: {allowed_preview}. "
            f"Schema keys: {keys_preview}."
        )

        pv_level = _prompt_validation_level(prompt_validation_level, pv)
        if pv_level is None:
            pv_level = pv.PromptValidationLevel.WARNING

        extraction_doc = (
            f"PAGE_URL: {page_url}\n"
            f"PAGE_TEXT:\n{clipped_text}"
        )

        result = lx.extract(
            text_or_documents=extraction_doc,
            prompt_description=prompt_description,
            examples=examples,
            model_id=route.model_id,
            extraction_passes=max(1, int(extraction_passes or 1)),
            max_char_buffer=max(200, int(max_char_buffer or 1000)),
            prompt_validation_level=pv_level,
            fetch_urls=False,
            show_progress=False,
        )

        document = result[0] if isinstance(result, list) else result
        payload = normalize_langextract_output_to_allowed_keys(
            extractions=getattr(document, "extractions", []) or [],
            allowed_keys=allowed_keys,
            page_url=page_url,
        )

        return {
            "ok": bool(payload),
            "payload": payload,
            "provider": route.provider,
            "model_id": route.model_id,
            "error": None if payload else "no_payload",
        }

    except Exception as exc:
        return {
            "ok": False,
            "payload": {},
            "provider": route.provider,
            "model_id": route.model_id,
            "error": f"langextract_error:{exc}",
        }
