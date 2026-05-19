"""Minimal stdio MCP server exposing ASA web search and webpage fetch tools."""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from asa_backend.search import (
    PatchedDuckDuckGoSearchAPIWrapper,
    PatchedDuckDuckGoSearchRun,
    SearchConfig,
    configure_tor,
    configure_tor_registry,
)
from tools.webpage_reader_tool import configure_webpage_reader, create_webpage_reader_tool


def _mcp_log_file() -> str:
    return str(os.getenv("ASA_FREE_CODE_MCP_LOG_FILE", "") or "").strip()


def _mcp_log(*parts: Any) -> None:
    path = _mcp_log_file()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(" ".join(str(part) for part in parts))
            handle.write("\n")
    except Exception:
        return


def _env_json(name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return dict(default or {})
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return dict(default or {})


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "t", "yes", "y", "on"}


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        out = int(value)
        return out if out > 0 else default
    except Exception:
        return default


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        out = float(value)
        return out if out > 0 else default
    except Exception:
        return default


def _loop_guard_config() -> Dict[str, Any]:
    cfg = _env_json("ASA_FREE_CODE_LOOP_GUARD_JSON")
    unknown_after = _as_int(cfg.get("unknown_after_searches"), None)
    return {
        "recursion_limit": _as_int(cfg.get("recursion_limit"), None),
        "search_budget_limit": _as_int(cfg.get("search_budget_limit"), None),
        "unknown_after_searches": unknown_after,
        "repeated_timeout_limit": _as_int(cfg.get("repeated_timeout_limit"), 2) or 2,
        "repeated_fetch_timeout_limit": _as_int(cfg.get("repeated_fetch_timeout_limit"), 1) or 1,
        "total_timeout_limit": _as_int(
            cfg.get("total_timeout_limit"),
            max(3, unknown_after) if unknown_after is not None else 6,
        )
        or 6,
        "tool_deadline_seconds": _as_float(cfg.get("tool_deadline_seconds"), 25.0) or 25.0,
    }


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


LOOP_GUARD = _loop_guard_config()
TOOL_STATE: Dict[str, Any] = {
    "requests": 0,
    "network_calls": 0,
    "timeouts": 0,
    "errors": 0,
    "timeout_by_signature": {},
    "error_by_signature": {},
    "terminal_failures": {},
    "cache": {},
}


class _ToolDeadlineExpired(TimeoutError):
    pass


@contextmanager
def _tool_deadline(seconds: Optional[float]):
    if (
        seconds is None
        or seconds <= 0
        or not hasattr(signal, "SIGALRM")
        or threading.current_thread() is not threading.main_thread()
    ):
        yield
        return

    def _raise_timeout(signum: int, frame: Any) -> None:
        raise _ToolDeadlineExpired(f"ASA tool deadline exceeded after {seconds:.1f}s")

    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_timeout)
    old_timer = signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, old_timer[0], old_timer[1])
        signal.signal(signal.SIGALRM, old_handler)


def _progress_token(value: Any, default: str = "na", max_chars: int = 72) -> str:
    text = str(value or default)
    text = "_".join(text.split())
    text = "".join(ch for ch in text if ch.isalnum() or ch in "_.:/-")
    if not text:
        text = default
    return text[:max_chars]


def _write_progress(phase: str, tool: str = "na", error: str = "none") -> None:
    path = str(os.getenv("ASA_PROGRESS_STATE_FILE", "") or "").strip()
    if not path:
        return
    limit = LOOP_GUARD.get("search_budget_limit")
    used = int(TOOL_STATE.get("network_calls", 0) or 0)
    remaining = max(0, int(limit) - used) if isinstance(limit, int) else "na"
    parts = {
        "phase": phase,
        "node": "mcp_search",
        "tool_used": used,
        "tool_limit": limit if isinstance(limit, int) else "na",
        "tool_rem": remaining,
        "pending_tool_calls": "1" if phase == "tool_call" else "0",
        "pending_tool": _progress_token(tool),
        "error": _progress_token(error, default="none"),
        "ts": int(time.time()),
    }
    line = " ".join(f"{key}={value}" for key, value in parts.items())
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        return


def _canonical_tool_name(name: str) -> str:
    normalized = str(name or "").strip()
    lowered = normalized.lower()
    if lowered.endswith("web_search") or lowered in {"search", "websearch"}:
        return "web_search"
    if lowered.endswith("web_fetch") or lowered in {"fetch", "webfetch", "openwebpage"}:
        return "web_fetch"
    return normalized


def _normalize_text(value: Any, *, lowercase: bool = False) -> str:
    text = " ".join(str(value or "").strip().split())
    return text.lower() if lowercase else text


def _tool_signature(name: str, arguments: Dict[str, Any]) -> str:
    canonical = _canonical_tool_name(name)
    if canonical == "web_search":
        payload = {"query": _normalize_text(arguments.get("query"), lowercase=True)}
    elif canonical == "web_fetch":
        payload = {
            "url": _normalize_text(arguments.get("url")),
            "query": _normalize_text(arguments.get("query"), lowercase=True),
        }
    else:
        payload = arguments
    return f"{canonical}:{json.dumps(payload, ensure_ascii=False, sort_keys=True)}"


def _guard_payload(
    *,
    error_type: str,
    message: str,
    name: str,
    arguments: Dict[str, Any],
    signature: str,
    retryable: bool = False,
) -> str:
    payload = {
        "error_type": error_type,
        "message": message,
        "tool": _canonical_tool_name(name),
        "signature": signature,
        "retryable": bool(retryable),
        "instruction": (
            "Do not retry this exact tool input when retryable is false. "
            "Use existing evidence, try a materially different source if budget remains, "
            "or return Unknown for unresolved fields."
        ),
        "budget_state": {
            "network_calls": TOOL_STATE.get("network_calls", 0),
            "search_budget_limit": LOOP_GUARD.get("search_budget_limit"),
            "timeouts": TOOL_STATE.get("timeouts", 0),
            "total_timeout_limit": LOOP_GUARD.get("total_timeout_limit"),
        },
        "input": arguments,
    }
    return "ASA_TOOL_ERROR " + json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _error_result(
    *,
    error_type: str,
    message: str,
    name: str,
    arguments: Dict[str, Any],
    signature: str,
    retryable: bool = False,
) -> Dict[str, Any]:
    _write_progress("tool_error", tool=name, error=error_type)
    return {
        "content": [
            {
                "type": "text",
                "text": _guard_payload(
                    error_type=error_type,
                    message=message,
                    name=name,
                    arguments=arguments,
                    signature=signature,
                    retryable=retryable,
                ),
            }
        ],
        "isError": True,
    }


def _same_input_timeout_limit(name: str) -> int:
    if _canonical_tool_name(name) == "web_fetch":
        return int(LOOP_GUARD.get("repeated_fetch_timeout_limit", 1) or 1)
    return int(LOOP_GUARD.get("repeated_timeout_limit", 2) or 2)


def _terminal_failure(signature: str) -> Optional[str]:
    failure = TOOL_STATE["terminal_failures"].get(signature)
    return str(failure) if failure else None


def _preflight_guard(name: str, arguments: Dict[str, Any], signature: str) -> Optional[Dict[str, Any]]:
    terminal_failure = _terminal_failure(signature)
    if terminal_failure:
        return _error_result(
            error_type=terminal_failure,
            message="This tool input already reached a terminal loop guard condition.",
            name=name,
            arguments=arguments,
            signature=signature,
            retryable=False,
        )

    if signature in TOOL_STATE["cache"]:
        _write_progress("tool_cache_hit", tool=name)
        return {
            "content": [
                {
                    "type": "text",
                    "text": "ASA_TOOL_CACHE_HIT\n" + str(TOOL_STATE["cache"][signature]),
                }
            ],
            "isError": False,
        }

    limit = LOOP_GUARD.get("search_budget_limit")
    if isinstance(limit, int) and int(TOOL_STATE.get("network_calls", 0) or 0) >= limit:
        return _error_result(
            error_type="tool_budget_exhausted",
            message="The configured web search/fetch budget has been exhausted.",
            name=name,
            arguments=arguments,
            signature=signature,
            retryable=False,
        )

    total_timeout_limit = int(LOOP_GUARD.get("total_timeout_limit", 6) or 6)
    if int(TOOL_STATE.get("timeouts", 0) or 0) >= total_timeout_limit:
        return _error_result(
            error_type="timeout_budget_exhausted",
            message="The configured transport timeout budget has been exhausted.",
            name=name,
            arguments=arguments,
            signature=signature,
            retryable=False,
        )

    repeated_timeouts = int(TOOL_STATE["timeout_by_signature"].get(signature, 0) or 0)
    if repeated_timeouts >= _same_input_timeout_limit(name):
        return _error_result(
            error_type="same_input_timeout_exhausted",
            message="This exact tool input has timed out too many times.",
            name=name,
            arguments=arguments,
            signature=signature,
            retryable=False,
        )

    return None


def _record_tool_error(name: str, signature: str, error_type: str) -> None:
    TOOL_STATE["errors"] = int(TOOL_STATE.get("errors", 0) or 0) + 1
    TOOL_STATE["error_by_signature"][signature] = int(
        TOOL_STATE["error_by_signature"].get(signature, 0) or 0
    ) + 1
    if error_type == "tool_timeout":
        TOOL_STATE["timeouts"] = int(TOOL_STATE.get("timeouts", 0) or 0) + 1
        TOOL_STATE["timeout_by_signature"][signature] = int(
            TOOL_STATE["timeout_by_signature"].get(signature, 0) or 0
        ) + 1
        if TOOL_STATE["timeout_by_signature"][signature] >= _same_input_timeout_limit(name):
            TOOL_STATE["terminal_failures"][signature] = "same_input_timeout_exhausted"


def _build_tools() -> Dict[str, Any]:
    search_options = _env_json("ASA_FREE_CODE_SEARCH_OPTIONS_JSON")
    tor_options = _env_json("ASA_FREE_CODE_TOR_OPTIONS_JSON")
    webpage_options = _env_json("ASA_FREE_CODE_WEBPAGE_OPTIONS_JSON")
    proxy = os.getenv("ASA_FREE_CODE_PROXY", "").strip() or None
    use_browser = _env_bool("ASA_FREE_CODE_USE_BROWSER", default=True)
    allow_read_webpages = _env_bool("ASA_FREE_CODE_ALLOW_READ_WEBPAGES", default=False)

    if tor_options:
        configure_tor_registry(
            registry_path=tor_options.get("registry_path"),
            enable=bool(tor_options.get("dirty_tor_exists", True)),
            bad_ttl=int(tor_options.get("bad_ttl", 1800)),
            good_ttl=int(tor_options.get("good_ttl", 300)),
            overuse_threshold=int(tor_options.get("overuse_threshold", 20)),
            overuse_decay=int(tor_options.get("overuse_decay", 3600)),
            max_rotation_attempts=int(tor_options.get("max_rotation_attempts", 4)),
            ip_cache_ttl=int(tor_options.get("ip_cache_ttl", 60)),
        )
        configure_tor(
            control_port=int(os.getenv("TOR_CONTROL_PORT", "9051") or 9051),
            control_password=(os.getenv("TOR_CONTROL_PASSWORD", "") or None),
            min_rotation_interval=float(os.getenv("ASA_TOR_MIN_ROTATION_INTERVAL", "5") or 5),
        )

    search_cfg = SearchConfig(
        max_results=int(search_options.get("max_results", 10) or 10),
        timeout=float(search_options.get("timeout", 30) or 30),
        max_retries=int(search_options.get("max_retries", 3) or 3),
        retry_delay=float(search_options.get("retry_delay", 2) or 2),
        backoff_multiplier=float(search_options.get("backoff_multiplier", 1.5) or 1.5),
        inter_search_delay=float(search_options.get("inter_search_delay", 1.5) or 1.5),
        humanize_timing=bool(search_options.get("humanize_timing", True)),
        jitter_factor=float(search_options.get("jitter_factor", 0.5) or 0.5),
        allow_direct_fallback=bool(search_options.get("allow_direct_fallback", False)),
        selenium_browser_preference=str(
            search_options.get("selenium_browser_preference", "chrome")
        ),
    )

    search_tool = PatchedDuckDuckGoSearchRun(
        name="Search",
        description="DuckDuckGo web search",
        api_wrapper=PatchedDuckDuckGoSearchAPIWrapper(
            proxy=proxy,
            use_browser=use_browser,
            max_results=int(search_options.get("max_results", 10) or 10),
            safesearch="moderate",
            time="none",
            search_config=search_cfg,
        ),
        doc_content_chars_max=int(search_options.get("search_doc_content_chars_max", 500) or 500),
        timeout=90,
        verbose=False,
        max_concurrency=1,
    )

    configure_webpage_reader(
        allow_read_webpages=allow_read_webpages,
        timeout=webpage_options.get("timeout"),
        max_bytes=webpage_options.get("max_bytes"),
        max_chars=webpage_options.get("max_chars"),
        max_chunks=webpage_options.get("max_chunks"),
        chunk_chars=webpage_options.get("chunk_chars"),
        pdf_enabled=webpage_options.get("pdf_enabled"),
        pdf_timeout=webpage_options.get("pdf_timeout"),
        pdf_max_bytes=webpage_options.get("pdf_max_bytes"),
        pdf_max_pages=webpage_options.get("pdf_max_pages"),
        pdf_max_text_chars=webpage_options.get("pdf_max_text_chars"),
        user_agent=webpage_options.get("user_agent"),
    )
    webpage_tool = create_webpage_reader_tool(proxy=proxy)

    return {
        "search": search_tool,
        "webpage": webpage_tool,
        "allow_read_webpages": allow_read_webpages,
    }


TOOLS = _build_tools()


def _tool_definitions() -> list[dict[str, Any]]:
    tools = [
        {
            "name": "web_search",
            "description": (
                "Search the public web using ASA's DuckDuckGo pipeline. "
                "Results include URLs and excerpt blocks suitable for citation."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query."},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        }
    ]

    if TOOLS["allow_read_webpages"]:
        tools.append(
            {
                "name": "web_fetch",
                "description": (
                    "Open a public webpage URL and extract readable text relevant "
                    "to the current query."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "Public webpage URL."},
                        "query": {
                            "type": "string",
                            "description": "Optional extraction focus.",
                        },
                    },
                    "required": ["url"],
                    "additionalProperties": False,
                },
            }
        )

    return tools


def _call_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    original_name = name
    name = _canonical_tool_name(name)
    signature = _tool_signature(name, arguments)
    TOOL_STATE["requests"] = int(TOOL_STATE.get("requests", 0) or 0) + 1
    _mcp_log("tools/call", name, json.dumps(arguments, ensure_ascii=False, sort_keys=True))

    if name not in {"web_search", "web_fetch"}:
        return _error_result(
            error_type="unknown_tool",
            message=f"Unknown tool: {original_name}",
            name=name,
            arguments=arguments,
            signature=signature,
            retryable=False,
        )

    if name == "web_search":
        query = str(arguments.get("query") or "").strip()
        if not query:
            return _error_result(
                error_type="invalid_tool_input",
                message="web_search requires a non-empty `query`.",
                name=name,
                arguments=arguments,
                signature=signature,
                retryable=False,
            )
        guarded = _preflight_guard(name, arguments, signature)
        if guarded is not None:
            return guarded
        TOOL_STATE["network_calls"] = int(TOOL_STATE.get("network_calls", 0) or 0) + 1
        _write_progress("tool_call", tool=name)
        try:
            with _tool_deadline(LOOP_GUARD.get("tool_deadline_seconds")):
                result = TOOLS["search"].invoke(query)
        except Exception as exc:
            text = str(exc)
            error_type = "tool_timeout" if isinstance(exc, _ToolDeadlineExpired) or "timeout" in text.lower() or "timed out" in text.lower() else "tool_exception"
            _record_tool_error(name, signature, error_type)
            retryable = (
                error_type == "tool_timeout"
                and int(TOOL_STATE["timeout_by_signature"].get(signature, 0) or 0) < _same_input_timeout_limit(name)
                and int(TOOL_STATE.get("timeouts", 0) or 0) < int(LOOP_GUARD.get("total_timeout_limit", 6) or 6)
            )
            return _error_result(
                error_type=error_type,
                message=text,
                name=name,
                arguments=arguments,
                signature=signature,
                retryable=retryable,
            )
        text = _stringify(result)
        TOOL_STATE["cache"][signature] = text
        _write_progress("tool_result", tool=name)
        return {"content": [{"type": "text", "text": text}], "isError": False}

    if name == "web_fetch":
        if not TOOLS["allow_read_webpages"]:
            return _error_result(
                error_type="tool_disabled",
                message="web_fetch is disabled for this run.",
                name=name,
                arguments=arguments,
                signature=signature,
                retryable=False,
            )
        url = str(arguments.get("url") or "").strip()
        if not url:
            return _error_result(
                error_type="invalid_tool_input",
                message="web_fetch requires a non-empty `url`.",
                name=name,
                arguments=arguments,
                signature=signature,
                retryable=False,
            )
        guarded = _preflight_guard(name, arguments, signature)
        if guarded is not None:
            return guarded
        payload = {"url": url}
        query = str(arguments.get("query") or "").strip()
        if query:
            payload["query"] = query
        TOOL_STATE["network_calls"] = int(TOOL_STATE.get("network_calls", 0) or 0) + 1
        _write_progress("tool_call", tool=name)
        try:
            with _tool_deadline(LOOP_GUARD.get("tool_deadline_seconds")):
                result = TOOLS["webpage"].invoke(payload)
        except Exception as exc:
            text = str(exc)
            error_type = "tool_timeout" if isinstance(exc, _ToolDeadlineExpired) or "timeout" in text.lower() or "timed out" in text.lower() else "tool_exception"
            _record_tool_error(name, signature, error_type)
            retryable = (
                error_type == "tool_timeout"
                and int(TOOL_STATE["timeout_by_signature"].get(signature, 0) or 0) < _same_input_timeout_limit(name)
                and int(TOOL_STATE.get("timeouts", 0) or 0) < int(LOOP_GUARD.get("total_timeout_limit", 6) or 6)
            )
            return _error_result(
                error_type=error_type,
                message=text,
                name=name,
                arguments=arguments,
                signature=signature,
                retryable=retryable,
            )
        text = _stringify(result)
        TOOL_STATE["cache"][signature] = text
        _write_progress("tool_result", tool=name)
        return {"content": [{"type": "text", "text": text}], "isError": False}


def _read_message() -> Optional[Dict[str, Any]]:
    first_line = sys.stdin.buffer.readline()
    if not first_line:
        return None

    stripped = first_line.strip()
    if not stripped:
        return None

    if first_line.lower().startswith(b"content-length:"):
        headers: Dict[str, str] = {}
        line = first_line
        while True:
            if line in (b"\r\n", b"\n"):
                break
            key, _, value = line.decode("utf-8").partition(":")
            headers[key.strip().lower()] = value.strip()
            line = sys.stdin.buffer.readline()
            if not line:
                return None

        length = int(headers.get("content-length", "0") or 0)
        if length <= 0:
            return None
        body = sys.stdin.buffer.read(length)
        if not body:
            return None
        return json.loads(body.decode("utf-8"))

    return json.loads(stripped.decode("utf-8"))


def _write_message(payload: Dict[str, Any]) -> None:
    body = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def _reply(request_id: Any, result: Optional[Dict[str, Any]] = None, error: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result or {}
    _write_message(payload)


def main() -> None:
    while True:
        request = _read_message()
        if request is None:
            return

        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params") or {}

        if method == "initialize":
            _mcp_log("initialize")
            _reply(
                request_id,
                result={
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "asa-free-code-search", "version": "0.1.0"},
                },
            )
            continue

        if method == "notifications/initialized":
            _mcp_log("notifications/initialized")
            continue

        if method == "ping":
            _mcp_log("ping")
            _reply(request_id, result={})
            continue

        if method == "tools/list":
            _mcp_log("tools/list")
            _reply(request_id, result={"tools": _tool_definitions()})
            continue

        if method == "tools/call":
            name = str(params.get("name") or "")
            arguments = params.get("arguments") or {}
            if not isinstance(arguments, dict):
                arguments = {}
            _reply(request_id, result=_call_tool(name, arguments))
            continue

        if request_id is not None:
            _reply(
                request_id,
                error={"code": -32601, "message": f"Method not found: {method}"},
            )


if __name__ == "__main__":
    main()
