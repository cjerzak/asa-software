"""Minimal stdio MCP server exposing ASA web search and webpage fetch tools."""

from __future__ import annotations

import json
import os
import sys
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


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


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
    _mcp_log("tools/call", name, json.dumps(arguments, ensure_ascii=False, sort_keys=True))
    if name == "web_search":
        query = str(arguments.get("query") or "").strip()
        if not query:
            return {
                "content": [{"type": "text", "text": "web_search requires a non-empty `query`."}],
                "isError": True,
            }
        result = TOOLS["search"].invoke(query)
        return {
            "content": [{"type": "text", "text": _stringify(result)}],
            "isError": False,
        }

    if name == "web_fetch":
        if not TOOLS["allow_read_webpages"]:
            return {
                "content": [{"type": "text", "text": "web_fetch is disabled for this run."}],
                "isError": True,
            }
        url = str(arguments.get("url") or "").strip()
        if not url:
            return {
                "content": [{"type": "text", "text": "web_fetch requires a non-empty `url`."}],
                "isError": True,
            }
        payload = {"url": url}
        query = str(arguments.get("query") or "").strip()
        if query:
            payload["query"] = query
        result = TOOLS["webpage"].invoke(payload)
        return {
            "content": [{"type": "text", "text": _stringify(result)}],
            "isError": False,
        }

    return {
        "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
        "isError": True,
    }


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
