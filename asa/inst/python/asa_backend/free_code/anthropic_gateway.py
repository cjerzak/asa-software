"""Local Anthropic-compatible gateway for routing free-code through ASA."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional


def _clear_proxy_env() -> None:
    for key in (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        os.environ[key] = ""


_clear_proxy_env()

from langchain_anthropic import ChatAnthropic
from langchain_aws import ChatBedrockConverse
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage


def _debug_enabled() -> bool:
    return str(os.getenv("ASA_FREE_CODE_GATEWAY_DEBUG", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _debug_log(*parts: Any) -> None:
    if _debug_enabled():
        print("[asa-free-code-gateway]", *parts, flush=True)


def _local_ip() -> str:
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        return str(sock.getsockname()[0])
    except Exception:
        return "127.0.0.1"
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def _openai_compatible_base_url(value: Optional[str], default: str) -> str:
    base_url = str(value or "").strip() or default
    base_url = base_url.rstrip("/")
    if not base_url.endswith("/v1"):
        base_url = f"{base_url}/v1"
    return base_url


def _normalize_azure_openai_base_url(endpoint: Optional[str]) -> str:
    base_url = str(endpoint or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_BASE is required for azure-openai")
    if "api.openai.com" in base_url.lower():
        raise ValueError("Azure backend cannot use api.openai.com")
    allow_insecure = str(os.getenv("ASA_ALLOW_INSECURE_AZURE_OPENAI_ENDPOINT", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not allow_insecure and not base_url.lower().startswith("https://"):
        raise ValueError("Azure OpenAI endpoint must use https://")
    if base_url.lower().endswith("/openai/v1"):
        return base_url
    if base_url.lower().endswith("/openai"):
        return f"{base_url}/v1"
    return f"{base_url}/openai/v1"


def _header_target(handler: BaseHTTPRequestHandler, name: str, default: Optional[str] = None) -> Optional[str]:
    value = handler.headers.get(name)
    if value is None:
        return default
    value = str(value).strip()
    return value or default


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _stream_response_headers(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("Connection", "close")
    handler.end_headers()


def _write_sse_event(handler: BaseHTTPRequestHandler, event: str, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False)
    data = f"event: {event}\ndata: {body}\n\n".encode("utf-8")
    handler.wfile.write(data)
    handler.wfile.flush()


def _read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or 0)
    raw = handler.rfile.read(length) if length > 0 else b"{}"
    if not raw:
      return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"Invalid JSON request body: {exc}") from exc


def _stringify_content(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _extract_text_blocks(content: Any) -> List[str]:
    if content is None:
        return []
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return [_stringify_content(content)]
    texts: List[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if text is not None:
                texts.append(str(text))
    return texts


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        out = int(value)
        return out if out > 0 else default
    except Exception:
        return default


def _loop_guard_config() -> Dict[str, Any]:
    raw = os.getenv("ASA_FREE_CODE_LOOP_GUARD_JSON", "").strip()
    cfg: Dict[str, Any] = {}
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                cfg = parsed
        except Exception:
            cfg = {}
    unknown_after = _as_int(cfg.get("unknown_after_searches"), None)
    return {
        "recursion_limit": _as_int(cfg.get("recursion_limit"), None),
        "search_budget_limit": _as_int(cfg.get("search_budget_limit"), None),
        "unknown_after_searches": unknown_after,
        "repeated_timeout_limit": _as_int(cfg.get("repeated_timeout_limit"), 2) or 2,
        "total_timeout_limit": _as_int(
            cfg.get("total_timeout_limit"),
            max(3, unknown_after) if unknown_after is not None else 6,
        )
        or 6,
        "repeated_invalid_tool_limit": _as_int(cfg.get("repeated_invalid_tool_limit"), 2) or 2,
        "total_invalid_tool_limit": _as_int(cfg.get("total_invalid_tool_limit"), 4) or 4,
        "repeated_tool_request_limit": _as_int(cfg.get("repeated_tool_request_limit"), 4) or 4,
    }


def _progress_token(value: Any, default: str = "na", max_chars: int = 72) -> str:
    text = str(value or default)
    text = "_".join(text.split())
    text = "".join(ch for ch in text if ch.isalnum() or ch in "_.:/-")
    if not text:
        text = default
    return text[:max_chars]


def _write_progress(phase: str, *, tool_used: int = 0, tool_limit: Any = "na", reason: str = "none") -> None:
    path = str(os.getenv("ASA_PROGRESS_STATE_FILE", "") or "").strip()
    if not path:
        return
    remaining: Any = "na"
    try:
        if isinstance(tool_limit, int):
            remaining = max(0, tool_limit - int(tool_used))
    except Exception:
        remaining = "na"
    parts = {
        "phase": phase,
        "node": "free_code_gateway",
        "tool_used": tool_used,
        "tool_limit": tool_limit if isinstance(tool_limit, int) else "na",
        "tool_rem": remaining,
        "pending_tool_calls": "0",
        "pending_tool": "na",
        "stop_reason": _progress_token(reason, default="none"),
        "ts": int(time.time()),
    }
    line = " ".join(f"{key}={value}" for key, value in parts.items())
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        return


def _available_tool_names(request: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for tool in request.get("tools", []) or []:
        if isinstance(tool, dict):
            name = str(tool.get("name") or "").strip()
            if name:
                names.append(name)
    return names


def _tool_base_name(name: str) -> str:
    out = str(name or "").strip()
    for prefix in ("mcp__asa_search__", "asa_search_"):
        if out.startswith(prefix):
            out = out[len(prefix):]
    if out in {"websearch", "search"}:
        return "web_search"
    if out in {"webfetch", "fetch", "openwebpage"}:
        return "web_fetch"
    return out


def _resolve_tool_alias(name: str, available: List[str]) -> Optional[str]:
    if name in available:
        return name
    base = _tool_base_name(name)
    matches = [candidate for candidate in available if _tool_base_name(candidate) == base]
    if len(matches) == 1:
        return matches[0]
    return None


def _tool_signature(name: str, args: Any) -> str:
    if not isinstance(args, dict):
        args = {}
    base = _tool_base_name(name)
    payload: Dict[str, Any]
    if base == "web_search":
        payload = {"query": " ".join(str(args.get("query") or "").lower().split())}
    elif base == "web_fetch":
        payload = {
            "url": " ".join(str(args.get("url") or "").split()),
            "query": " ".join(str(args.get("query") or "").lower().split()),
        }
    else:
        payload = args
    return f"{base}:{json.dumps(payload, ensure_ascii=False, sort_keys=True)}"


def _block_text(block: Any) -> str:
    if isinstance(block, dict):
        content = block.get("content")
        if isinstance(content, list):
            return "\n".join(_stringify_content(item) for item in content)
        return _stringify_content(content)
    return _stringify_content(block)


def _analyze_loop_guard(request: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _loop_guard_config()
    assistant_turns = 0
    tool_requests = 0
    timeout_errors = 0
    invalid_tool_errors = 0
    asa_terminal_errors = 0
    tool_id_to_signature: Dict[str, str] = {}
    errors_by_signature: Dict[str, int] = {}
    invalid_by_tool: Dict[str, int] = {}
    requests_by_signature: Dict[str, int] = {}

    for message in request.get("messages", []) or []:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "").strip().lower()
        content = message.get("content")
        if role == "assistant":
            assistant_turns += 1
            blocks = content if isinstance(content, list) else []
            for block in blocks:
                if not isinstance(block, dict) or block.get("type") != "tool_use":
                    continue
                tool_requests += 1
                tool_id = str(block.get("id") or "")
                signature = _tool_signature(str(block.get("name") or ""), block.get("input") or {})
                if tool_id:
                    tool_id_to_signature[tool_id] = signature
                requests_by_signature[signature] = requests_by_signature.get(signature, 0) + 1
            continue

        if role != "user":
            continue
        blocks = content if isinstance(content, list) else []
        for block in blocks:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            text = _block_text(block)
            lowered = text.lower()
            tool_id = str(block.get("tool_use_id") or "")
            signature = tool_id_to_signature.get(tool_id)
            is_timeout = "-32001" in lowered or "request timed out" in lowered or "tool_timeout" in lowered
            is_invalid = "unavailable tool" in lowered or "invalid tool" in lowered
            is_terminal = "asa_tool_error" in lowered and '"retryable": false' in lowered
            if is_timeout:
                timeout_errors += 1
            if is_invalid:
                invalid_tool_errors += 1
                marker = "unknown"
                if "unavailable tool '" in text:
                    marker = text.split("unavailable tool '", 1)[1].split("'", 1)[0]
                invalid_by_tool[marker] = invalid_by_tool.get(marker, 0) + 1
            if is_terminal:
                asa_terminal_errors += 1
            if signature and (is_timeout or is_invalid or is_terminal or bool(block.get("is_error"))):
                errors_by_signature[signature] = errors_by_signature.get(signature, 0) + 1

    reason = ""
    if cfg["recursion_limit"] is not None and assistant_turns >= int(cfg["recursion_limit"]):
        reason = "recursion_limit"
    elif invalid_tool_errors >= int(cfg["total_invalid_tool_limit"]):
        reason = "invalid_tool_limit"
    elif any(count >= int(cfg["repeated_invalid_tool_limit"]) for count in invalid_by_tool.values()):
        reason = "repeated_invalid_tool"
    elif timeout_errors >= int(cfg["total_timeout_limit"]):
        reason = "timeout_limit"
    elif any(count >= int(cfg["repeated_timeout_limit"]) for count in errors_by_signature.values()):
        reason = "repeated_tool_error"
    elif any(count >= int(cfg["repeated_tool_request_limit"]) for count in requests_by_signature.values()):
        reason = "repeated_tool_request"
    elif asa_terminal_errors > 0:
        reason = "terminal_tool_guard"

    return {
        "finalize": bool(reason),
        "reason": reason,
        "config": cfg,
        "assistant_turns": assistant_turns,
        "tool_requests": tool_requests,
        "timeout_errors": timeout_errors,
        "invalid_tool_errors": invalid_tool_errors,
        "asa_terminal_errors": asa_terminal_errors,
        "max_repeated_tool_request": max(requests_by_signature.values()) if requests_by_signature else 0,
        "max_repeated_tool_error": max(errors_by_signature.values()) if errors_by_signature else 0,
    }


def _finalization_instruction(guard: Dict[str, Any]) -> str:
    return (
        "ASA loop guard triggered: "
        f"{guard.get('reason') or 'tool_cycle'}.\n"
        "Do not call any tools in this response. Produce the final answer now using only "
        "the evidence already present in the conversation. If a requested field is not "
        "supported by the available evidence, mark it Unknown or null according to the "
        "requested output schema. If a strict JSON output was requested, return only JSON."
    )


def _apply_tool_aliases(message: AIMessage, request: Dict[str, Any]) -> AIMessage:
    available = _available_tool_names(request)
    if not available:
        return message
    for tool_call in getattr(message, "tool_calls", []) or []:
        if not isinstance(tool_call, dict):
            continue
        name = str(tool_call.get("name") or "")
        resolved = _resolve_tool_alias(name, available)
        if resolved and resolved != name:
            tool_call["name"] = resolved
    return message


def _convert_request_messages(request: Dict[str, Any]) -> List[Any]:
    messages: List[Any] = []
    system_value = request.get("system")
    system_text = "\n\n".join(
        part for part in _extract_text_blocks(system_value) if part
    ).strip()
    if system_text:
        messages.append(SystemMessage(content=system_text))

    for message in request.get("messages", []) or []:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "").strip().lower()
        content = message.get("content")

        if role == "assistant":
            if isinstance(content, str):
                messages.append(AIMessage(content=content))
                continue
            text_parts = []
            tool_calls = []
            for block in content or []:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    text = block.get("text")
                    if text is not None:
                        text_parts.append(str(text))
                elif block_type == "tool_use":
                    tool_calls.append(
                        {
                            "id": str(block.get("id") or f"toolu_{uuid.uuid4().hex[:12]}"),
                            "name": str(block.get("name") or ""),
                            "args": block.get("input") or {},
                            "type": "tool_call",
                        }
                    )
            messages.append(
                AIMessage(
                    content="\n".join(part for part in text_parts if part),
                    tool_calls=tool_calls,
                )
            )
            continue

        if role != "user":
            continue

        if isinstance(content, str):
            messages.append(HumanMessage(content=content))
            continue

        text_parts = []
        for block in content or []:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                text = block.get("text")
                if text is not None:
                    text_parts.append(str(text))
            elif block_type == "tool_result":
                if text_parts:
                    messages.append(HumanMessage(content="\n".join(text_parts)))
                    text_parts = []
                messages.append(
                    ToolMessage(
                        content=_stringify_content(block.get("content")),
                        tool_call_id=str(block.get("tool_use_id") or ""),
                        status="error" if bool(block.get("is_error")) else "success",
                    )
                )
        if text_parts:
            messages.append(HumanMessage(content="\n".join(text_parts)))

    return messages


def _tool_choice_for_backend(tool_choice: Any) -> Any:
    if not isinstance(tool_choice, dict):
        return None
    choice_type = str(tool_choice.get("type") or "").strip().lower()
    if choice_type in {"", "auto", "none"}:
        return None
    if choice_type == "any":
        return "any"
    if choice_type == "tool":
        name = str(tool_choice.get("name") or "").strip()
        return name or None
    return None


def _create_model(
    backend: str,
    model: str,
    *,
    max_tokens: Optional[int],
    timeout_s: Optional[float],
) -> Any:
    backend = str(backend or "").strip().lower()
    timeout_s = None if timeout_s is None else float(timeout_s)

    if backend in {"openai", "azure-openai", "xai", "exo", "ollama", "openrouter"}:
        base_url = None
        api_key = None
        default_headers = None
        azure_limiter = None
        if backend == "openai":
            base_url = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
            api_key = os.getenv("OPENAI_API_KEY", "")
        elif backend == "azure-openai":
            base_url = _normalize_azure_openai_base_url(
                os.getenv("AZURE_OPENAI_API_BASE") or os.getenv("AZURE_OPENAI_ENDPOINT")
            )
            api_key = os.getenv("AZURE_OPENAI_API_KEY", "")
            if not api_key:
                raise ValueError("AZURE_OPENAI_API_KEY is required for azure-openai")
            os.environ["ASA_MAIN_BACKEND"] = "azure-openai"
            os.environ["ASA_AZURE_OPENAI_ACTIVE_BASE_URL"] = base_url
            os.environ["ASA_AZURE_OPENAI_ACTIVE_DEPLOYMENT"] = model
            try:
                from shared.azure_rate_limit import AzureOpenAISharedRateLimiter

                azure_limiter = AzureOpenAISharedRateLimiter(
                    endpoint=base_url,
                    deployment=model,
                )
            except Exception as exc:
                raise ValueError(f"Could not initialize Azure OpenAI shared limiter: {exc}") from exc
        elif backend == "xai":
            base_url = "https://api.x.ai/v1"
            api_key = os.getenv("XAI_API_KEY", "")
        elif backend == "exo":
            base_url = f"http://{_local_ip()}:52415/v1"
            api_key = "exo-local"
        elif backend == "ollama":
            base_url = _openai_compatible_base_url(
                os.getenv("OLLAMA_API_BASE"),
                "http://127.0.0.1:11434/v1",
            )
            api_key = "ollama-local"
        else:
            base_url = "https://openrouter.ai/api/v1"
            api_key = os.getenv("OPENROUTER_API_KEY", "")
            default_headers = {
                "HTTP-Referer": "https://github.com/cjerzak/asa-software",
                "X-Title": "asa",
            }

        kwargs: Dict[str, Any] = {
            "model": model,
            "api_key": api_key,
            "base_url": base_url,
            "temperature": 0,
            "streaming": False,
        }
        if timeout_s is not None:
            kwargs["timeout"] = timeout_s
        if max_tokens is not None:
            kwargs["max_completion_tokens"] = int(max_tokens)
        if default_headers is not None:
            kwargs["default_headers"] = default_headers
        llm = ChatOpenAI(**kwargs)
        if azure_limiter is not None:
            from shared.azure_rate_limit import wrap_chat_model

            llm = wrap_chat_model(llm, azure_limiter)
        return llm

    if backend == "groq":
        kwargs = {
            "model": model,
            "api_key": os.getenv("GROQ_API_KEY", ""),
            "temperature": 0,
            "streaming": False,
        }
        if timeout_s is not None:
            kwargs["timeout"] = timeout_s
        if max_tokens is not None:
            kwargs["max_tokens"] = int(max_tokens)
        return ChatGroq(**kwargs)

    if backend == "gemini":
        kwargs = {
            "model": model,
            "api_key": os.getenv("GOOGLE_API_KEY", ""),
            "temperature": 0,
            "streaming": False,
        }
        if timeout_s is not None:
            kwargs["request_timeout"] = timeout_s
        if max_tokens is not None:
            kwargs["max_tokens"] = int(max_tokens)
        return ChatGoogleGenerativeAI(**kwargs)

    if backend == "anthropic":
        kwargs = {
            "model_name": model,
            "api_key": os.getenv("ANTHROPIC_API_KEY", ""),
            "temperature": 0,
            "streaming": False,
        }
        if timeout_s is not None:
            kwargs["timeout"] = timeout_s
        if max_tokens is not None:
            kwargs["max_tokens_to_sample"] = int(max_tokens)
        return ChatAnthropic(**kwargs)

    if backend == "bedrock":
        kwargs = {
            "model": model,
            "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "temperature": 0,
        }
        if timeout_s is not None:
            kwargs["config"] = {"read_timeout": timeout_s, "connect_timeout": timeout_s}
        if max_tokens is not None:
            kwargs["max_tokens"] = int(max_tokens)
        return ChatBedrockConverse(**kwargs)

    raise ValueError(f"Unsupported ASA backend for free-code gateway: {backend}")


def _invoke_model(request: Dict[str, Any], backend: str, model: str) -> AIMessage:
    max_tokens = request.get("max_tokens")
    try:
        max_tokens = None if max_tokens is None else int(max_tokens)
    except Exception:
        max_tokens = None

    timeout_s = os.getenv("ASA_FREE_CODE_GATEWAY_TIMEOUT", "")
    try:
        timeout_value = float(timeout_s) if timeout_s else None
    except Exception:
        timeout_value = None

    llm = _create_model(
        backend,
        model,
        max_tokens=max_tokens,
        timeout_s=timeout_value,
    )

    anthropic_tools = []
    for tool in request.get("tools", []) or []:
        if not isinstance(tool, dict):
            continue
        anthropic_tools.append(
            {
                "name": str(tool.get("name") or ""),
                "description": str(tool.get("description") or ""),
                "input_schema": tool.get("input_schema") or {"type": "object", "properties": {}},
            }
        )

    guard = _analyze_loop_guard(request)
    messages = _convert_request_messages(request)
    if guard.get("finalize"):
        messages.append(HumanMessage(content=_finalization_instruction(guard)))
        anthropic_tools = []

    _write_progress(
        "model_finalize" if guard.get("finalize") else "model_invoke",
        tool_used=int(guard.get("tool_requests", 0) or 0),
        tool_limit=guard.get("config", {}).get("search_budget_limit"),
        reason=str(guard.get("reason") or "none"),
    )
    runnable = llm
    if anthropic_tools:
        runnable = llm.bind_tools(
            anthropic_tools,
            tool_choice=_tool_choice_for_backend(request.get("tool_choice")),
        )

    response = runnable.invoke(messages)
    if not isinstance(response, AIMessage):
        response = AIMessage(content=_stringify_content(response))
    if guard.get("finalize") and getattr(response, "tool_calls", None):
        response.tool_calls = []
    if not guard.get("finalize"):
        response = _apply_tool_aliases(response, request)
    return response


def _usage_payload(message: AIMessage) -> Dict[str, int]:
    usage = getattr(message, "usage_metadata", None) or {}
    input_tokens = int(usage.get("input_tokens", 0) or 0)
    output_tokens = int(usage.get("output_tokens", 0) or 0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


def _anthropic_content_blocks(message: AIMessage) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    content = message.content
    if isinstance(content, str):
      if content:
          blocks.append({"type": "text", "text": content})
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, str):
                if item:
                    blocks.append({"type": "text", "text": item})
            elif isinstance(item, dict):
                item_type = item.get("type")
                if item_type == "text" and item.get("text") is not None:
                    blocks.append({"type": "text", "text": str(item.get("text"))})

    for tool_call in getattr(message, "tool_calls", []) or []:
        blocks.append(
            {
                "type": "tool_use",
                "id": str(tool_call.get("id") or f"toolu_{uuid.uuid4().hex[:12]}"),
                "name": str(tool_call.get("name") or ""),
                "input": tool_call.get("args") or {},
            }
        )

    if not blocks:
        blocks.append({"type": "text", "text": ""})
    return blocks


def _anthropic_stop_reason(message: AIMessage) -> str:
    tool_calls = getattr(message, "tool_calls", []) or []
    if tool_calls:
        return "tool_use"
    return "end_turn"


def _stream_anthropic_message(
    handler: BaseHTTPRequestHandler,
    request: Dict[str, Any],
    response_message: AIMessage,
) -> None:
    content_blocks = _anthropic_content_blocks(response_message)
    usage = _usage_payload(response_message)
    message_id = f"msg_{uuid.uuid4().hex}"
    model_name = str(request.get("model") or "")

    _stream_response_headers(handler)
    _write_sse_event(
        handler,
        "message_start",
        {
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model_name,
                "content": [],
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {
                    "input_tokens": usage.get("input_tokens", 0),
                    "output_tokens": 0,
                },
            },
        },
    )

    for index, block in enumerate(content_blocks):
        block_type = block.get("type")
        if block_type == "text":
            _write_sse_event(
                handler,
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "text", "text": ""},
                },
            )
            _write_sse_event(
                handler,
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "text_delta", "text": str(block.get("text") or "")},
                },
            )
            _write_sse_event(
                handler,
                "content_block_stop",
                {"type": "content_block_stop", "index": index},
            )
            continue

        if block_type == "tool_use":
            _write_sse_event(
                handler,
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": str(block.get("id") or f"toolu_{uuid.uuid4().hex[:12]}"),
                        "name": str(block.get("name") or ""),
                        "input": {},
                    },
                },
            )
            input_json = json.dumps(block.get("input") or {}, ensure_ascii=False)
            if input_json:
                _write_sse_event(
                    handler,
                    "content_block_delta",
                    {
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "input_json_delta", "partial_json": input_json},
                    },
                )
            _write_sse_event(
                handler,
                "content_block_stop",
                {"type": "content_block_stop", "index": index},
            )

    _write_sse_event(
        handler,
        "message_delta",
        {
            "type": "message_delta",
            "delta": {
                "stop_reason": _anthropic_stop_reason(response_message),
                "stop_sequence": None,
            },
            "usage": {"output_tokens": usage.get("output_tokens", 0)},
        },
    )
    _write_sse_event(handler, "message_stop", {"type": "message_stop"})


class _GatewayHandler(BaseHTTPRequestHandler):
    server_version = "ASAFreeCodeGateway/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return None

    def do_HEAD(self) -> None:  # noqa: N802
        _debug_log("HEAD", self.path)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        _debug_log("GET", self.path)
        _json_response(self, 200, {"ok": True, "service": "asa-free-code-gateway"})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path not in {"/v1/messages", "/messages"}:
            _json_response(self, 404, {"error": {"type": "not_found", "message": f"Unsupported path: {path}"}})
            return

        try:
            request = _read_json(self)
            target_backend = _header_target(self, "X-ASA-Target-Backend")
            target_model = _header_target(self, "X-ASA-Target-Model")
            if not target_backend or not target_model:
                raise ValueError("Missing X-ASA-Target-Backend or X-ASA-Target-Model header.")

            _debug_log(
                "POST /v1/messages",
                f"stream={bool(request.get('stream'))}",
                f"backend={target_backend}",
                f"model={target_model}",
            )
            response_message = _invoke_model(request, target_backend, target_model)
            if bool(request.get("stream")):
                _stream_anthropic_message(self, request, response_message)
                _debug_log("stream response complete")
                return
            payload = {
                "id": f"msg_{uuid.uuid4().hex}",
                "type": "message",
                "role": "assistant",
                "model": str(request.get("model") or ""),
                "content": _anthropic_content_blocks(response_message),
                "stop_reason": _anthropic_stop_reason(response_message),
                "stop_sequence": None,
                "usage": _usage_payload(response_message),
            }
            _json_response(self, 200, payload)
            _debug_log("json response complete")
        except Exception as exc:
            _debug_log("error", str(exc))
            _json_response(
                self,
                500,
                {
                    "type": "error",
                    "error": {
                        "type": "api_error",
                        "message": str(exc),
                    },
                },
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="ASA free-code Anthropic gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), _GatewayHandler)
    port_file = os.getenv("ASA_FREE_CODE_PORT_FILE", "")
    if port_file:
        with open(port_file, "w", encoding="utf-8") as handle:
            handle.write(str(server.server_port))
            handle.flush()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
