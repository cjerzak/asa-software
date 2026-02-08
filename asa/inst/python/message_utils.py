# message_utils.py
#
# Shared helpers for best-effort message content extraction and text coercion.
#
from typing import Any, Literal


def message_content_from_message(msg: Any) -> Any:
    """Best-effort extraction of message content payload."""
    if msg is None:
        return None
    try:
        if isinstance(msg, dict):
            return msg.get("content", msg.get("text"))
    except Exception:
        pass
    try:
        return getattr(msg, "content", None)
    except Exception:
        return None


def message_content_to_text(
    content: Any,
    *,
    list_mode: Literal["join", "stringify"] = "join",
) -> str:
    """Best-effort conversion of message content payload to text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content.replace("\x00", "")

    if isinstance(content, list):
        if list_mode == "stringify":
            try:
                return str(content)
            except Exception:
                return ""

        parts = []
        for item in content:
            if isinstance(item, str):
                if item.strip():
                    parts.append(item)
                continue

            if isinstance(item, dict):
                for key in ("text", "content", "value"):
                    val = item.get(key)
                    if isinstance(val, str) and val.strip():
                        parts.append(val)
                        break
                continue

            try:
                item_text = str(item)
            except Exception:
                item_text = ""
            if item_text.strip():
                parts.append(item_text)
        return "\n".join(parts).replace("\x00", "")

    try:
        return str(content).replace("\x00", "")
    except Exception:
        return ""


def message_text_from_message(
    msg: Any,
    *,
    list_mode: Literal["join", "stringify"] = "join",
) -> str:
    """Extract message content then coerce to text."""
    return message_content_to_text(message_content_from_message(msg), list_mode=list_mode)
