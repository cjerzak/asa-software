# webpage_tool.py
#
# LangChain-compatible tool for reading public webpages.
# Designed for use inside LangGraph agents and graphs.
#
# Key goals:
# - Optional capability: disabled by default unless allow_read_webpages is enabled.
# - Safety: basic SSRF protections (block localhost/private IPs, non-http(s) schemes).
# - Relevance: when given a query, return only the most relevant excerpts.
#   Supports embedding-based relevance (optional; falls back to lexical).
#
import hashlib
import ipaddress
import json
import logging
import math
import os
import re
import shutil
import socket
import subprocess
import threading
import tempfile
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional, Tuple, Type
from urllib.parse import urljoin, urlparse, urlunparse

from curl_cffi import requests
from bs4 import BeautifulSoup
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

try:
    RequestException = requests.exceptions.RequestException
except Exception:
    RequestException = getattr(requests, "RequestsError", Exception)


@dataclass
class WebpageReaderConfig:
    """Global configuration for webpage reading."""
    allow_read_webpages: bool = False
    timeout: float = 20.0
    max_bytes: int = 2_000_000  # hard cap on downloaded bytes
    max_chars: int = 8_000      # hard cap on returned text
    max_chunks: int = 6         # relevant chunks to return
    chunk_chars: int = 1_200
    # Relevance selection: "auto" (default), "lexical", "embeddings"
    relevance_mode: str = "auto"
    # Link annotation heuristics profile: "generic" (default)
    heuristic_profile: str = "generic"
    # Embedding selection: "auto" (default), "openai", "sentence_transformers"
    embedding_provider: str = "auto"
    embedding_model: str = "text-embedding-3-small"
    embedding_api_base: Optional[str] = None  # defaults to OPENAI_API_BASE or https://api.openai.com/v1
    prefilter_k: int = 30
    use_mmr: bool = True
    mmr_lambda: float = 0.7
    # Per-run caching (cleared by the R wrapper per operation)
    cache_enabled: bool = True
    cache_max_entries: int = 64
    cache_max_text_chars: int = 200_000
    blocked_cache_ttl_sec: int = 600
    blocked_cache_max_entries: int = 256
    blocked_probe_bytes: int = 65_536
    blocked_detect_on_200: bool = True
    blocked_body_scan_bytes: int = 32_768
    pdf_enabled: bool = True
    pdf_timeout: float = 20.0
    pdf_max_bytes: int = 8_000_000
    pdf_max_pages: int = 8
    pdf_max_text_chars: int = 40_000
    user_agent: str = "ASA-Research-Agent/1.0"


_config_lock = threading.Lock()
_default_config = WebpageReaderConfig()

_cache_lock = threading.Lock()
_page_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_url_aliases: Dict[str, str] = {}
_blocked_cache_lock = threading.Lock()
_blocked_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_blocked_aliases: Dict[str, str] = {}


def clear_webpage_reader_cache() -> None:
    """Clear the in-memory webpage cache."""
    with _cache_lock:
        _page_cache.clear()
        _url_aliases.clear()
    with _blocked_cache_lock:
        _blocked_cache.clear()
        _blocked_aliases.clear()


def _normalize_url_for_cache(url: str) -> str:
    parsed = urlparse(url or "")
    scheme = (parsed.scheme or "").lower()
    hostname = (parsed.hostname or "").lower().rstrip(".")
    port = parsed.port

    netloc = hostname
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"

    # Normalize empty path to "/"; drop fragment to avoid cache misses.
    path = parsed.path or "/"
    query = parsed.query or ""
    return urlunparse((scheme, netloc, path, "", query, ""))


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    with _cache_lock:
        resolved = _url_aliases.get(key, key)
        entry = _page_cache.get(resolved)
        if entry is not None:
            # LRU: keep hot entries at the end.
            _page_cache.move_to_end(resolved, last=True)
        return entry


def _cache_set(key: str, entry: Dict[str, Any], *, cfg: WebpageReaderConfig) -> None:
    if not cfg.cache_enabled:
        return
    with _cache_lock:
        _page_cache[key] = entry
        _page_cache.move_to_end(key, last=True)
        # Enforce cache size.
        max_entries = max(1, int(cfg.cache_max_entries or 64))
        while len(_page_cache) > max_entries:
            old_key, _old_val = _page_cache.popitem(last=False)
            # Drop aliases pointing to evicted keys.
            for k, v in list(_url_aliases.items()):
                if k == old_key or v == old_key:
                    del _url_aliases[k]


def _cache_alias(from_key: str, to_key: str) -> None:
    with _cache_lock:
        _url_aliases[from_key] = to_key


def _drop_blocked_aliases_for_key(cache_key: str) -> None:
    for alias_key, alias_value in list(_blocked_aliases.items()):
        if alias_key == cache_key or alias_value == cache_key:
            del _blocked_aliases[alias_key]


def _blocked_cache_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _blocked_cache_lock:
        resolved = _blocked_aliases.get(key, key)
        entry = _blocked_cache.get(resolved)
        if entry is None:
            return None
        expires_at = float(entry.get("expires_at") or 0.0)
        if expires_at > 0 and expires_at <= now:
            _blocked_cache.pop(resolved, None)
            _drop_blocked_aliases_for_key(resolved)
            return None
        _blocked_cache.move_to_end(resolved, last=True)
        return entry


def _blocked_cache_set(key: str, entry: Dict[str, Any], *, cfg: WebpageReaderConfig) -> None:
    if not cfg.cache_enabled:
        return
    ttl = int(cfg.blocked_cache_ttl_sec or 0)
    if ttl <= 0:
        return
    with _blocked_cache_lock:
        payload = dict(entry)
        payload["expires_at"] = time.time() + float(ttl)
        _blocked_cache[key] = payload
        _blocked_cache.move_to_end(key, last=True)
        max_entries = max(1, int(cfg.blocked_cache_max_entries or 256))
        while len(_blocked_cache) > max_entries:
            old_key, _old_val = _blocked_cache.popitem(last=False)
            _drop_blocked_aliases_for_key(old_key)


def _blocked_cache_alias(from_key: str, to_key: str) -> None:
    with _blocked_cache_lock:
        _blocked_aliases[from_key] = to_key


def configure_webpage_reader(
    allow_read_webpages: bool = None,
    timeout: float = None,
    max_bytes: int = None,
    max_chars: int = None,
    max_chunks: int = None,
    chunk_chars: int = None,
    relevance_mode: str = None,
    heuristic_profile: str = None,
    embedding_provider: str = None,
    embedding_model: str = None,
    embedding_api_base: str = None,
    prefilter_k: int = None,
    use_mmr: bool = None,
    mmr_lambda: float = None,
    cache_enabled: bool = None,
    cache_max_entries: int = None,
    cache_max_text_chars: int = None,
    blocked_cache_ttl_sec: int = None,
    blocked_cache_max_entries: int = None,
    blocked_probe_bytes: int = None,
    blocked_detect_on_200: bool = None,
    blocked_body_scan_bytes: int = None,
    pdf_enabled: bool = None,
    pdf_timeout: float = None,
    pdf_max_bytes: int = None,
    pdf_max_pages: int = None,
    pdf_max_text_chars: int = None,
    user_agent: str = None,
) -> WebpageReaderConfig:
    """Configure global defaults for webpage reading. Call from R via reticulate.

    If all args are None, returns the current config without modification.
    Thread-safe: uses lock to prevent races during configuration.
    """
    global _default_config
    with _config_lock:
        if allow_read_webpages is not None:
            _default_config.allow_read_webpages = bool(allow_read_webpages)
        if timeout is not None:
            _default_config.timeout = float(timeout)
        if max_bytes is not None:
            _default_config.max_bytes = int(max_bytes)
        if max_chars is not None:
            _default_config.max_chars = int(max_chars)
        if max_chunks is not None:
            _default_config.max_chunks = int(max_chunks)
        if chunk_chars is not None:
            _default_config.chunk_chars = int(chunk_chars)
        if relevance_mode is not None:
            _default_config.relevance_mode = str(relevance_mode)
        if heuristic_profile is not None:
            _default_config.heuristic_profile = _normalize_heuristic_profile(heuristic_profile)
        if embedding_provider is not None:
            _default_config.embedding_provider = str(embedding_provider)
        if embedding_model is not None:
            _default_config.embedding_model = str(embedding_model)
        if embedding_api_base is not None:
            _default_config.embedding_api_base = str(embedding_api_base) if embedding_api_base else None
        if prefilter_k is not None:
            _default_config.prefilter_k = int(prefilter_k)
        if use_mmr is not None:
            _default_config.use_mmr = bool(use_mmr)
        if mmr_lambda is not None:
            _default_config.mmr_lambda = float(mmr_lambda)
        if cache_enabled is not None:
            _default_config.cache_enabled = bool(cache_enabled)
        if cache_max_entries is not None:
            _default_config.cache_max_entries = int(cache_max_entries)
        if cache_max_text_chars is not None:
            _default_config.cache_max_text_chars = int(cache_max_text_chars)
        if blocked_cache_ttl_sec is not None:
            _default_config.blocked_cache_ttl_sec = int(blocked_cache_ttl_sec)
        if blocked_cache_max_entries is not None:
            _default_config.blocked_cache_max_entries = int(blocked_cache_max_entries)
        if blocked_probe_bytes is not None:
            _default_config.blocked_probe_bytes = int(blocked_probe_bytes)
        if blocked_detect_on_200 is not None:
            _default_config.blocked_detect_on_200 = bool(blocked_detect_on_200)
        if blocked_body_scan_bytes is not None:
            _default_config.blocked_body_scan_bytes = int(blocked_body_scan_bytes)
        if pdf_enabled is not None:
            _default_config.pdf_enabled = bool(pdf_enabled)
        if pdf_timeout is not None:
            _default_config.pdf_timeout = float(pdf_timeout)
        if pdf_max_bytes is not None:
            _default_config.pdf_max_bytes = int(pdf_max_bytes)
        if pdf_max_pages is not None:
            _default_config.pdf_max_pages = int(pdf_max_pages)
        if pdf_max_text_chars is not None:
            _default_config.pdf_max_text_chars = int(pdf_max_text_chars)
        if user_agent is not None:
            _default_config.user_agent = str(user_agent)
        return _default_config


def _normalize_heuristic_profile(profile: Optional[str]) -> str:
    text = str(profile or "").strip().lower()
    if text in {"", "generic"}:
        return "generic"
    if text == "legacy":
        raise ValueError(
            "heuristic_profile='legacy' is no longer supported; use 'generic'."
        )
    raise ValueError("heuristic_profile must be 'generic'.")


def _is_disallowed_host(hostname: str) -> Tuple[bool, str]:
    """Best-effort SSRF guard. Blocks obvious internal addresses and hostnames."""
    if not hostname:
        return True, "missing_hostname"

    host = hostname.strip().lower().rstrip(".")

    if host in {"localhost", "localhost.localdomain"}:
        return True, "localhost"
    if host.endswith(".local"):
        return True, "dot_local"

    # If hostname is an IP literal, validate directly.
    try:
        ip = ipaddress.ip_address(host)
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return True, f"disallowed_ip:{ip}"
        return False, "ok"
    except ValueError:
        pass  # not an IP literal

    # Resolve host to IPs and block private ranges.
    try:
        infos = socket.getaddrinfo(host, None)
        ips = sorted({info[4][0] for info in infos if info and info[4]})
        if not ips:
            return True, "unresolvable_host"
        for ip_s in ips:
            try:
                ip = ipaddress.ip_address(ip_s)
                if (
                    ip.is_private
                    or ip.is_loopback
                    or ip.is_link_local
                    or ip.is_multicast
                    or ip.is_reserved
                    or ip.is_unspecified
                ):
                    return True, f"disallowed_resolved_ip:{ip}"
            except ValueError:
                continue
        return False, "ok"
    except Exception as e:
        return True, f"resolve_error:{e}"


def _validate_url(url: str) -> Tuple[bool, str, str]:
    """Validate URL format and enforce basic SSRF protections.

    Returns (ok, reason, normalized_url)
    """
    if not isinstance(url, str) or not url.strip():
        return False, "empty_url", ""

    u = url.strip()
    parsed = urlparse(u)
    if parsed.scheme not in {"http", "https"}:
        return False, "unsupported_scheme", u
    if not parsed.netloc:
        return False, "missing_netloc", u
    if parsed.username or parsed.password:
        return False, "credentials_in_url", u

    hostname = parsed.hostname or ""
    disallowed, reason = _is_disallowed_host(hostname)
    if disallowed:
        return False, reason, u

    return True, "ok", u


def _extract_main_text(
    html: str,
    page_url: str = "",
    cfg: Optional[WebpageReaderConfig] = None,
) -> Tuple[str, str]:
    """Extract readable text from HTML. Returns (title, text).

    Preserves hyperlinks inline so that the agent can discover linked URLs
    (e.g., detail pages referenced from index/listing pages).
    """
    soup = BeautifulSoup(html or "", "html.parser")

    title = ""
    try:
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
    except Exception:
        title = ""

    # Remove clearly non-content elements
    for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
        try:
            tag.decompose()
        except Exception:
            continue

    # Prefer <article> or <main> when present.
    candidates = []
    for selector in ["article", "main", "body"]:
        node = soup.find(selector)
        if node:
            candidates.append(node)

    def node_text_len(node) -> int:
        try:
            text = " ".join(list(node.stripped_strings))
            return len(text)
        except Exception:
            return 0

    best = max(candidates, key=node_text_len) if candidates else soup

    # --- Inline hyperlinks before text extraction ---
    # Detect the base URL from <base> tag if present, for resolving relative hrefs.
    base_url = ""
    base_tag = soup.find("base")
    if base_tag and base_tag.get("href"):
        base_url = base_tag["href"].strip()
    elif page_url:
        base_url = page_url

    heuristic_profile = _normalize_heuristic_profile(
        getattr(cfg, "heuristic_profile", "generic")
    )

    def _should_annotate_link(href: str, link_text: str) -> bool:
        """Decide if a hyperlink is worth inlining in extracted text."""
        if not href:
            return False
        href_lower = href.lower()
        text_lower = link_text.lower().strip()
        # Skip fragment-only links, javascript, mailto, tel
        if href_lower.startswith(("#", "javascript:", "mailto:", "tel:")):
            return False
        if not text_lower:
            return False

        # Generic boilerplate navigation labels are rarely evidence-bearing.
        nav_tokens = {
            "home", "inicio", "menu", "buscar", "search", "rss", "xml",
            "login", "log in", "sign in", "register", "registr", "contact",
            "about", "privacy", "terms", "cookie", "cookies", "help",
            "next", "previous", "prev", "back",
        }
        if text_lower in nav_tokens:
            return False

        words = [w for w in link_text.strip().split() if w]
        has_descriptive_anchor = len(text_lower) >= 8 and len(words) >= 2
        is_absolute = href_lower.startswith(("http://", "https://"))
        has_structured_path = bool(re.search(r"[0-9]{2,}|[-_/]", href_lower))

        # Generic default: annotate links with meaningful anchor text or
        # URL/path structure likely tied to specific records/documents.
        if has_descriptive_anchor:
            return True
        if has_structured_path and len(text_lower) >= 4:
            return True
        if not is_absolute and len(text_lower) >= 3:
            return True
        return False

    def _resolve_href(href: str) -> str:
        """Resolve a potentially relative href to an absolute URL."""
        if not href:
            return href
        href = href.strip()
        if href.startswith(("http://", "https://")):
            return href
        if base_url:
            return urljoin(base_url, href)
        return href

    if best:
        for a_tag in best.find_all("a", href=True):
            try:
                href = a_tag.get("href", "")
                link_text = a_tag.get_text(strip=True)
                if _should_annotate_link(href, link_text):
                    resolved = _resolve_href(href)
                    # Append link annotation after the anchor text
                    marker = soup.new_string(f" [link: {resolved}]")
                    a_tag.append(marker)
            except Exception:
                continue

    raw_text = "\n".join(best.stripped_strings) if best else ""

    # Normalize whitespace but keep newlines as separators.
    lines = []
    for line in raw_text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    text = "\n".join(lines)
    return title, text


_WORD_RE = re.compile(r"[A-Za-z0-9]{2,}")
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have",
    "in", "is", "it", "its", "of", "on", "or", "that", "the", "to", "was", "were",
    "with", "this", "these", "those", "you", "your", "we", "our", "their", "they",
}


def _tokenize(text: str) -> List[str]:
    return [t.lower() for t in _WORD_RE.findall(text or "")]


def _chunk_text(text: str, *, chunk_chars: int) -> List[str]:
    """Chunk the page by lines to preserve some structure."""
    if not text:
        return []
    lines = text.splitlines()
    chunks: List[str] = []
    buf: List[str] = []
    size = 0
    for line in lines:
        if not line:
            continue
        if size + len(line) + 1 > chunk_chars and buf:
            chunks.append("\n".join(buf))
            buf = [line]
            size = len(line) + 1
        else:
            buf.append(line)
            size += len(line) + 1
    if buf:
        chunks.append("\n".join(buf))
    return chunks


def _lexical_scores(chunks: List[str], q_tokens: List[str]) -> List[int]:
    scores: List[int] = []
    for ch in chunks:
        tokens = _tokenize(ch)
        if not tokens:
            scores.append(0)
            continue
        score = 0
        for qt in q_tokens:
            score += tokens.count(qt)
        scores.append(score)
    return scores


def _relevant_chunks_lexical(text: str, query: str, *, chunk_chars: int, max_chunks: int) -> List[str]:
    """Return top-N chunks by simple lexical overlap scoring."""
    if not text:
        return []
    if not query or not query.strip():
        # No query: return the start of the page as a single chunk.
        return [text[:chunk_chars]]

    q_tokens = [t for t in _tokenize(query) if t not in _STOPWORDS]
    if not q_tokens:
        return [text[:chunk_chars]]

    chunks = _chunk_text(text, chunk_chars=chunk_chars)
    if not chunks:
        return []
    scored = list(zip(_lexical_scores(chunks, q_tokens), chunks))
    scored.sort(key=lambda x: x[0], reverse=True)

    # Keep only chunks with some signal; fall back to the first chunk.
    top = [ch for s, ch in scored if s > 0][:max_chunks]
    if not top:
        return [chunks[0][:chunk_chars]] if chunks else []
    return top


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    dot = 0.0
    na = 0.0
    nb = 0.0
    for i in range(n):
        x = float(a[i])
        y = float(b[i])
        dot += x * y
        na += x * x
        nb += y * y
    denom = math.sqrt(na) * math.sqrt(nb)
    return dot / denom if denom > 0 else 0.0


def _openai_api_base(cfg: WebpageReaderConfig) -> str:
    base = cfg.embedding_api_base or os.getenv("OPENAI_API_BASE") or "https://api.openai.com/v1"
    base = base.rstrip("/")
    # Ensure /v1 suffix (OpenAI-compatible).
    if not base.endswith("/v1"):
        base = base + "/v1"
    return base


def _openai_embed_texts(texts: List[str], *, cfg: WebpageReaderConfig) -> List[List[float]]:
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("missing OPENAI_API_KEY for embeddings")

    url = _openai_api_base(cfg) + "/embeddings"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": cfg.user_agent,
    }
    payload = {"model": cfg.embedding_model, "input": texts}
    r = requests.post(url, headers=headers, json=payload, timeout=cfg.timeout)
    r.raise_for_status()
    data = (r.json() or {}).get("data") or []
    data.sort(key=lambda d: int(d.get("index", 0)))
    out: List[List[float]] = []
    for item in data:
        emb = item.get("embedding")
        if isinstance(emb, list) and emb:
            out.append(emb)
    if len(out) != len(texts):
        raise RuntimeError("unexpected embeddings response shape")
    return out


_st_lock = threading.Lock()
_st_models: Dict[str, Any] = {}


def _st_embed_texts(texts: List[str], *, cfg: WebpageReaderConfig) -> List[List[float]]:
    try:
        from sentence_transformers import SentenceTransformer
    except Exception as e:
        raise RuntimeError(f"sentence_transformers unavailable: {e}")

    model_name = cfg.embedding_model
    with _st_lock:
        model = _st_models.get(model_name)
        if model is None:
            model = SentenceTransformer(model_name)
            _st_models[model_name] = model

    # normalize_embeddings=True makes cosine similarity equal to dot product.
    vecs = model.encode(texts, normalize_embeddings=True)
    return [v.tolist() for v in vecs]


def _embed_provider(cfg: WebpageReaderConfig) -> str:
    provider = (cfg.embedding_provider or "auto").strip().lower()
    if provider in {"openai", "sentence_transformers"}:
        return provider
    # auto: prefer local ST if available, else OpenAI if key present.
    try:
        import sentence_transformers  # noqa: F401
        return "sentence_transformers"
    except Exception:
        pass
    if os.getenv("OPENAI_API_KEY"):
        return "openai"
    return "none"


def _embed_texts(texts: List[str], *, cfg: WebpageReaderConfig) -> List[List[float]]:
    provider = _embed_provider(cfg)
    if provider == "sentence_transformers":
        return _st_embed_texts(texts, cfg=cfg)
    if provider == "openai":
        return _openai_embed_texts(texts, cfg=cfg)
    raise RuntimeError("no embedding provider available")


def _mmr_select(
    query_vec: List[float],
    chunk_vecs: List[List[float]],
    sims: List[float],
    *,
    k: int,
    lambda_mult: float,
) -> List[int]:
    """Maximal marginal relevance selection (returns indices into chunk_vecs)."""
    if k <= 0 or not chunk_vecs:
        return []
    k = min(k, len(chunk_vecs))
    lambda_mult = max(0.0, min(1.0, float(lambda_mult)))

    selected: List[int] = []
    candidates = list(range(len(chunk_vecs)))

    # Seed with best similarity.
    best = max(candidates, key=lambda i: sims[i])
    selected.append(best)
    candidates.remove(best)

    while candidates and len(selected) < k:
        def mmr_score(i: int) -> float:
            diversity = max(_cosine_similarity(chunk_vecs[i], chunk_vecs[j]) for j in selected) if selected else 0.0
            return lambda_mult * sims[i] - (1.0 - lambda_mult) * diversity

        nxt = max(candidates, key=mmr_score)
        selected.append(nxt)
        candidates.remove(nxt)
    return selected


def _relevant_chunks_embeddings(
    text: str,
    query: str,
    *,
    chunk_chars: int,
    max_chunks: int,
    cfg: WebpageReaderConfig,
    cache_entry: Optional[Dict[str, Any]] = None,
) -> List[str]:
    if not text:
        return []
    chunks = _chunk_text(text, chunk_chars=chunk_chars)
    if not chunks:
        return []

    if not query or not query.strip():
        return [chunks[0][:chunk_chars]]

    q_tokens = [t for t in _tokenize(query) if t not in _STOPWORDS]

    # Lexical prefilter to reduce embedding work.
    candidate_idx = list(range(len(chunks)))
    prefilter_k = int(cfg.prefilter_k or 0)
    if prefilter_k > 0 and len(chunks) > prefilter_k and q_tokens:
        scores = _lexical_scores(chunks, q_tokens)
        ranked = sorted(range(len(chunks)), key=lambda i: scores[i], reverse=True)
        candidate_idx = [i for i in ranked[:prefilter_k] if scores[i] > 0]
        if not candidate_idx:
            # If lexical prefilter found nothing, fall back to first chunk.
            candidate_idx = [0]

    candidates = [chunks[i] for i in candidate_idx]

    embed_key = f"{_embed_provider(cfg)}:{cfg.embedding_model}:{_openai_api_base(cfg) if _embed_provider(cfg)=='openai' else ''}"

    # Cache embeddings per-page to avoid repeated API calls within a run.
    emb_cache: Dict[str, Dict[str, List[float]]] = {}
    q_cache: Dict[str, Dict[str, List[float]]] = {}
    if isinstance(cache_entry, dict):
        emb_cache = cache_entry.setdefault("embedding_cache", {})
        q_cache = cache_entry.setdefault("query_embedding_cache", {})

    per_key = emb_cache.setdefault(embed_key, {})
    per_key_q = q_cache.setdefault(embed_key, {})

    def h(txt: str) -> str:
        return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()

    # Query embedding (cached)
    qh = h(query.strip().lower())
    if qh in per_key_q:
        query_vec = per_key_q[qh]
    else:
        query_vec = _embed_texts([query], cfg=cfg)[0]
        per_key_q[qh] = query_vec

    # Chunk embeddings (cached)
    chunk_vecs: List[List[float]] = [None] * len(candidates)  # type: ignore
    missing_texts: List[str] = []
    missing_keys: List[Tuple[int, str]] = []
    for i, ch in enumerate(candidates):
        ch_key = h(ch)
        vec = per_key.get(ch_key)
        if vec is not None:
            chunk_vecs[i] = vec
        else:
            missing_texts.append(ch)
            missing_keys.append((i, ch_key))

    if missing_texts:
        vecs = _embed_texts(missing_texts, cfg=cfg)
        for (i, ch_key), v in zip(missing_keys, vecs):
            per_key[ch_key] = v
            chunk_vecs[i] = v

    sims = [_cosine_similarity(query_vec, v) for v in chunk_vecs]

    # Select top chunks.
    k = max(1, int(max_chunks))
    if cfg.use_mmr and len(candidates) > 1:
        selected_local = _mmr_select(
            query_vec, chunk_vecs, sims, k=k, lambda_mult=float(cfg.mmr_lambda or 0.7)
        )
        selected = [candidates[i] for i in selected_local]
    else:
        order = sorted(range(len(candidates)), key=lambda i: sims[i], reverse=True)
        selected = [candidates[i] for i in order[:k]]

    return selected


def _relevant_chunks(text: str, query: str, *, chunk_chars: int, max_chunks: int, cfg: WebpageReaderConfig,
                    cache_entry: Optional[Dict[str, Any]] = None) -> List[str]:
    mode = (cfg.relevance_mode or "auto").strip().lower()
    if mode not in {"auto", "lexical", "embeddings"}:
        mode = "auto"

    if mode == "lexical":
        return _relevant_chunks_lexical(text, query, chunk_chars=chunk_chars, max_chunks=max_chunks)

    if mode in {"auto", "embeddings"}:
        # Auto: use embeddings when a provider is available, otherwise lexical.
        if mode == "auto" and _embed_provider(cfg) == "none":
            return _relevant_chunks_lexical(text, query, chunk_chars=chunk_chars, max_chunks=max_chunks)
        try:
            return _relevant_chunks_embeddings(
                text,
                query,
                chunk_chars=chunk_chars,
                max_chunks=max_chunks,
                cfg=cfg,
                cache_entry=cache_entry,
            )
        except Exception as e:
            logger.warning(f"Embedding relevance failed; falling back to lexical: {e}")
            return _relevant_chunks_lexical(text, query, chunk_chars=chunk_chars, max_chunks=max_chunks)

    return _relevant_chunks_lexical(text, query, chunk_chars=chunk_chars, max_chunks=max_chunks)


def _is_tls_fetch_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "ssl",
            "certificate",
            "tls",
            "handshake",
            "protocol_error",
            "http/2",
        )
    )


def _as_http_fallback_url(url: str) -> str:
    parsed = urlparse(url or "")
    if parsed.scheme.lower() != "https":
        return url
    return urlunparse(("http", parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


_BLOCKED_TEXT_MARKERS_HIGH_CONFIDENCE = (
    "one moment, please",
    "please wait while your request is being verified",
    "checking your browser before accessing",
    "enable javascript and cookies",
    "verify you are human",
    "attention required",
    "ddos protection",
)

_BLOCKED_TEXT_MARKERS_LOW_CONFIDENCE = (
    "access denied",
    "forbidden",
    "captcha",
    "cloudflare",
    "unusual traffic",
    "temporarily blocked",
    "blocked",
    "rate limit",
    "too many requests",
    "bot detection",
    "security check",
)

_BLOCKED_TITLE_MARKERS = (
    "one moment, please",
    "attention required",
    "security check",
    "just a moment",
)


def _decode_bytes_for_probe(raw_bytes: bytes) -> str:
    if not raw_bytes:
        return ""
    return raw_bytes.decode("utf-8", errors="replace").replace("\x00", "")


def _extract_title_text(html_text: str) -> str:
    if not html_text:
        return ""
    m = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    title = re.sub(r"\s+", " ", m.group(1) or "")
    return title.strip()


def _marker_hits(text: str, markers: Tuple[str, ...]) -> List[str]:
    if not text:
        return []
    hits = []
    for marker in markers:
        if marker and marker in text:
            hits.append(marker)
    return hits


def _detect_blocked_response(
    status_code: Optional[int],
    body_text: str,
    *,
    title_text: str = "",
) -> Tuple[bool, Optional[str], List[str]]:
    status = int(status_code or 0)
    text_norm = (body_text or "").lower()
    title_norm = (title_text or "").lower()

    high_hits = _marker_hits(text_norm, _BLOCKED_TEXT_MARKERS_HIGH_CONFIDENCE)
    low_hits = _marker_hits(text_norm, _BLOCKED_TEXT_MARKERS_LOW_CONFIDENCE)
    title_hits = _marker_hits(title_norm, _BLOCKED_TITLE_MARKERS)
    marker_hits = list(dict.fromkeys(high_hits + low_hits + [f"title:{m}" for m in title_hits]))

    has_marker = bool(marker_hits)
    if status in {403, 429} and has_marker:
        return True, f"http_{status}_bot_marker", marker_hits
    if status in {403, 429}:
        return True, f"http_{status}", marker_hits
    if status == 200:
        if title_hits:
            return True, "http_200_challenge_title", marker_hits
        if high_hits:
            return True, "http_200_bot_marker", marker_hits
        if len(set(low_hits)) >= 2:
            return True, "http_200_bot_marker", marker_hits
        return False, None, marker_hits
    return False, None, marker_hits


def _extract_pdf_text_from_bytes(pdf_bytes: bytes, *, cfg: WebpageReaderConfig) -> Dict[str, Any]:
    if not pdf_bytes:
        return {"ok": False, "error": "pdf_unsupported_or_empty"}

    pdftotext_bin = shutil.which("pdftotext")
    if not pdftotext_bin:
        return {"ok": False, "error": "pdftotext_missing"}

    max_pages = max(1, int(cfg.pdf_max_pages or 8))
    max_text_chars = max(500, int(cfg.pdf_max_text_chars or 40_000))
    timeout = max(1.0, float(cfg.pdf_timeout or 20.0))

    with tempfile.TemporaryDirectory(prefix="asa_pdf_") as tmp_dir:
        pdf_path = os.path.join(tmp_dir, "input.pdf")
        txt_path = os.path.join(tmp_dir, "output.txt")
        with open(pdf_path, "wb") as file_handle:
            file_handle.write(pdf_bytes)

        cmd = [
            pdftotext_bin,
            "-q",
            "-enc",
            "UTF-8",
            "-f",
            "1",
            "-l",
            str(max_pages),
            pdf_path,
            txt_path,
        ]

        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "pdf_timeout"}
        except Exception as exc:
            return {"ok": False, "error": "pdf_parse_failed", "error_detail": str(exc)}

        if int(proc.returncode or 0) != 0:
            stderr_text = _decode_bytes_for_probe(proc.stderr or b"").strip()
            return {
                "ok": False,
                "error": "pdf_parse_failed",
                "error_detail": stderr_text or f"returncode={proc.returncode}",
            }

        try:
            with open(txt_path, "r", encoding="utf-8", errors="replace") as file_handle:
                text = file_handle.read().replace("\x00", "")
        except Exception as exc:
            return {"ok": False, "error": "pdf_parse_failed", "error_detail": str(exc)}

        text = re.sub(r"\s+\n", "\n", text or "")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()
        if not text:
            return {"ok": False, "error": "pdf_unsupported_or_empty"}
        if len(text) > max_text_chars:
            text = text[:max_text_chars]
        return {"ok": True, "text": text}


def _allow_http_fallback(url: str, exc: Exception) -> bool:
    if not _is_tls_fetch_error(exc):
        return False
    parsed = urlparse(url or "")
    if (parsed.scheme or "").lower() != "https":
        return False
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return False
    return (
        host.endswith(".gov")
        or ".gov." in host
        or host.endswith(".gob")
        or ".gob." in host
    )


def _fetch_html(url: str, *, proxy: Optional[str], cfg: WebpageReaderConfig) -> Dict[str, Any]:
    headers = {"User-Agent": cfg.user_agent}
    proxies = {"http": proxy, "https": proxy} if proxy else None
    def _download(target_url: str) -> Dict[str, Any]:
        r = None
        try:
            r = requests.get(
                target_url,
                headers=headers,
                timeout=cfg.timeout,
                proxies=proxies,
                stream=True,
                allow_redirects=True,
            )
            status_code = int(getattr(r, "status_code", 0) or 0)
            content_type = (r.headers.get("Content-Type") or "").lower()
            final_url = str(getattr(r, "url", target_url))

            if status_code >= 400:
                probe_cap = max(1024, int(cfg.blocked_probe_bytes or 65_536))
                probe = bytearray()
                for chunk in r.iter_content(chunk_size=8 * 1024):
                    if not chunk:
                        continue
                    probe.extend(chunk)
                    if len(probe) >= probe_cap:
                        break
                probe_text = _decode_bytes_for_probe(bytes(probe))
                probe_title = _extract_title_text(probe_text)
                is_blocked, blocked_reason, marker_hits = _detect_blocked_response(
                    status_code,
                    probe_text,
                    title_text=probe_title,
                )
                return {
                    "ok": False,
                    "error": "http_error",
                    "status_code": status_code,
                    "content_type": content_type,
                    "final_url": final_url,
                    "is_blocked": bool(is_blocked),
                    "blocked_reason": blocked_reason,
                    "bytes_probed": int(len(probe)),
                    "marker_hits": marker_hits,
                }

            allow_pdf = bool(cfg.pdf_enabled)
            is_pdf = ("application/pdf" in content_type) or final_url.lower().endswith(".pdf")
            if is_pdf and not allow_pdf:
                return {
                    "ok": False,
                    "error": "unsupported_content_type",
                    "content_type": content_type or "application/pdf",
                    "final_url": final_url,
                }

            if is_pdf and allow_pdf:
                pdf_max_bytes = max(16 * 1024, int(cfg.pdf_max_bytes or 8_000_000))
                data = bytearray()
                for chunk in r.iter_content(chunk_size=16 * 1024):
                    if not chunk:
                        continue
                    data.extend(chunk)
                    if len(data) >= pdf_max_bytes:
                        break
                parsed_pdf = _extract_pdf_text_from_bytes(bytes(data), cfg=cfg)
                if not parsed_pdf.get("ok"):
                    return {
                        "ok": False,
                        "error": parsed_pdf.get("error", "pdf_parse_failed"),
                        "content_type": content_type or "application/pdf",
                        "final_url": final_url,
                        "error_detail": parsed_pdf.get("error_detail"),
                    }
                return {
                    "ok": True,
                    "source_type": "pdf",
                    "title": "",
                    "text": parsed_pdf.get("text", ""),
                    "content_type": content_type or "application/pdf",
                    "final_url": final_url,
                    "bytes_read": int(len(data)),
                }

            if not (
                "text/html" in content_type
                or "application/xhtml+xml" in content_type
                or "text/plain" in content_type
                or content_type == ""
            ):
                return {
                    "ok": False,
                    "error": "unsupported_content_type",
                    "content_type": content_type,
                    "final_url": final_url,
                }

            data = bytearray()
            for chunk in r.iter_content(chunk_size=16 * 1024):
                if not chunk:
                    continue
                data.extend(chunk)
                if len(data) >= cfg.max_bytes:
                    break

            if bool(cfg.blocked_detect_on_200):
                body_scan_cap = max(1024, int(cfg.blocked_body_scan_bytes or 32_768))
                probe_bytes = bytes(data[:body_scan_cap])
                probe_text = _decode_bytes_for_probe(probe_bytes)
                probe_title = _extract_title_text(probe_text)
                is_blocked, blocked_reason, marker_hits = _detect_blocked_response(
                    200,
                    probe_text,
                    title_text=probe_title,
                )
                if is_blocked:
                    return {
                        "ok": False,
                        "error": "http_error",
                        "status_code": status_code,
                        "content_type": content_type,
                        "final_url": final_url,
                        "is_blocked": True,
                        "blocked_reason": blocked_reason,
                        "bytes_probed": int(len(probe_bytes)),
                        "marker_hits": marker_hits,
                    }

            # Best-effort decode
            encoding = r.encoding or "utf-8"
            html = data.decode(encoding, errors="replace").replace("\x00", "")
            return {
                "ok": True,
                "source_type": "html",
                "html": html,
                "content_type": content_type,
                "final_url": final_url,
                "bytes_read": int(len(data)),
            }
        finally:
            if r is not None:
                try:
                    r.close()
                except Exception:
                    pass

    try:
        return _download(url)
    except RequestException as primary_exc:
        if _allow_http_fallback(url, primary_exc):
            fallback_url = _as_http_fallback_url(url)
            if fallback_url != url:
                try:
                    fetched = _download(fallback_url)
                    if fetched.get("ok"):
                        fetched["fallback_note"] = "https_failed_retry_http"
                        fetched["requested_url"] = url
                    return fetched
                except RequestException as fallback_exc:
                    logger.warning(
                        "OpenWebpage HTTP fallback failed for %s after TLS error (%s): %s",
                        url,
                        primary_exc,
                        fallback_exc,
                    )
                    raise fallback_exc from primary_exc
        raise


def _is_retryable_http_status(status_code: Any) -> bool:
    try:
        code = int(status_code)
    except Exception:
        return False
    if code in (408, 425, 429, 500, 502, 503, 504):
        return True
    if 520 <= code < 600:
        return True
    return False


def _classify_request_exception(exc: Exception) -> Tuple[str, bool]:
    text = str(exc or "").strip().lower()
    if not text:
        return "request_exception", True

    if "curl" in text and "(28)" in text:
        return "timeout", True
    if "timeout" in text or "timed out" in text:
        return "timeout", True
    if "connection reset" in text or "connection aborted" in text or "connection error" in text:
        return "connection_error", True
    if "ssl" in text or "tls" in text:
        return "tls_error", True
    if "name or service not known" in text or "dns" in text:
        return "dns_error", True

    return "request_exception", True


def _format_openwebpage_error_payload(
    *,
    error_type: str,
    retryable: bool,
    url: str,
    final_url: Optional[str] = None,
    status_code: Optional[int] = None,
    error: Optional[str] = None,
    error_detail: Optional[str] = None,
    attempt: Optional[int] = None,
    attempts: Optional[int] = None,
) -> str:
    payload: Dict[str, Any] = {
        "ok": False,
        "tool": "OpenWebpage",
        "error_type": str(error_type or "error"),
        "retryable": bool(retryable),
        "url": str(url or ""),
        "final_url": final_url,
        "status_code": int(status_code) if status_code is not None else None,
        "error": str(error or "")[:800] if error else None,
        "error_detail": str(error_detail or "")[:1200] if error_detail else None,
        "attempt": int(attempt) if attempt is not None else None,
        "attempts": int(attempts) if attempts is not None else None,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class OpenWebpageInput(BaseModel):
    url: str = Field(..., description="Public http(s) URL to open.")
    query: Optional[str] = Field(
        default=None,
        description="Optional relevance query; when provided, returns only the most relevant excerpts.",
    )


class OpenWebpageTool(BaseTool):
    """Tool that fetches and extracts readable text from a webpage."""

    name: str = "OpenWebpage"
    description: str = (
        "Open and read a public webpage (HTML/text) at a given URL. "
        "Provide an optional 'query' to get only the most relevant excerpts. "
        "This tool is disabled unless allow_read_webpages is enabled."
    )
    args_schema: Type[BaseModel] = OpenWebpageInput

    def __init__(self, proxy: Optional[str] = None, **kwargs: Any):
        super().__init__(**kwargs)
        self._proxy = proxy

    def _run(self, url: str, query: Optional[str] = None) -> str:
        cfg = configure_webpage_reader()

        if not cfg.allow_read_webpages:
            return (
                "Webpage reading is disabled (allow_read_webpages=FALSE). "
                "Enable it explicitly to use this tool."
            )

        ok, reason, norm = _validate_url(url)
        if not ok:
            return f"Refusing to open URL ({reason}): {url}"

        try:
            chunk_chars = max(200, int(cfg.chunk_chars))
            max_chunks = max(1, int(cfg.max_chunks))
            query_text = str(query or "").strip()

            # Prevent runaway prompt growth from very large pages while still
            # preserving focused evidence excerpts for the agent.
            if query_text:
                max_chunks = min(max_chunks, 6)
            else:
                max_chunks = 1
                chunk_chars = min(chunk_chars, 1800)

            cache_hit = False
            cache_entry = None

            cache_key = _normalize_url_for_cache(norm)
            if cfg.cache_enabled:
                cache_entry = _cache_get(cache_key)
                if cache_entry is not None:
                    cache_hit = True

            blocked_entry = None
            if cache_entry is None and cfg.cache_enabled:
                blocked_entry = _blocked_cache_get(cache_key)
            if blocked_entry is not None:
                final_url = blocked_entry.get("final_url") or norm
                status_code = blocked_entry.get("status_code")
                blocked_reason = blocked_entry.get("blocked_reason") or "blocked"
                marker_hits = blocked_entry.get("marker_hits") or []
                return (
                    f"URL: {norm}\n"
                    f"Final URL: {final_url}\n"
                    "Title: (unavailable)\n"
                    "Bytes read: 0\n"
                    "Cache: HIT_BLOCKED\n\n"
                    f"Blocked fetch cached for this URL.\n"
                    f"Reason: {blocked_reason}"
                    + (f" (status={status_code})" if status_code is not None else "")
                    + (
                        f"\nMarkers: {', '.join(str(x) for x in marker_hits[:4])}"
                        if marker_hits
                        else ""
                    )
                )

            if cache_entry is None:
                fetch_attempts = 2
                base_timeout = float(cfg.timeout or 20.0)
                backoff_schedule = (0.5, 1.5)
                fetched = None

                for attempt_idx in range(1, fetch_attempts + 1):
                    attempt_timeout = base_timeout
                    if attempt_idx == fetch_attempts:
                        attempt_timeout = max(1.0, base_timeout * 1.5)
                    attempt_cfg = cfg if attempt_timeout == cfg.timeout else replace(cfg, timeout=attempt_timeout)

                    try:
                        fetched = _fetch_html(norm, proxy=self._proxy, cfg=attempt_cfg)
                    except RequestException as exc:
                        error_type, retryable = _classify_request_exception(exc)
                        if retryable and attempt_idx < fetch_attempts:
                            time.sleep(backoff_schedule[min(attempt_idx - 1, len(backoff_schedule) - 1)])
                            continue
                        return _format_openwebpage_error_payload(
                            error_type=error_type,
                            retryable=retryable,
                            url=norm,
                            final_url=None,
                            status_code=None,
                            error=str(exc),
                            attempt=attempt_idx,
                            attempts=fetch_attempts,
                        )

                    if fetched.get("ok"):
                        break

                    if bool(fetched.get("is_blocked")):
                        final_url = fetched.get("final_url") or norm
                        final_key = _normalize_url_for_cache(final_url)
                        block_entry = {
                            "url": norm,
                            "final_url": final_url,
                            "status_code": fetched.get("status_code"),
                            "blocked_reason": fetched.get("blocked_reason") or "blocked",
                            "marker_hits": list(fetched.get("marker_hits") or []),
                            "fetched_at": time.time(),
                        }
                        _blocked_cache_set(final_key, block_entry, cfg=cfg)
                        if final_key != cache_key:
                            _blocked_cache_alias(cache_key, final_key)
                        return (
                            f"URL: {norm}\n"
                            f"Final URL: {final_url}\n"
                            "Title: (unavailable)\n"
                            "Bytes read: 0\n"
                            "Cache: MISS\n\n"
                            "Blocked fetch detected.\n"
                            f"Reason: {block_entry['blocked_reason']}"
                            + (
                                f" (status={block_entry.get('status_code')})"
                                if block_entry.get("status_code") is not None
                                else ""
                            )
                            + (
                                f"\nMarkers: {', '.join(str(x) for x in block_entry.get('marker_hits', [])[:4])}"
                                if block_entry.get("marker_hits")
                                else ""
                            )
                        )

                    status_code = fetched.get("status_code")
                    retryable = bool(fetched.get("error") == "http_error" and _is_retryable_http_status(status_code))
                    if retryable and attempt_idx < fetch_attempts:
                        time.sleep(backoff_schedule[min(attempt_idx - 1, len(backoff_schedule) - 1)])
                        continue

                    return _format_openwebpage_error_payload(
                        error_type=str(fetched.get("error") or "fetch_failed"),
                        retryable=retryable,
                        url=norm,
                        final_url=fetched.get("final_url"),
                        status_code=status_code if status_code is not None else None,
                        error=str(fetched.get("error") or ""),
                        error_detail=fetched.get("error_detail"),
                        attempt=attempt_idx,
                        attempts=fetch_attempts,
                    )

                source_type = str(fetched.get("source_type") or "html")
                if source_type == "pdf":
                    title = fetched.get("title") or ""
                    text = str(fetched.get("text") or "")
                else:
                    title, text = _extract_main_text(
                        fetched.get("html", ""),
                        page_url=fetched.get("final_url") or norm,
                        cfg=cfg,
                    )
                # Avoid caching huge pages unbounded.
                max_text = int(cfg.cache_max_text_chars or 200_000)
                if max_text > 0 and len(text) > max_text:
                    text = text[:max_text]

                final_url = fetched.get("final_url") or norm
                final_key = _normalize_url_for_cache(final_url)
                cache_entry = {
                    "url": norm,
                    "final_url": final_url,
                    "title": title,
                    "text": text,
                    "bytes_read": fetched.get("bytes_read"),
                    "content_type": fetched.get("content_type"),
                    "source_type": source_type,
                    "fetched_at": time.time(),
                }
                if cfg.cache_enabled:
                    _cache_set(final_key, cache_entry, cfg=cfg)
                    if final_key != cache_key:
                        _cache_alias(cache_key, final_key)

            title = cache_entry.get("title") or ""
            text = cache_entry.get("text") or ""
            final_url = cache_entry.get("final_url") or norm

            chunks = _relevant_chunks(
                text,
                query_text,
                chunk_chars=chunk_chars,
                max_chunks=max_chunks,
                cfg=cfg,
                cache_entry=cache_entry if cfg.cache_enabled else None,
            )

            header = [
                f"URL: {norm}",
                f"Final URL: {final_url}",
                f"Title: {title}" if title else "Title: (unknown)",
                f"Bytes read: {cache_entry.get('bytes_read')}",
                f"Cache: {'HIT' if cache_hit else 'MISS'}",
            ]
            source_type = str(cache_entry.get("source_type") or "")
            if source_type == "pdf":
                header.append("Source-Type: pdf")
            fallback_note = cache_entry.get("fallback_note")
            if isinstance(fallback_note, str) and fallback_note.strip():
                header.append(f"Fetch fallback: {fallback_note}")
            out = "\n".join(header) + "\n\n"

            if chunks:
                out += "Relevant excerpts:\n"
                for i, ch in enumerate(chunks, start=1):
                    out += f"\n[{i}]\n{ch}\n"
            else:
                out += "No readable text extracted."

            # Enforce output size cap.
            max_chars = max(500, int(cfg.max_chars))
            if query_text:
                max_chars = min(max_chars, 12_000)
            else:
                max_chars = min(max_chars, 6_000)
            if len(out) > max_chars:
                out = out[: max_chars] + "\n\n[Truncated]"
            return out

        except RequestException as e:
            error_type, retryable = _classify_request_exception(e)
            return _format_openwebpage_error_payload(
                error_type=error_type,
                retryable=retryable,
                url=norm,
                final_url=None,
                status_code=None,
                error=str(e),
            )
        except Exception as e:
            logger.exception("OpenWebpageTool error")
            return _format_openwebpage_error_payload(
                error_type=type(e).__name__,
                retryable=False,
                url=norm,
                final_url=None,
                status_code=None,
                error=str(e),
            )

    async def _arun(self, url: str, query: Optional[str] = None) -> str:  # pragma: no cover
        # Keep synchronous implementation for now.
        return self._run(url=url, query=query)


def create_webpage_reader_tool(proxy: Optional[str] = None) -> OpenWebpageTool:
    """Factory for R/reticulate to create the tool with a configured proxy."""
    return OpenWebpageTool(proxy=proxy)


__all__ = [
    "WebpageReaderConfig",
    "configure_webpage_reader",
    "clear_webpage_reader_cache",
    "OpenWebpageTool",
    "create_webpage_reader_tool",
]
