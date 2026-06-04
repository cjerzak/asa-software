"""Shared Azure OpenAI rate limiting helpers.

The limiter coordinates local ASA worker processes through a small SQLite
database. It is intentionally standard-library only so it can run inside both
the LangGraph backend and the free-code gateway.
"""

from __future__ import annotations

import asyncio
import os
import random
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse


def _env_bool(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return bool(default)
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _env_float(name: str, default: float, minimum: Optional[float] = None) -> float:
    try:
        value = float(os.getenv(name, ""))
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    return value


def _env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    try:
        value = int(float(os.getenv(name, "")))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    return value


def _default_db_path() -> str:
    return os.path.join(tempfile.gettempdir(), "asa_azure_rate_limit.sqlite")


def normalize_azure_openai_base_url(endpoint: Optional[str]) -> str:
    """Normalize Azure OpenAI endpoints to the v1 OpenAI-compatible base URL."""
    base_url = str(endpoint or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_BASE is required")
    if "api.openai.com" in base_url.lower():
        raise ValueError("Azure backend cannot use api.openai.com")
    if not base_url.lower().startswith("https://") and not _env_bool(
        "ASA_ALLOW_INSECURE_AZURE_OPENAI_ENDPOINT",
        False,
    ):
        raise ValueError("Azure OpenAI endpoint must use https://")
    if base_url.lower().endswith("/openai/v1"):
        return base_url
    if base_url.lower().endswith("/openai"):
        return f"{base_url}/v1"
    return f"{base_url}/openai/v1"


def _host_from_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    return (parsed.hostname or endpoint).lower().rstrip(".")


def _limiter_key(endpoint: str, deployment: str, scope: str) -> Tuple[str, str]:
    host = _host_from_endpoint(endpoint)
    deployment_token = str(deployment or "").strip()
    scope_token = str(scope or "").strip()
    key = "|".join((host, deployment_token, scope_token))
    return key, host


def _coerce_header_map(headers: Any) -> Dict[str, str]:
    if not headers:
        return {}
    if isinstance(headers, dict):
        iterable = headers.items()
    else:
        try:
            iterable = headers.items()
        except Exception:
            return {}
    out: Dict[str, str] = {}
    for key, value in iterable:
        key_s = str(key or "").strip().lower()
        if key_s:
            out[key_s] = str(value or "").strip()
    return out


def _headers_from_response(response: Any) -> Dict[str, str]:
    candidates = []
    try:
        candidates.append(getattr(response, "response_metadata", None))
    except Exception:
        pass
    if isinstance(response, dict):
        candidates.append(response.get("response_metadata"))
    for meta in candidates:
        if not isinstance(meta, dict):
            continue
        for key in ("headers", "response_headers", "http_response_headers"):
            headers = _coerce_header_map(meta.get(key))
            if headers:
                return headers
    return {}


def _headers_from_exception(exc: BaseException) -> Dict[str, str]:
    candidates = []
    for attr in ("headers", "response"):
        try:
            value = getattr(exc, attr, None)
        except Exception:
            value = None
        if value is None:
            continue
        if attr == "response":
            try:
                candidates.append(getattr(value, "headers", None))
            except Exception:
                pass
        else:
            candidates.append(value)
    for candidate in candidates:
        headers = _coerce_header_map(candidate)
        if headers:
            return headers
    return {}


def _parse_float_header(headers: Dict[str, str], name: str) -> Optional[float]:
    try:
        value = headers.get(name.lower())
        if value is None or str(value).strip() == "":
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _retry_after_seconds(headers: Dict[str, str]) -> float:
    retry_after_ms = _parse_float_header(headers, "retry-after-ms")
    if retry_after_ms is not None and retry_after_ms > 0:
        return max(0.0, retry_after_ms / 1000.0)
    retry_after = _parse_float_header(headers, "retry-after")
    if retry_after is not None and retry_after > 0:
        return max(0.0, retry_after)
    return 0.0


class AzureOpenAISharedRateLimiter:
    """SQLite-backed deployment-level rate limiter for Azure OpenAI."""

    def __init__(
        self,
        *,
        endpoint: str,
        deployment: str,
        rpm: Optional[float] = None,
        tpm: Optional[float] = None,
        max_concurrent_requests: Optional[int] = None,
        estimated_tokens_per_call: Optional[int] = None,
        db_path: Optional[str] = None,
        scope: Optional[str] = None,
        mode: Optional[str] = None,
        lease_ttl_seconds: Optional[float] = None,
    ) -> None:
        self.endpoint = normalize_azure_openai_base_url(endpoint)
        self.deployment = str(deployment or "").strip()
        if not self.deployment:
            raise ValueError("Azure OpenAI deployment/model name is required")
        self.scope = str(scope if scope is not None else os.getenv("ASA_AZURE_RATE_LIMIT_SCOPE", "")).strip()
        self.key, self.endpoint_host = _limiter_key(self.endpoint, self.deployment, self.scope)
        os.environ["ASA_MAIN_BACKEND"] = "azure-openai"
        os.environ["ASA_AZURE_OPENAI_ACTIVE_BASE_URL"] = self.endpoint
        os.environ["ASA_AZURE_OPENAI_ACTIVE_DEPLOYMENT"] = self.deployment
        self.mode = str(mode or os.getenv("ASA_AZURE_RATE_LIMIT_MODE", "shared")).strip().lower()
        if self.mode not in {"shared", "local", "off"}:
            self.mode = "shared"
        self.rpm = _env_float("ASA_AZURE_RPM", float(rpm if rpm is not None else 60.0), minimum=1e-6)
        self.tpm = _env_float("ASA_AZURE_TPM", float(tpm if tpm is not None else 60000.0), minimum=1.0)
        self.max_concurrent_requests = _env_int(
            "ASA_AZURE_MAX_CONCURRENT_REQUESTS",
            int(max_concurrent_requests if max_concurrent_requests is not None else 4),
            minimum=1,
        )
        self.estimated_tokens_per_call = _env_int(
            "ASA_AZURE_ESTIMATED_TOKENS_PER_CALL",
            int(estimated_tokens_per_call if estimated_tokens_per_call is not None else 2000),
            minimum=1,
        )
        self.lease_ttl_seconds = _env_float(
            "ASA_AZURE_CONCURRENCY_LEASE_SECONDS",
            float(lease_ttl_seconds if lease_ttl_seconds is not None else 300.0),
            minimum=1.0,
        )
        self.db_path = str(db_path or os.getenv("ASA_AZURE_RATE_LIMIT_DB", "") or _default_db_path())
        self.busy_timeout_s = _env_float("ASA_AZURE_SQLITE_BUSY_TIMEOUT", 5.0, minimum=0.1)
        self.poll_seconds = _env_float("ASA_AZURE_RATE_LIMIT_POLL_SECONDS", 0.10, minimum=0.01)
        self.max_wait_seconds = _env_float("ASA_AZURE_RATE_LIMIT_MAX_WAIT_SECONDS", 3600.0, minimum=1.0)
        self.last_wait_seconds = 0.0
        self.last_lease_id: Optional[str] = None
        self._local_lock = threading.Lock()
        self._local_requests: Deque[float] = deque()
        self._local_tokens: Deque[Tuple[float, int]] = deque()
        self._local_leases: Dict[str, float] = {}
        if self.mode == "shared":
            self._init_db()

    def acquire(self, blocking: bool = True) -> bool:
        lease_id = self.acquire_lease(blocking=blocking)
        if lease_id:
            self.last_lease_id = lease_id
            return True
        return False

    async def aacquire(self, blocking: bool = True) -> bool:
        return await asyncio.to_thread(self.acquire, blocking=blocking)

    def acquire_lease(self, blocking: bool = True) -> Optional[str]:
        if self.mode == "off":
            self.last_wait_seconds = 0.0
            return f"off-{uuid.uuid4().hex}"
        started = time.monotonic()
        while True:
            if self.mode == "local":
                lease_id, wait_for = self._try_acquire_local()
            else:
                lease_id, wait_for = self._try_acquire_shared()
            if lease_id:
                self.last_wait_seconds = max(0.0, time.monotonic() - started)
                self.last_lease_id = lease_id
                return lease_id
            if not blocking:
                self.last_wait_seconds = max(0.0, time.monotonic() - started)
                return None
            elapsed = time.monotonic() - started
            if elapsed >= self.max_wait_seconds:
                self.last_wait_seconds = max(0.0, elapsed)
                raise TimeoutError(
                    f"Timed out waiting for Azure OpenAI limiter after {elapsed:.1f}s "
                    f"for deployment {self.deployment}"
                )
            sleep_for = max(self.poll_seconds, min(float(wait_for or self.poll_seconds), 5.0))
            sleep_for *= random.uniform(0.90, 1.10)
            time.sleep(max(self.poll_seconds, sleep_for))

    def release(self, lease_id: Optional[str] = None) -> None:
        lease = str(lease_id or self.last_lease_id or "").strip()
        if not lease or self.mode == "off":
            return
        if self.mode == "local":
            with self._local_lock:
                self._local_leases.pop(lease, None)
            return
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM azure_rate_leases WHERE lease_id = ?", (lease,))
        except Exception:
            pass

    def record_response(self, response: Any) -> None:
        headers = _headers_from_response(response)
        if headers:
            self.record_response_headers(headers)
        self._attach_response_metadata(response)

    def record_response_headers(self, headers: Any) -> None:
        header_map = _coerce_header_map(headers)
        if not header_map:
            return
        retry_after = _retry_after_seconds(header_map)
        if retry_after > 0:
            self.record_backoff(retry_after)
        if self.mode != "shared":
            return
        values = {
            "limit_requests": _parse_float_header(header_map, "x-ratelimit-limit-requests"),
            "remaining_requests": _parse_float_header(header_map, "x-ratelimit-remaining-requests"),
            "limit_tokens": _parse_float_header(header_map, "x-ratelimit-limit-tokens"),
            "remaining_tokens": _parse_float_header(header_map, "x-ratelimit-remaining-tokens"),
            "retry_after_ms": _parse_float_header(header_map, "retry-after-ms"),
        }
        try:
            with self._transaction() as conn:
                self._ensure_row(conn, time.time())
                conn.execute(
                    """
                    UPDATE azure_rate_limits
                    SET limit_requests = COALESCE(?, limit_requests),
                        remaining_requests = COALESCE(?, remaining_requests),
                        limit_tokens = COALESCE(?, limit_tokens),
                        remaining_tokens = COALESCE(?, remaining_tokens),
                        retry_after_ms = COALESCE(?, retry_after_ms),
                        updated_at = ?
                    WHERE key = ?
                    """,
                    (
                        values["limit_requests"],
                        values["remaining_requests"],
                        values["limit_tokens"],
                        values["remaining_tokens"],
                        values["retry_after_ms"],
                        time.time(),
                        self.key,
                    ),
                )
        except Exception:
            pass

    def record_exception(self, exc: BaseException) -> None:
        headers = _headers_from_exception(exc)
        retry_after = _retry_after_seconds(headers)
        text = f"{type(exc).__name__}: {exc}".lower()
        if retry_after <= 0 and ("429" in text or "rate limit" in text or "too many requests" in text):
            retry_after = 1.0
        if retry_after > 0:
            self.record_backoff(retry_after)
        if headers:
            self.record_response_headers(headers)

    def record_backoff(self, seconds: float) -> None:
        try:
            seconds_f = max(0.0, float(seconds))
        except Exception:
            seconds_f = 0.0
        if seconds_f <= 0:
            return
        until = time.time() + seconds_f
        if self.mode == "local":
            # Local mode shares no state; sleeping callers will observe this via
            # the regular acquire loop because we keep the value in memory.
            with self._local_lock:
                self._local_backoff_until = until  # type: ignore[attr-defined]
            return
        if self.mode != "shared":
            return
        try:
            with self._transaction() as conn:
                self._ensure_row(conn, time.time())
                conn.execute(
                    """
                    UPDATE azure_rate_limits
                    SET backoff_until = MAX(COALESCE(backoff_until, 0), ?),
                        updated_at = ?
                    WHERE key = ?
                    """,
                    (until, time.time(), self.key),
                )
        except Exception:
            pass

    def snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "azure_rate_limit_mode": self.mode,
            "azure_rate_limit_endpoint_host": self.endpoint_host,
            "azure_rate_limit_deployment": self.deployment,
            "azure_limiter_wait_seconds": float(self.last_wait_seconds or 0.0),
        }
        if self.mode != "shared":
            return out
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT remaining_requests, remaining_tokens, retry_after_ms, backoff_until
                    FROM azure_rate_limits
                    WHERE key = ?
                    """,
                    (self.key,),
                ).fetchone()
            if row:
                if row[0] is not None:
                    out["azure_rate_limit_remaining_requests"] = row[0]
                if row[1] is not None:
                    out["azure_rate_limit_remaining_tokens"] = row[1]
                if row[2] is not None:
                    out["azure_retry_after_ms"] = row[2]
                if row[3] is not None:
                    out["azure_rate_limit_backoff_until"] = row[3]
        except Exception:
            pass
        return out

    def _attach_response_metadata(self, response: Any) -> None:
        if response is None:
            return
        snapshot = self.snapshot()
        try:
            meta = getattr(response, "response_metadata", None)
            if isinstance(meta, dict):
                meta.update(snapshot)
                return
        except Exception:
            pass
        if isinstance(response, dict):
            meta = response.setdefault("response_metadata", {})
            if isinstance(meta, dict):
                meta.update(snapshot)

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(
            self.db_path,
            timeout=float(self.busy_timeout_s),
            isolation_level=None,
        )
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_s * 1000)}")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS azure_rate_limits (
                    key TEXT PRIMARY KEY,
                    endpoint_host TEXT NOT NULL,
                    deployment TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    request_window_start REAL NOT NULL,
                    request_count INTEGER NOT NULL DEFAULT 0,
                    token_window_start REAL NOT NULL,
                    token_count INTEGER NOT NULL DEFAULT 0,
                    backoff_until REAL NOT NULL DEFAULT 0,
                    limit_requests REAL,
                    remaining_requests REAL,
                    limit_tokens REAL,
                    remaining_tokens REAL,
                    retry_after_ms REAL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS azure_rate_leases (
                    key TEXT NOT NULL,
                    lease_id TEXT PRIMARY KEY,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_azure_rate_leases_key ON azure_rate_leases(key)")

    def _transaction(self) -> sqlite3.Connection:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        return conn

    def _ensure_row(self, conn: sqlite3.Connection, now: float) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO azure_rate_limits (
                key, endpoint_host, deployment, scope,
                request_window_start, request_count,
                token_window_start, token_count,
                backoff_until, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, ?, 0, 0, ?)
            """,
            (self.key, self.endpoint_host, self.deployment, self.scope, now, now, now),
        )

    def _try_acquire_shared(self) -> Tuple[Optional[str], float]:
        now = time.time()
        lease_id = uuid.uuid4().hex
        rpm = float(self.rpm)
        request_window_seconds = 60.0 if rpm >= 1.0 else 60.0 / max(rpm, 1e-6)
        request_limit = int(rpm) if rpm >= 1.0 else 1
        try:
            with self._transaction() as conn:
                self._ensure_row(conn, now)
                conn.execute("DELETE FROM azure_rate_leases WHERE expires_at <= ?", (now,))
                row = conn.execute(
                    """
                    SELECT request_window_start, request_count,
                           token_window_start, token_count,
                           backoff_until
                    FROM azure_rate_limits
                    WHERE key = ?
                    """,
                    (self.key,),
                ).fetchone()
                if row is None:
                    return None, self.poll_seconds
                req_start, req_count, tok_start, tok_count, backoff_until = row
                req_start = float(req_start or now)
                tok_start = float(tok_start or now)
                req_count = int(req_count or 0)
                tok_count = int(tok_count or 0)
                if now - req_start >= request_window_seconds:
                    req_start = now
                    req_count = 0
                if now - tok_start >= 60.0:
                    tok_start = now
                    tok_count = 0

                active_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM azure_rate_leases WHERE key = ? AND expires_at > ?",
                        (self.key, now),
                    ).fetchone()[0]
                )
                wait_for = 0.0
                if float(backoff_until or 0.0) > now:
                    wait_for = max(wait_for, float(backoff_until) - now)
                if active_count >= int(self.max_concurrent_requests):
                    oldest = conn.execute(
                        "SELECT MIN(expires_at) FROM azure_rate_leases WHERE key = ? AND expires_at > ?",
                        (self.key, now),
                    ).fetchone()[0]
                    if oldest is not None:
                        wait_for = max(wait_for, float(oldest) - now)
                    else:
                        wait_for = max(wait_for, self.poll_seconds)
                if req_count >= request_limit:
                    wait_for = max(wait_for, (req_start + request_window_seconds) - now)
                if tok_count + int(self.estimated_tokens_per_call) > float(self.tpm):
                    wait_for = max(wait_for, (tok_start + 60.0) - now)

                if wait_for > 0:
                    conn.execute(
                        """
                        UPDATE azure_rate_limits
                        SET request_window_start = ?, request_count = ?,
                            token_window_start = ?, token_count = ?,
                            updated_at = ?
                        WHERE key = ?
                        """,
                        (req_start, req_count, tok_start, tok_count, now, self.key),
                    )
                    return None, max(self.poll_seconds, wait_for)

                conn.execute(
                    """
                    UPDATE azure_rate_limits
                    SET request_window_start = ?, request_count = ?,
                        token_window_start = ?, token_count = ?,
                        updated_at = ?
                    WHERE key = ?
                    """,
                    (
                        req_start,
                        req_count + 1,
                        tok_start,
                        tok_count + int(self.estimated_tokens_per_call),
                        now,
                        self.key,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO azure_rate_leases(key, lease_id, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (self.key, lease_id, now, now + float(self.lease_ttl_seconds)),
                )
                return lease_id, 0.0
        except sqlite3.OperationalError:
            return None, self.poll_seconds

    def _try_acquire_local(self) -> Tuple[Optional[str], float]:
        now = time.time()
        lease_id = uuid.uuid4().hex
        rpm = float(self.rpm)
        request_window_seconds = 60.0 if rpm >= 1.0 else 60.0 / max(rpm, 1e-6)
        request_limit = int(rpm) if rpm >= 1.0 else 1
        with self._local_lock:
            backoff_until = float(getattr(self, "_local_backoff_until", 0.0) or 0.0)
            while self._local_requests and now - self._local_requests[0] >= request_window_seconds:
                self._local_requests.popleft()
            while self._local_tokens and now - self._local_tokens[0][0] >= 60.0:
                self._local_tokens.popleft()
            for key, expires_at in list(self._local_leases.items()):
                if expires_at <= now:
                    self._local_leases.pop(key, None)

            wait_for = 0.0
            if backoff_until > now:
                wait_for = max(wait_for, backoff_until - now)
            if len(self._local_leases) >= int(self.max_concurrent_requests):
                wait_for = max(wait_for, min(self._local_leases.values()) - now)
            if len(self._local_requests) >= request_limit:
                wait_for = max(wait_for, (self._local_requests[0] + request_window_seconds) - now)
            token_count = sum(tokens for _, tokens in self._local_tokens)
            if token_count + int(self.estimated_tokens_per_call) > float(self.tpm):
                wait_for = max(wait_for, (self._local_tokens[0][0] + 60.0) - now if self._local_tokens else 60.0)
            if wait_for > 0:
                return None, max(self.poll_seconds, wait_for)

            self._local_requests.append(now)
            self._local_tokens.append((now, int(self.estimated_tokens_per_call)))
            self._local_leases[lease_id] = now + float(self.lease_ttl_seconds)
            return lease_id, 0.0


class AzureRateLimitedModel:
    """Small proxy that acquires/releases a limiter around model invocations."""

    asa_backend = "azure-openai"
    _asa_backend = "azure-openai"

    def __init__(self, model: Any, limiter: AzureOpenAISharedRateLimiter) -> None:
        self._model = model
        self._asa_limiter = limiter

    def __getattr__(self, name: str) -> Any:
        return getattr(self._model, name)

    def bind(self, *args: Any, **kwargs: Any) -> "AzureRateLimitedModel":
        return AzureRateLimitedModel(self._model.bind(*args, **kwargs), self._asa_limiter)

    def bind_tools(self, *args: Any, **kwargs: Any) -> "AzureRateLimitedModel":
        return AzureRateLimitedModel(self._model.bind_tools(*args, **kwargs), self._asa_limiter)

    def with_structured_output(self, *args: Any, **kwargs: Any) -> "AzureRateLimitedModel":
        return AzureRateLimitedModel(self._model.with_structured_output(*args, **kwargs), self._asa_limiter)

    def invoke(self, *args: Any, **kwargs: Any) -> Any:
        lease_id = self._asa_limiter.acquire_lease(blocking=True)
        try:
            response = self._model.invoke(*args, **kwargs)
            self._asa_limiter.record_response(response)
            return response
        except Exception as exc:
            self._asa_limiter.record_exception(exc)
            raise
        finally:
            self._asa_limiter.release(lease_id)

    async def ainvoke(self, *args: Any, **kwargs: Any) -> Any:
        lease_id = await asyncio.to_thread(self._asa_limiter.acquire_lease, True)
        try:
            ainvoke = getattr(self._model, "ainvoke", None)
            if callable(ainvoke):
                response = await ainvoke(*args, **kwargs)
            else:
                response = await asyncio.to_thread(self._model.invoke, *args, **kwargs)
            self._asa_limiter.record_response(response)
            return response
        except Exception as exc:
            self._asa_limiter.record_exception(exc)
            raise
        finally:
            self._asa_limiter.release(lease_id)

    def stream(self, *args: Any, **kwargs: Any) -> Iterable[Any]:
        lease_id = self._asa_limiter.acquire_lease(blocking=True)
        try:
            for chunk in self._model.stream(*args, **kwargs):
                self._asa_limiter.record_response(chunk)
                yield chunk
        except Exception as exc:
            self._asa_limiter.record_exception(exc)
            raise
        finally:
            self._asa_limiter.release(lease_id)


def wrap_chat_model(model: Any, limiter: AzureOpenAISharedRateLimiter) -> AzureRateLimitedModel:
    return AzureRateLimitedModel(model, limiter)


def active_limiter_snapshot() -> Dict[str, Any]:
    if str(os.getenv("ASA_MAIN_BACKEND", "")).strip().lower() != "azure-openai":
        return {}
    endpoint = os.getenv("ASA_AZURE_OPENAI_ACTIVE_BASE_URL") or os.getenv("AZURE_OPENAI_API_BASE") or os.getenv("AZURE_OPENAI_ENDPOINT")
    deployment = os.getenv("ASA_AZURE_OPENAI_ACTIVE_DEPLOYMENT") or os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not endpoint or not deployment:
        return {}
    try:
        limiter = AzureOpenAISharedRateLimiter(endpoint=endpoint, deployment=deployment)
        return limiter.snapshot()
    except Exception:
        return {}


def merge_azure_diagnostics(diagnostics: Any, state: Any = None) -> Dict[str, Any]:
    out = dict(diagnostics) if isinstance(diagnostics, dict) else {}
    snapshot = active_limiter_snapshot()
    if snapshot:
        out.update(snapshot)
    if isinstance(state, dict):
        for message in list(state.get("messages") or []):
            try:
                meta = getattr(message, "response_metadata", None)
            except Exception:
                meta = None
            if isinstance(meta, dict):
                for key in (
                    "azure_rate_limit_remaining_requests",
                    "azure_rate_limit_remaining_tokens",
                    "azure_retry_after_ms",
                    "azure_limiter_wait_seconds",
                    "azure_rate_limit_backoff_until",
                ):
                    if key in meta:
                        out[key] = meta[key]
    return out


def record_active_azure_exception(exc: BaseException) -> None:
    endpoint = os.getenv("ASA_AZURE_OPENAI_ACTIVE_BASE_URL") or os.getenv("AZURE_OPENAI_API_BASE") or os.getenv("AZURE_OPENAI_ENDPOINT")
    deployment = os.getenv("ASA_AZURE_OPENAI_ACTIVE_DEPLOYMENT") or os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not endpoint or not deployment:
        return
    try:
        limiter = AzureOpenAISharedRateLimiter(endpoint=endpoint, deployment=deployment)
        limiter.record_exception(exc)
    except Exception:
        pass
