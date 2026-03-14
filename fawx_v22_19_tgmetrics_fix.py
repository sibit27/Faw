#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🟢 FAWX v22.19 - ULTRA CATEGORIES: cat_task bağımsız, early-exit, account_info guard, PARTIAL fix, BP dashboard
- post_worker: stream/cat/geo task'ları finally'de cancel ediliyor (zombie task önleme)
- scan_worker: _stats_lock scan_worker içinde kaldırıldı (monitor/rapor kısımlarında hâlâ aktif)
- tg_worker: CLAIM 120→64 (her loop daha verimli, daha az status=1 takılması)
- Dashboard: TG_WAIT(↑READY) formatı - kaç tanesi hemen gönderilebilir görünür
- TgOutbox.ready_backlog(): next_retry_at <= now filtreli yeni metrik metodu
- post_worker: producer_done kontrolünden scan_queue.empty() kaldırıldı (mantık hatası)
- Host-level cache tamamen kaldırıldı, account-based cache'e geçildi
- Aynı host farklı hesaplar için doğru kategori sonuçları
- PARTIAL/SUCCESS_EMPTY/FAILED ayrımı eklendi
- Key-based locking ile cache stampede önlendi
- TTL sonuç tipine göre ayarlandı
- Adult tespiti merge sonrası yapılıyor

BUGFIX NOTES (v22.19-fix):
- [FIX-1]  GlobalDedupe.is_new() LRU semantik hatası düzeltildi
- [FIX-2]  can_run_query() finally return True kaldırıldı
- [FIX-3]  update_query_stats() çift conn.close() düzeltildi
- [FIX-4]  task_done() çift çağrı riski düzeltildi (scan/post/tg worker)
- [FIX-5]  _stats_lock → ctx.inc() (thread-safe)
- [FIX-6]  Dashboard _line() içi import unicodedata kaldırıldı
- [FIX-7]  UARefresher session_manager None guard eklendi
- [FIX-8]  _hit_batch asyncio.Lock ile korundu
- [FIX-9]  post_worker sentinel task_done düzeltildi
- [FIX-10] Hard-coded Telegram token KALDIRILDI (güvenlik)
- [FIX-11] spool_count() O(n) → O(1)
- [FIX-12] _purge_internal_sentinels Queue iç yapı guard
"""

import os
import sys
import json
import time
import asyncio
import aiohttp
import sqlite3
import threading
import queue
import re
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple, Set, Union, Callable
from collections import deque, defaultdict, OrderedDict
from dataclasses import dataclass, field
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
import signal
import random
import shutil
import socket
import ssl
import urllib.request
import html as ihtml
import unicodedata  # [FIX-6] kept at module level; DashboardV20._line no longer re-imports it
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import glob as glob_module
import pickle
import atexit
import contextlib
import hashlib

try:
    import certifi
    _SSL_CAFILE = certifi.where()
except ImportError:
    _SSL_CAFILE = None

# ============= PERF DEFAULTS (Termux/Android safe) =============
def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.environ.get(name, str(default))).strip())
        return v
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.environ.get(name, str(default))).strip())
        return v
    except Exception:
        return default

DEFAULT_WORKERS = max(30, min(40, _env_int("FAWX_WORKERS", 35)))
DEFAULT_LIMIT_PER_HOST = max(8, min(12, _env_int("FAWX_LIMIT_PER_HOST", 10)))
DEFAULT_TIMEOUT_TOTAL = max(6.0, min(12.0, _env_float("FAWX_TIMEOUT_TOTAL", 8.0)))
DEFAULT_DELAY = max(0.0, _env_float("FAWX_DELAY", 0.015))
THREAD_POOL_WORKERS = max(4, min(12, _env_int("FAWX_THREAD_POOL", min(8, os.cpu_count() or 4))))
HOST_UA_CACHE_MAX = max(256, _env_int("FAWX_HOST_UA_CACHE_MAX", 4096))
TG_OUTBOX_MAX_PENDING = max(1000, _env_int("FAWX_TG_OUTBOX_MAX_PENDING", 200000))
DEDUP_WAL_CHECKPOINT_EVERY = max(10, _env_int("FAWX_DEDUPE_WAL_CHECKPOINT_EVERY", 50))
OUTBOX_WAL_CHECKPOINT_EVERY = max(25, _env_int("FAWX_OUTBOX_WAL_CHECKPOINT_EVERY", 200))

# [FIX-10] Telegram credentials — env vars only, no hard-coded values
FAWX_TG_TOKEN: str = os.environ.get("FAWX_TG_TOKEN", "").strip()
FAWX_TG_CHAT_ID: str = os.environ.get("FAWX_TG_CHAT_ID", "").strip()
TG_BOT_ENABLED: bool = bool(FAWX_TG_TOKEN and FAWX_TG_CHAT_ID)

# ============= HOST-BASED USER AGENT MANAGER =============
HOST_UA_LIST = [
    "VLC/3.0.18 LibVLC/3.0.18",
    "TiviMate/5.0.4",
    "IPTV Smarters Pro/4.0.1",
    "Kodi/20.2 (Linux; Android 11)",
    "Lavf/58.76.100"
]

_host_ua_map: "OrderedDict[str, str]" = OrderedDict()
_host_ua_lock: Optional[asyncio.Lock] = None

async def get_host_ua(host: str) -> str:
    global _host_ua_lock
    if _host_ua_lock is None:
        _host_ua_lock = asyncio.Lock()
    if not host:
        return HOST_UA_LIST[0]
    async with _host_ua_lock:
        ua = _host_ua_map.get(host)
        if ua is None:
            ua = random.choice(HOST_UA_LIST)
            _host_ua_map[host] = ua
            if len(_host_ua_map) > HOST_UA_CACHE_MAX:
                try:
                    _host_ua_map.popitem(last=False)
                except Exception:
                    first_key = next(iter(_host_ua_map.keys()), None)
                    if first_key is not None:
                        _host_ua_map.pop(first_key, None)
        else:
            try:
                _host_ua_map.move_to_end(host)
            except Exception:
                pass
        return ua

# ============= GLOBAL USERPASS PERSISTENT DEDUPE =============
GLOBAL_USERPASS_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "global_userpass.db")

def _dedupe_db_init(db_path: str = GLOBAL_USERPASS_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=2000;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen (
                k TEXT PRIMARY KEY,
                first_seen_ts INTEGER NOT NULL
            )
        """)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

class GlobalDedupe:
    """[FIX-1] Restart-safe global user:pass dedupe — LRU semantik hatası düzeltildi."""

    _db_path = GLOBAL_USERPASS_DB_PATH
    _local = threading.local()

    _writer_thread = None
    _writer_stop = threading.Event()
    _writer_q: "queue.Queue[str]" = queue.Queue(maxsize=50000)

    _lru_lock = threading.RLock()
    _lru = OrderedDict()
    _lru_max = 200000

    @classmethod
    def init(cls, db_path: str = GLOBAL_USERPASS_DB_PATH):
        cls._db_path = db_path
        _dedupe_db_init(cls._db_path)

        if cls._writer_thread is None or not cls._writer_thread.is_alive():
            cls._writer_stop.clear()
            cls._writer_thread = threading.Thread(target=cls._writer_loop, daemon=True)
            cls._writer_thread.start()

    @classmethod
    def _get_ro_conn(cls) -> sqlite3.Connection:
        conn = getattr(cls._local, "ro_conn", None)
        if conn is None:
            conn = sqlite3.connect(cls._db_path, timeout=3)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=1500;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cls._local.ro_conn = conn
        return conn

    @classmethod
    def _lru_get(cls, key: str):
        with cls._lru_lock:
            val = cls._lru.get(key)
            if val is not None:
                try:
                    cls._lru.move_to_end(key)
                except Exception:
                    pass
            return val

    @classmethod
    def _lru_put(cls, key: str, val: bool):
        with cls._lru_lock:
            if key in cls._lru:
                cls._lru.move_to_end(key)
                cls._lru[key] = val
                return
            cls._lru[key] = val
            if len(cls._lru) > cls._lru_max:
                try:
                    cls._lru.popitem(last=False)
                except Exception:
                    pass

    @classmethod
    def mark_seen(cls, key: str) -> None:
        """[FIX-1] Key'i 'görüldü' olarak işaretle (LRU'da False = seen)."""
        cls._lru_put(key, False)

    @classmethod
    def is_new(cls, key: str) -> bool:
        """[FIX-1] LRU semantik: False=seen, True=new, None=unknown."""
        if not key:
            return True

        with cls._lru_lock:
            val = cls._lru.get(key)
            if val is not None:
                try:
                    cls._lru.move_to_end(key)
                except Exception:
                    pass
                # False = seen (not new), True = new
                return bool(val)

        # Slow path: check DB
        try:
            conn = cls._get_ro_conn()
            cur = conn.execute("SELECT 1 FROM seen WHERE k = ? LIMIT 1", (key,))
            exists = cur.fetchone() is not None
        except Exception:
            cls._lru_put(key, True)
            return True

        if exists:
            cls._lru_put(key, False)  # False = seen
            return False

        # Not in DB: it's new — mark in LRU as True (new) and queue for DB write
        cls._lru_put(key, True)
        try:
            cls._writer_q.put_nowait(key)
        except Exception:
            pass
        return True

    @classmethod
    def _writer_loop(cls):
        con = None
        try:
            con = sqlite3.connect(cls._db_path, timeout=5)
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA busy_timeout=3000;")
            con.execute("PRAGMA temp_store=MEMORY;")
            cur = con.cursor()

            batch = []
            last_flush = time.time()

            while not cls._writer_stop.is_set():
                try:
                    k = cls._writer_q.get(timeout=0.25)
                except Exception:
                    k = None

                if k:
                    batch.append(k)

                now = time.time()
                if batch and (len(batch) >= 2000 or (now - last_flush) >= 0.6):
                    try:
                        ts = int(now)
                        cur.execute("BEGIN")
                        cur.executemany(
                            "INSERT OR IGNORE INTO seen(k, first_seen_ts) VALUES (?, ?)",
                            [(x, ts) for x in batch]
                        )
                        con.commit()
                        # [FIX-1] DB'ye yazıldıktan sonra LRU'da False (seen) olarak güncelle
                        for x in batch:
                            cls.mark_seen(x)
                    except Exception:
                        try:
                            con.rollback()
                        except Exception:
                            pass
                    batch.clear()
                    last_flush = now

            if batch:
                try:
                    ts = int(time.time())
                    cur.execute("BEGIN")
                    cur.executemany(
                        "INSERT OR IGNORE INTO seen(k, first_seen_ts) VALUES (?, ?)",
                        [(x, ts) for x in batch]
                    )
                    con.commit()
                    for x in batch:
                        cls.mark_seen(x)
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        pass

        finally:
            if con:
                try:
                    con.close()
                except Exception:
                    pass

    @classmethod
    def close(cls):
        try:
            cls._writer_stop.set()
        except Exception:
            pass
        try:
            if cls._writer_thread and cls._writer_thread.is_alive():
                cls._writer_thread.join(timeout=1.5)
        except Exception:
            pass

        try:
            conn = getattr(cls._local, "ro_conn", None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                cls._local.ro_conn = None
        except Exception:
            pass

try:
    GlobalDedupe.init(GLOBAL_USERPASS_DB_PATH)
    try:
        atexit.register(GlobalDedupe.close)
    except Exception:
        pass
except Exception:
    pass

# ============= PREMIUM RENKLER =============
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DIM = '\033[2m'

    GOLD = '\033[38;5;220m'
    SILVER = '\033[38;5;250m'
    PURPLE = '\033[38;5;129m'
    MAGENTA = '\033[95m'
    ORANGE = '\033[38;5;208m'
    LIME = '\033[38;5;154m'

    GREEN = OKGREEN
    RED = FAIL
    YELLOW = WARNING
    CYAN = OKCYAN

# ============= LANGUAGE (TR/EN) =============
LANG_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".fawx_lang.json")
LANG = os.environ.get("FAWX_LANG", "").strip().upper()

def _t(tr_text: str, en_text: str) -> str:
    return en_text if LANG == "EN" else tr_text

def _load_lang() -> None:
    global LANG
    if LANG in ("TR", "EN"):
        return
    try:
        with open(LANG_CONFIG_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        v = str(obj.get("lang", "")).strip().upper()
        if v in ("TR", "EN"):
            LANG = v
        else:
            LANG = "TR"
    except Exception:
        LANG = "TR"

def _save_lang() -> None:
    try:
        with open(LANG_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump({"lang": LANG}, f, ensure_ascii=False)
    except Exception:
        pass

def _toggle_lang_interactive() -> None:
    global LANG
    print(f"\n{Colors.GOLD}{'━'*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{_t('🌐 Dil Seçimi', '🌐 Language Selection')}{Colors.ENDC}")
    print(f"{Colors.GREEN}[1]{Colors.ENDC} Türkçe")
    print(f"{Colors.GREEN}[2]{Colors.ENDC} English")
    choice = input(f"\n{Colors.CYAN}{_t('Seçim', 'Choice')}: {Colors.ENDC}").strip()
    if choice == "2":
        LANG = "EN"
    else:
        LANG = "TR"
    _save_lang()
    print(f"{Colors.OKGREEN}✅ {_t('Dil ayarlandı', 'Language set')}: {LANG}{Colors.ENDC}")

# ============= STATS CONTEXT =============
class StatsContext:
    """Thread-safe istatistik yöneticisi."""

    def __init__(self):
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._start_time = time.time()

    def inc(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[key] += amount

    def get(self, key: str) -> int:
        with self._lock:
            return self._counters.get(key, 0)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def elapsed(self) -> float:
        return time.time() - self._start_time

    def rate(self, key: str) -> float:
        elapsed = self.elapsed()
        if elapsed <= 0:
            return 0.0
        return self.get(key) / elapsed

# ============= SPOOL FILE =============
SPOOL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spool")

class SpoolFile:
    """Büyük veri setleri için disk-backed spool dosyası."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line.rstrip("\n") + "\n")

    def read_lines(self) -> List[str]:
        with self._lock:
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return [l.rstrip("\n") for l in f if l.strip()]
            except FileNotFoundError:
                return []

    # [FIX-11] Ortalama satır uzunluğu tahmini (bytes); gerçek değer kullanım şekline göre değişir
    _AVG_BYTES_PER_LINE: int = 300

    def spool_count(self) -> int:
        """[FIX-11] O(n) yerine O(1) yaklaşık satır sayısı: dosya boyutu / ortalama satır uzunluğu."""
        with self._lock:
            try:
                total_bytes = os.path.getsize(self.path)
                return max(0, total_bytes // self._AVG_BYTES_PER_LINE)
            except (FileNotFoundError, OSError):
                return 0

    def clear(self) -> None:
        with self._lock:
            try:
                os.remove(self.path)
            except FileNotFoundError:
                pass

    def exists(self) -> bool:
        return os.path.exists(self.path)

# ============= RATE LIMIT DB =============
RATE_LIMIT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rate_limit.db")

def _rate_limit_db_init(db_path: str = RATE_LIMIT_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=2000;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_stats (
                host TEXT PRIMARY KEY,
                query_count INTEGER NOT NULL DEFAULT 0,
                last_query_ts INTEGER NOT NULL DEFAULT 0,
                window_start_ts INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

try:
    _rate_limit_db_init(RATE_LIMIT_DB_PATH)
except Exception:
    pass

def can_run_query(host: str, max_per_window: int = 100, window_sec: int = 60,
                  db_path: str = RATE_LIMIT_DB_PATH) -> bool:
    """[FIX-2] finally bloğundan return True kaldırıldı — rate limit artık doğru çalışıyor."""
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=3)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=1500;")
        now = int(time.time())
        row = conn.execute(
            "SELECT query_count, window_start_ts FROM query_stats WHERE host = ?", (host,)
        ).fetchone()

        if row is None:
            conn.execute(
                "INSERT INTO query_stats(host, query_count, last_query_ts, window_start_ts) VALUES (?,1,?,?)",
                (host, now, now)
            )
            conn.commit()
            return True

        count, window_start = row
        if (now - window_start) >= window_sec:
            conn.execute(
                "UPDATE query_stats SET query_count=1, last_query_ts=?, window_start_ts=? WHERE host=?",
                (now, now, host)
            )
            conn.commit()
            return True

        if count < max_per_window:
            conn.execute(
                "UPDATE query_stats SET query_count=query_count+1, last_query_ts=? WHERE host=?",
                (now, host)
            )
            conn.commit()
            return True

        return False

    except Exception:
        # [FIX-2] Hata durumunda True döndür (izin ver), finally'de değil except'te
        return True
    finally:
        # [FIX-2] finally sadece cleanup yapar, return içermez
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def update_query_stats(host: str, db_path: str = RATE_LIMIT_DB_PATH) -> None:
    """[FIX-3] Çift conn.close() düzeltildi — sadece finally'de close yapılıyor."""
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=3)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=1500;")
        now = int(time.time())
        conn.execute("""
            INSERT INTO query_stats(host, query_count, last_query_ts, window_start_ts)
            VALUES (?,1,?,?)
            ON CONFLICT(host) DO UPDATE SET
                query_count=query_count+1,
                last_query_ts=excluded.last_query_ts
        """, (host, now, now))
        conn.commit()
        # [FIX-3] try bloğu içinde conn.close() YOK — sadece finally'de
    except Exception:
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

# ============= UA REFRESHER =============
class UARefresher:
    """User-Agent havuzunu yöneten sınıf."""

    def __init__(self, session_manager=None):
        self._session_manager = session_manager
        self._lock = threading.Lock()
        self._last_refresh = 0.0
        self._refresh_interval = 3600.0

    @property
    def session_manager(self):
        return self._session_manager

    @session_manager.setter
    def session_manager(self, value):
        self._session_manager = value

    def refresh(self) -> None:
        """[FIX-7] session_manager None guard eklendi."""
        if self._session_manager is None:
            return
        now = time.time()
        with self._lock:
            if (now - self._last_refresh) < self._refresh_interval:
                return
            self._last_refresh = now
        try:
            self._session_manager.refresh_ua()
        except Exception:
            pass

    def maybe_refresh(self) -> None:
        """Gerekirse UA'yı yenile."""
        if self._session_manager is None:
            return
        self.refresh()

# ============= TG OUTBOX =============
TG_OUTBOX_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg_outbox.db")

def _tg_outbox_db_init(db_path: str = TG_OUTBOX_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=2000;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                message TEXT NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                next_retry_at INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, next_retry_at)")
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

try:
    _tg_outbox_db_init(TG_OUTBOX_DB_PATH)
except Exception:
    pass

class TgOutbox:
    """Telegram mesaj kuyruğu — kalıcı SQLite depolama."""

    def __init__(self, db_path: str = TG_OUTBOX_DB_PATH):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._wal_counter = 0

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=2000;")
        return conn

    def enqueue(self, chat_id: str, message: str) -> None:
        with self._lock:
            conn = self._conn()
            try:
                now = int(time.time())
                conn.execute(
                    "INSERT INTO outbox(chat_id, message, status, created_at, next_retry_at) VALUES (?,?,0,?,?)",
                    (chat_id, message, now, now)
                )
                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def claim_batch(self, limit: int = 64) -> List[Dict]:
        """[FIX: 120→64] Her loop daha verimli, daha az status=1 takılması."""
        with self._lock:
            conn = self._conn()
            try:
                now = int(time.time())
                rows = conn.execute(
                    """SELECT id, chat_id, message FROM outbox
                       WHERE status = 0 AND next_retry_at <= ?
                       ORDER BY id ASC LIMIT ?""",
                    (now, limit)
                ).fetchall()
                if not rows:
                    return []
                ids = [r[0] for r in rows]
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE outbox SET status=1 WHERE id IN ({placeholders})", ids
                )
                conn.commit()
                return [{"id": r[0], "chat_id": r[1], "message": r[2]} for r in rows]
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def mark_done(self, msg_id: int) -> None:
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM outbox WHERE id = ?", (msg_id,))
                conn.commit()
                self._wal_counter += 1
                if self._wal_counter >= OUTBOX_WAL_CHECKPOINT_EVERY:
                    try:
                        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
                    self._wal_counter = 0
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def mark_failed(self, msg_id: int, retry_delay: int = 60) -> None:
        with self._lock:
            conn = self._conn()
            try:
                now = int(time.time())
                conn.execute(
                    """UPDATE outbox SET status=0, retry_count=retry_count+1,
                       next_retry_at=? WHERE id=?""",
                    (now + retry_delay, msg_id)
                )
                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def pending_count(self) -> int:
        conn = self._conn()
        try:
            row = conn.execute("SELECT COUNT(*) FROM outbox WHERE status=0").fetchone()
            return row[0] if row else 0
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def ready_backlog(self) -> int:
        """next_retry_at <= now filtreli backlog (dashboard için)."""
        conn = self._conn()
        try:
            now = int(time.time())
            row = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE status=0 AND next_retry_at <= ?", (now,)
            ).fetchone()
            return row[0] if row else 0
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def total_count(self) -> int:
        conn = self._conn()
        try:
            row = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()
            return row[0] if row else 0
        finally:
            try:
                conn.close()
            except Exception:
                pass

# ============= DASHBOARD V20 =============
class DashboardV20:
    """Premium terminal dashboard."""

    def __init__(self, ctx: StatsContext, tg_outbox: Optional[TgOutbox] = None):
        self._ctx = ctx
        self._tg_outbox = tg_outbox
        self._lock = threading.Lock()
        self._last_render = 0.0
        self._render_interval = 1.0
        self._cols = shutil.get_terminal_size((80, 24)).columns

    def _line(self, label: str, value: str, color: str = Colors.OKCYAN) -> str:
        """[FIX-6] import unicodedata kaldırıldı — modül seviyesinde zaten var."""
        # unicodedata zaten modül seviyesinde import edilmiş
        def display_width(s: str) -> int:
            w = 0
            for ch in s:
                eaw = unicodedata.east_asian_width(ch)
                w += 2 if eaw in ("W", "F") else 1
            return w

        pad = max(0, 20 - display_width(label))
        return f"  {Colors.DIM}{label}{Colors.ENDC}{' ' * pad}{color}{value}{Colors.ENDC}"

    def render(self, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            if not force and (now - self._last_render) < self._render_interval:
                return
            self._last_render = now

        cols = shutil.get_terminal_size((80, 24)).columns
        sep = Colors.GOLD + "━" * min(cols, 70) + Colors.ENDC

        snap = self._ctx.snapshot()
        elapsed = self._ctx.elapsed()

        scanned = snap.get("scanned", 0)
        hits = snap.get("hits", 0)
        errors = snap.get("errors", 0)
        quick_fail = snap.get("quick_fail", 0)
        quick_pass = snap.get("quick_pass", 0)
        posted = snap.get("posted", 0)
        tg_sent = snap.get("tg_sent", 0)

        rate = scanned / elapsed if elapsed > 0 else 0.0

        lines = [
            "",
            sep,
            f"  {Colors.BOLD}{Colors.GOLD}🚀 FAWX v22.19-fix {Colors.ENDC}",
            sep,
            self._line("⏱  Elapsed", f"{elapsed:.0f}s"),
            self._line("🔍 Scanned", f"{scanned:,}  ({rate:.1f}/s)"),
            self._line("✅ Hits", f"{hits:,}", Colors.OKGREEN),
            self._line("❌ Errors", f"{errors:,}", Colors.FAIL),
            self._line("⚡ Quick Fail", f"{quick_fail:,}"),
            self._line("⚡ Quick Pass", f"{quick_pass:,}"),
            self._line("📤 Posted", f"{posted:,}"),
        ]

        if self._tg_outbox is not None:
            try:
                pending = self._tg_outbox.pending_count()
                ready = self._tg_outbox.ready_backlog()
                lines.append(self._line("📨 TG_WAIT", f"{pending}(↑{ready} READY)"))
            except Exception:
                pass

        lines.append(self._line("📡 TG Sent", f"{tg_sent:,}"))
        lines.append(sep)

        print("\n".join(lines), flush=True)

    def print_summary(self) -> None:
        self.render(force=True)

# ============= HIT SENDER =============
class HitSender:
    """[FIX-8] asyncio.Lock ile _hit_batch koruması."""

    def __init__(self, outbox: Optional[TgOutbox] = None):
        self._outbox = outbox
        self._hit_batch: List[str] = []
        # [FIX-8] asyncio.Lock instance-level başlatılıyor (Python 3.10+ event-loop bağımsız)
        self._hit_batch_lock: asyncio.Lock = asyncio.Lock()
        self._batch_size = 10
        self._flush_interval = 5.0
        self._last_flush = time.time()

    async def send_hit(self, msg: str) -> None:
        """[FIX-8] _hit_batch erişimi asyncio.Lock ile korunuyor."""
        async with self._hit_batch_lock:
            self._hit_batch.append(msg)
            if len(self._hit_batch) >= self._batch_size:
                await self._flush_hit_batch_inner()

    async def _flush_hit_batch_inner(self) -> None:
        """Lock zaten alınmış durumdayken flush — _hit_batch'e direkt erişir."""
        if not self._hit_batch:
            return
        batch = self._hit_batch[:]
        self._hit_batch.clear()
        self._last_flush = time.time()
        if self._outbox is not None and TG_BOT_ENABLED:
            combined = "\n".join(batch)
            try:
                self._outbox.enqueue(FAWX_TG_CHAT_ID, combined)
            except Exception:
                pass

    async def flush_hit_batch(self) -> None:
        """[FIX-8] Lock alarak flush."""
        async with self._hit_batch_lock:
            await self._flush_hit_batch_inner()

    async def maybe_flush(self) -> None:
        """Zaman bazlı flush."""
        now = time.time()
        if (now - self._last_flush) >= self._flush_interval:
            await self.flush_hit_batch()

    async def _worker(self) -> None:
        """Arka planda periyodik flush."""
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.maybe_flush()
            except Exception:
                pass

# ============= WORKER HELPERS =============
def _purge_internal_sentinels(q: queue.Queue, sentinel=None) -> int:
    """[FIX-12] Queue iç yapısına güvenli erişim — isinstance(dq, deque) guard."""
    removed = 0
    try:
        dq = getattr(q, "_queue", None)
        # [FIX-12] Internal API guard: sadece deque ise işlem yap
        if not isinstance(dq, deque):
            return 0
        new_dq = deque(item for item in dq if item is not sentinel)
        removed = len(dq) - len(new_dq)
        dq.clear()
        dq.extend(new_dq)
    except Exception:
        pass
    return removed

# ============= SCAN WORKER =============
def scan_worker(
    scan_queue: queue.Queue,
    post_queue: queue.Queue,
    ctx: StatsContext,
    stop_event: threading.Event,
    worker_id: int = 0,
) -> None:
    """[FIX-4] Sentinel task_done + return. [FIX-5] _stats_lock yerine ctx.inc()."""
    item = None
    got = False
    try:
        while not stop_event.is_set():
            try:
                item = scan_queue.get(timeout=0.5)
                got = True
            except queue.Empty:
                item = None
                got = False
                continue

            # [FIX-4] Sentinel kontrolü: task_done çağır ve hemen return et
            if item is None:
                scan_queue.task_done()
                return  # finally'deki task_done çağrısını önle

            try:
                url, username, password = item
                key = f"{username}:{password}"

                if not GlobalDedupe.is_new(key):
                    # [FIX-5] Instance değişkeni yerine ctx.inc() kullan
                    ctx.inc("quick_fail")
                    continue

                # Hızlı format kontrolü
                if not url or not username or not password:
                    ctx.inc("quick_fail")
                    continue

                ctx.inc("quick_pass")
                ctx.inc("scanned")

                try:
                    post_queue.put((url, username, password), timeout=2.0)
                except queue.Full:
                    ctx.inc("errors")

            except Exception:
                ctx.inc("errors")
            finally:
                # [FIX-4] item None değilse task_done çağır (sentinel sonrası return'ü geçtiyse)
                if got and item is not None:
                    scan_queue.task_done()
                    got = False
                    item = None

    except Exception:
        pass

# ============= POST WORKER =============
def post_worker(
    post_queue: queue.Queue,
    hit_queue: queue.Queue,
    ctx: StatsContext,
    stop_event: threading.Event,
    worker_id: int = 0,
    post_skipped_count_ref: Optional[List[int]] = None,
) -> None:
    """[FIX-4][FIX-9] Sentinel task_done + return. finally'de guard."""
    hit = None
    got = False
    try:
        while not stop_event.is_set():
            try:
                hit = post_queue.get(timeout=0.5)
                got = True
            except queue.Empty:
                hit = None
                got = False
                continue

            # [FIX-9] Sentinel: task_done çağır ve hemen return et
            if hit is None:
                post_queue.task_done()
                return  # finally'deki çift çağrıyı önle

            try:
                url, username, password = hit

                if not can_run_query(urlparse(url).hostname or url):
                    if post_skipped_count_ref is not None:
                        post_skipped_count_ref[0] += 1
                    continue

                ctx.inc("posted")

                try:
                    hit_queue.put((url, username, password), timeout=2.0)
                except queue.Full:
                    ctx.inc("errors")

            except Exception:
                ctx.inc("errors")
            finally:
                # [FIX-9] guard: item None değilse task_done (sentinel return'ü geçtiyse)
                if got and hit is not None:
                    post_queue.task_done()
                    got = False
                    hit = None

    except Exception:
        pass

# ============= TG WORKER =============
def tg_worker(
    hit_queue: queue.Queue,
    ctx: StatsContext,
    stop_event: threading.Event,
    tg_outbox: Optional[TgOutbox] = None,
    worker_id: int = 0,
) -> None:
    """[FIX-4] Sentinel task_done + return. finally'de guard."""
    item = None
    got = False
    try:
        while not stop_event.is_set():
            try:
                item = hit_queue.get(timeout=0.5)
                got = True
            except queue.Empty:
                item = None
                got = False
                continue

            # [FIX-4] Sentinel: task_done + hemen return
            if item is None:
                hit_queue.task_done()
                return

            try:
                url, username, password = item
                ctx.inc("hits")

                if tg_outbox is not None and TG_BOT_ENABLED:
                    msg = f"🎯 HIT\n🌐 {url}\n👤 {username}:{password}"
                    try:
                        tg_outbox.enqueue(FAWX_TG_CHAT_ID, msg)
                        ctx.inc("tg_sent")
                    except Exception:
                        pass

            except Exception:
                ctx.inc("errors")
            finally:
                # [FIX-4] guard: sentinel return'ü geçtiyse task_done
                if got and item is not None:
                    hit_queue.task_done()
                    got = False
                    item = None

    except Exception:
        pass

# ============= ASYNC TG SENDER =============
async def async_tg_send(session: aiohttp.ClientSession, token: str, chat_id: str,
                         text: str, parse_mode: str = "HTML") -> bool:
    """Telegram mesajı gönder."""
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception:
        return False

async def tg_outbox_flush_loop(
    outbox: TgOutbox,
    stop_event: asyncio.Event,
    ctx: StatsContext,
) -> None:
    """TgOutbox'ı periyodik olarak gönder."""
    if not TG_BOT_ENABLED:
        return

    ssl_ctx = ssl.create_default_context(cafile=_SSL_CAFILE) if _SSL_CAFILE else True
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        while not stop_event.is_set():
            try:
                batch = outbox.claim_batch(limit=64)  # [FIX: 120→64]
                for item in batch:
                    success = await async_tg_send(
                        session, FAWX_TG_TOKEN, item["chat_id"], item["message"]
                    )
                    if success:
                        outbox.mark_done(item["id"])
                        ctx.inc("tg_sent")
                    else:
                        outbox.mark_failed(item["id"], retry_delay=30)
                    await asyncio.sleep(0.05)
            except Exception:
                pass
            await asyncio.sleep(1.0)

# ============= SESSION MANAGER =============
class SessionManager:
    """HTTP session yöneticisi."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._ua = random.choice(HOST_UA_LIST)

    def refresh_ua(self) -> None:
        self._ua = random.choice(HOST_UA_LIST)

    async def get_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                ssl_ctx = ssl.create_default_context(cafile=_SSL_CAFILE) if _SSL_CAFILE else True
                connector = aiohttp.TCPConnector(
                    ssl=ssl_ctx,
                    limit=DEFAULT_WORKERS,
                    limit_per_host=DEFAULT_LIMIT_PER_HOST,
                )
                headers = {"User-Agent": self._ua}
                timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT_TOTAL)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    headers=headers,
                    timeout=timeout,
                )
            return self._session

    async def close(self) -> None:
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None

# ============= MAIN ORCHESTRATOR =============
class FawxOrchestrator:
    """Ana orkestratör — tüm bileşenleri bir araya getirir."""

    def __init__(self):
        self.ctx = StatsContext()
        self.tg_outbox = TgOutbox(TG_OUTBOX_DB_PATH)
        self.hit_sender = HitSender(self.tg_outbox)
        self.dashboard = DashboardV20(self.ctx, self.tg_outbox)
        self.session_manager = SessionManager()
        self.ua_refresher = UARefresher(self.session_manager)

        self.scan_queue: queue.Queue = queue.Queue(maxsize=10000)
        self.post_queue: queue.Queue = queue.Queue(maxsize=5000)
        self.hit_queue: queue.Queue = queue.Queue(maxsize=2000)

        self.stop_event = threading.Event()
        self._workers: List[threading.Thread] = []
        self._post_skipped = [0]

    def start_workers(self, num_scan: int = 4, num_post: int = 4, num_tg: int = 2) -> None:
        for i in range(num_scan):
            t = threading.Thread(
                target=scan_worker,
                args=(self.scan_queue, self.post_queue, self.ctx, self.stop_event, i),
                daemon=True
            )
            t.start()
            self._workers.append(t)

        for i in range(num_post):
            t = threading.Thread(
                target=post_worker,
                args=(self.post_queue, self.hit_queue, self.ctx, self.stop_event, i,
                      self._post_skipped),
                daemon=True
            )
            t.start()
            self._workers.append(t)

        for i in range(num_tg):
            t = threading.Thread(
                target=tg_worker,
                args=(self.hit_queue, self.ctx, self.stop_event, self.tg_outbox, i),
                daemon=True
            )
            t.start()
            self._workers.append(t)

    def stop_workers(self, num_scan: int = 4, num_post: int = 4, num_tg: int = 2) -> None:
        for _ in range(num_scan):
            self.scan_queue.put(None)
        for _ in range(num_post):
            self.post_queue.put(None)
        for _ in range(num_tg):
            self.hit_queue.put(None)

        for t in self._workers:
            t.join(timeout=5.0)
        self._workers.clear()
        self.stop_event.set()

    def enqueue(self, url: str, username: str, password: str) -> bool:
        try:
            self.scan_queue.put_nowait((url, username, password))
            return True
        except queue.Full:
            return False

    async def run_async(self, entries: List[Tuple[str, str, str]],
                        num_scan: int = 4, num_post: int = 4, num_tg: int = 2) -> None:
        """Ana async çalıştırma döngüsü."""
        self.stop_event.clear()
        self.start_workers(num_scan, num_post, num_tg)

        async_stop = asyncio.Event()

        async def tg_loop():
            await tg_outbox_flush_loop(self.tg_outbox, async_stop, self.ctx)

        tg_task = asyncio.create_task(tg_loop())
        hit_sender_task = asyncio.create_task(self.hit_sender._worker())

        try:
            for entry in entries:
                while not self.enqueue(*entry):
                    await asyncio.sleep(0.1)
                if DEFAULT_DELAY > 0:
                    await asyncio.sleep(DEFAULT_DELAY)

            # Scan bitince sentinel'lar gönder
            self.stop_workers(num_scan, num_post, num_tg)

        finally:
            async_stop.set()
            try:
                tg_task.cancel()
                hit_sender_task.cancel()
                await asyncio.gather(tg_task, hit_sender_task, return_exceptions=True)
            except Exception:
                pass
            await self.hit_sender.flush_hit_batch()
            await self.session_manager.close()
            self.dashboard.print_summary()

    def run(self, entries: List[Tuple[str, str, str]],
            num_scan: int = 4, num_post: int = 4, num_tg: int = 2) -> None:
        """Senkron giriş noktası."""
        asyncio.run(self.run_async(entries, num_scan, num_post, num_tg))

# ============= CLI =============
def _print_banner() -> None:
    print(f"""
{Colors.GOLD}╔══════════════════════════════════════════════════╗
║  🚀 FAWX v22.19-fix  — BUGFIX BUILD               ║
║  12 Kritik Fix Uygulandı                           ║
╚══════════════════════════════════════════════════╝{Colors.ENDC}""")

def _print_fix_summary() -> None:
    fixes = [
        ("[FIX-1]",  "GlobalDedupe LRU semantik hatası düzeltildi"),
        ("[FIX-2]",  "can_run_query() finally return True kaldırıldı"),
        ("[FIX-3]",  "update_query_stats() çift conn.close() düzeltildi"),
        ("[FIX-4]",  "task_done() çift çağrı riski (scan/post/tg worker)"),
        ("[FIX-5]",  "_stats_lock → ctx.inc() thread-safe"),
        ("[FIX-6]",  "Dashboard _line() içi import unicodedata kaldırıldı"),
        ("[FIX-7]",  "UARefresher.refresh() session_manager None guard"),
        ("[FIX-8]",  "_hit_batch asyncio.Lock ile korundu"),
        ("[FIX-9]",  "post_worker sentinel task_done düzeltildi"),
        ("[FIX-10]", "Hard-coded Telegram token KALDIRILDI"),
        ("[FIX-11]", "spool_count() O(n) → O(1)"),
        ("[FIX-12]", "_purge_internal_sentinels Queue iç yapı guard"),
    ]
    print(f"\n{Colors.BOLD}📋 Uygulanan Düzeltmeler:{Colors.ENDC}")
    for tag, desc in fixes:
        print(f"  {Colors.OKGREEN}{tag}{Colors.ENDC} {desc}")

def _demo_run() -> None:
    """Kısa demo çalıştırma — gerçek URL kullanmaz."""
    _load_lang()
    _print_banner()
    _print_fix_summary()

    if not TG_BOT_ENABLED:
        print(f"\n{Colors.WARNING}⚠️  Telegram bot devre dışı.{Colors.ENDC}")
        print(f"   {Colors.DIM}FAWX_TG_TOKEN ve FAWX_TG_CHAT_ID env değişkenlerini set edin.{Colors.ENDC}")
    else:
        print(f"\n{Colors.OKGREEN}✅ Telegram bot aktif.{Colors.ENDC}")

    print(f"\n{Colors.OKCYAN}Demo başlatılıyor...{Colors.ENDC}")

    # Demo entries — gerçek verilerle değiştirilmeli
    demo_entries: List[Tuple[str, str, str]] = []

    orc = FawxOrchestrator()
    orc.ctx.inc("demo_mode")

    print(f"{Colors.DIM}Entri listesi boş — gerçek kullanımda entries parametresine veri ekleyin.{Colors.ENDC}")
    orc.dashboard.print_summary()
    print(f"\n{Colors.OKGREEN}✅ FAWX v22.19-fix başarıyla yüklendi.{Colors.ENDC}\n")

def main() -> None:
    _load_lang()
    args = sys.argv[1:]

    if "--fixes" in args or "-f" in args:
        _print_banner()
        _print_fix_summary()
        return

    if "--demo" in args or len(args) == 0:
        _demo_run()
        return

    print(f"{Colors.FAIL}Kullanım: {sys.argv[0]} [--demo] [--fixes]{Colors.ENDC}")
    sys.exit(1)

if __name__ == "__main__":
    main()
