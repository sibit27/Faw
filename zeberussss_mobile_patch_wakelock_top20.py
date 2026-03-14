#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeberussss_mobile_patch_wakelock_top20.py
Zeberus IPTV Checker — Termux/Android, top-20 edition with wake-lock patch.

Fixes applied
─────────────
FIX 1   can_run_query        — return True moved outside finally block
FIX 2   update_query_stats   — conn.close() only in finally (no double-close)
FIX 3   post_check_all_hits  — state.all_hits guarded with list(... or [])
FIX 4   enable_wakelock      — returns False on failure path (consistent bool)
FIX 5   server_worker        — removed spurious state.active_threads lines
FIX 6   safe_fetch           — bare except changed to except Exception
FIX 7   Thread-safety        — state.checks += 1 wrapped inside with state.lock
FIX 8   post_check_all_hits  — rich.progress import moved to module top
FIX 9   hit_info ordering    — goruntu_durumu set before categorize_and_save_hits
FIX 10  run_scan             — state object preserved; no new DashboardState()
"""

from __future__ import annotations

import os
import queue
import sqlite3
import subprocess
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from queue import Empty
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table

    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VERSION = "top20-wakelock"
MAX_WORKERS = 20
TIMEOUT = 10
RETRY = 2
USER_AGENT = "Mozilla/5.0 (Linux; Android 11; Termux) AppleWebKit/537.36"

CATEGORY_GORUNTULU = "GORUNTULU"
CATEGORY_ADULT = "ADULT"
CATEGORY_GORUNTUSUZ = "GORUNTUSUZ"

# ---------------------------------------------------------------------------
# Termux wake lock  (FIX 4)
# ---------------------------------------------------------------------------


def enable_wakelock() -> bool:
    """Activate Termux wake lock to prevent CPU sleep during scan.

    FIX 4: returns True on success, False on any failure (was falling through
    to implicit None previously).
    """
    try:
        result = subprocess.run(
            ["termux-wake-lock"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            print("[WAKELOCK] Termux wake lock activated.")
            return True
        print("[WAKELOCK] termux-wake-lock returned non-zero; continuing anyway.")
        return False
    except FileNotFoundError:
        print("[WAKELOCK] termux-wake-lock not found (not on Termux?); skipping.")
        return False
    except Exception as exc:
        print(f"[WAKELOCK] Warning: could not activate wake lock: {exc}")
        return False  # FIX 4: explicit False instead of falling through


def disable_wakelock() -> None:
    """Release Termux wake lock."""
    try:
        subprocess.run(["termux-wake-unlock"], capture_output=True, timeout=5)
        print("[WAKELOCK] Termux wake lock released.")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# SQLite query-cache helpers
# ---------------------------------------------------------------------------


def _open_cache_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS query_cache "
        "(query TEXT PRIMARY KEY, last_run REAL, count INTEGER)"
    )
    conn.commit()
    return conn


def can_run_query(query: str, db_path: str = "query_cache.db", min_interval: float = 60.0) -> bool:
    """Return True if *query* can be run again (interval has elapsed).

    FIX 1: return True was previously inside the finally block, which caused
    Python to silently swallow the explicit return value from the try block.
    The finally block no longer returns a value; the fallback True is returned
    only on the except path.
    """
    conn = _open_cache_db(db_path)
    try:
        row = conn.execute(
            "SELECT last_run FROM query_cache WHERE query = ?", (query,)
        ).fetchone()
        if row is None:
            return True
        return (time.time() - row[0]) >= min_interval
    except Exception:
        return True  # FIX 1: fallback True only on error, not unconditionally
    finally:
        conn.close()  # FIX 1: finally never returns a value


def update_query_stats(query: str, db_path: str = "query_cache.db") -> None:
    """Record that *query* was just executed.

    FIX 2: conn.close() is called only in finally; the early close() inside the
    try block is removed to prevent a double-close.
    """
    conn = _open_cache_db(db_path)
    try:
        conn.execute(
            "INSERT INTO query_cache (query, last_run, count) VALUES (?, ?, 1) "
            "ON CONFLICT(query) DO UPDATE SET last_run=excluded.last_run, "
            "count=count+1",
            (query, time.time()),
        )
        conn.commit()
        # FIX 2: no conn.close() here — only in finally below
    except Exception:
        pass
    finally:
        conn.close()  # FIX 2: single, guaranteed close


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ServerState:
    """Per-server mutable counters."""

    url: str
    active_threads: int = 0
    checks: int = 0
    hits: int = 0
    done: bool = False


@dataclass
class DashboardState:
    """Global dashboard state shared across all threads."""

    servers: List[ServerState] = field(default_factory=list)
    hits: int = 0
    checks: int = 0
    all_hits: Optional[List[Dict[str, Any]]] = None
    goruntulu_hits: int = 0
    adult_hits: int = 0
    goruntusuz_hits: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, compare=False, repr=False)

    def update_hit(self, category: str) -> None:
        """Increment the relevant hit counter (thread-safe)."""
        with self.lock:
            self.hits += 1
            if category == CATEGORY_ADULT:
                self.adult_hits += 1
            elif category == CATEGORY_GORUNTULU:
                self.goruntulu_hits += 1
            else:
                self.goruntusuz_hits += 1

    def add_hit_record(self, record: Dict[str, Any]) -> None:
        """Append a hit record to all_hits (thread-safe)."""
        with self.lock:
            if self.all_hits is None:
                self.all_hits = []
            self.all_hits.append(record)

    def reset_run(self) -> None:
        """Reset counters without touching server entries."""
        with self.lock:
            self.hits = 0
            self.checks = 0
            self.all_hits = None
            self.goruntulu_hits = 0
            self.adult_hits = 0
            self.goruntusuz_hits = 0


# ---------------------------------------------------------------------------
# Category fetching
# ---------------------------------------------------------------------------


def safe_fetch(url: str, timeout: int = TIMEOUT) -> Optional[Any]:
    """Perform a GET request and return the parsed JSON, or None on any error.

    FIX 6: bare except replaced with except Exception so KeyboardInterrupt
    and SystemExit propagate correctly.
    """
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        return resp.json()
    except Exception:  # FIX 6: was bare except
        return None


def get_all_categories_separated_enhanced(
    server: str,
    username: str,
    password: str,
) -> Dict[str, List[Dict]]:
    """Fetch live, VOD and series category lists from an Xtream-codes server.

    Uses safe_fetch (FIX 6) so that KeyboardInterrupt is never swallowed.
    """
    base = f"{server}/player_api.php?username={username}&password={password}"
    endpoints = {
        "live": f"{base}&action=get_live_categories",
        "vod": f"{base}&action=get_vod_categories",
        "series": f"{base}&action=get_series_categories",
    }
    result: Dict[str, List[Dict]] = {}
    for kind, url in endpoints.items():
        data = safe_fetch(url)
        result[kind] = data if isinstance(data, list) else []
    return result


# ---------------------------------------------------------------------------
# Check a single credential
# ---------------------------------------------------------------------------


def check_line_for_server(
    server: str,
    username: str,
    password: str,
    state: DashboardState,
    srv_state: ServerState,
) -> Optional[Dict[str, Any]]:
    """Validate one (server, username, password) triple.

    FIX 7: state.checks is incremented inside with state.lock so the
    increment is thread-safe.
    """
    url = (
        f"{server}/player_api.php"
        f"?username={username}&password={password}"
    )
    for attempt in range(RETRY + 1):
        try:
            resp = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            user_info = data.get("user_info", {})
            if user_info.get("auth") != 1:
                break
            adult = "adult" in str(data).lower()
            streams = int(user_info.get("active_cons", 0))
            category = (
                CATEGORY_ADULT
                if adult
                else CATEGORY_GORUNTULU
                if streams > 0
                else CATEGORY_GORUNTUSUZ
            )
            # FIX 9: goruntu_durumu is set HERE, before categorize_and_save_hits
            hit_info: Dict[str, Any] = {
                "server": server,
                "username": username,
                "password": password,
                "adult": adult,
                "streams": streams,
                "category": category,
                "goruntu_durumu": category,  # FIX 9: set before save
            }
            state.update_hit(category)
            state.add_hit_record(hit_info)
            with srv_state.active_threads.__class__:  # pragma: no cover – lock guard comment
                pass
            srv_state.hits += 1
            # FIX 7: checks increment is inside the lock
            with state.lock:
                state.checks += 1
            srv_state.checks += 1
            return hit_info
        except Exception:
            if attempt < RETRY:
                time.sleep(0.5)
    # FIX 7: even on miss, increment checks inside lock
    with state.lock:
        state.checks += 1
    srv_state.checks += 1
    return None


# ---------------------------------------------------------------------------
# Category save
# ---------------------------------------------------------------------------


def categorize_and_save_hits(
    hits: List[Dict[str, Any]],
    output_dir: str = "output",
) -> Dict[str, List[Dict[str, Any]]]:
    """Write hits into per-category files.

    FIX 9: goruntu_durumu is already set in hit_info before this function is
    called (done inside check_line_for_server), so hit.get('goruntu_durumu')
    is always populated here.
    """
    os.makedirs(output_dir, exist_ok=True)
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for hit in hits:
        cat = hit.get("goruntu_durumu", CATEGORY_GORUNTUSUZ)
        buckets[cat].append(hit)

    for cat, bucket in buckets.items():
        path = os.path.join(output_dir, f"{cat.lower()}_hits.txt")
        with open(path, "w", encoding="utf-8") as fh:
            for h in bucket:
                fh.write(f"{h['server']}|{h['username']}|{h['password']}\n")
    return dict(buckets)


# ---------------------------------------------------------------------------
# Post-check
# ---------------------------------------------------------------------------


def post_check_all_hits(state: DashboardState, output_dir: str = "output") -> None:
    """Re-validate every recorded hit after the main scan completes.

    FIX 3: state.all_hits is guarded with list(state.all_hits or []) so that
    None is handled gracefully.
    FIX 8: rich.progress is imported at the module level (top of file) rather
    than lazily inside this function.
    """
    # FIX 3: guard against None
    hits_snapshot = list(state.all_hits or [])
    if not hits_snapshot:
        return

    if _RICH_AVAILABLE:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        )
        task_id = progress.add_task("Post-checking hits…", total=len(hits_snapshot))
        progress.start()
    else:
        progress = None
        task_id = None

    still_alive: List[Dict[str, Any]] = []
    try:
        for hit in hits_snapshot:
            url = (
                f"{hit['server']}/player_api.php"
                f"?username={hit['username']}&password={hit['password']}"
            )
            data = safe_fetch(url)
            if data and data.get("user_info", {}).get("auth") == 1:
                still_alive.append(hit)
            if progress is not None:
                progress.advance(task_id)
    finally:
        if progress is not None:
            progress.stop()

    with state.lock:
        state.all_hits = still_alive


# ---------------------------------------------------------------------------
# Server worker  (FIX 5)
# ---------------------------------------------------------------------------


def server_worker(
    srv_state: ServerState,
    job_queue: queue.Queue,
    state: DashboardState,
    stop_event: threading.Event,
) -> None:
    """Worker thread for one server.

    FIX 5: The spurious state.active_threads increment/decrement lines have
    been removed.  Only srv_state.active_threads (per-server) is maintained;
    the dashboard sums those values itself.
    """
    # FIX 5: do NOT touch state.active_threads — it doesn't exist on DashboardState
    srv_state.active_threads += 1
    try:
        while not stop_event.is_set():
            try:
                username, password = job_queue.get(timeout=1.0)
            except Empty:
                if srv_state.done:
                    break
                continue
            try:
                check_line_for_server(
                    srv_state.url,
                    username,
                    password,
                    state,
                    srv_state,
                )
            finally:
                job_queue.task_done()
    finally:
        srv_state.active_threads -= 1


# ---------------------------------------------------------------------------
# Scan orchestrator  (FIX 10)
# ---------------------------------------------------------------------------


def run_scan(
    servers: List[str],
    credentials: List[Tuple[str, str]],
    state: DashboardState,
    num_workers: int = MAX_WORKERS,
) -> None:
    """Orchestrate the full scan, writing results into *state*.

    FIX 10: state is passed in and reused; a new DashboardState() is NOT
    created inside this function, which would discard already-configured
    per-server state.
    """
    # Initialise per-server state entries if not already present
    existing_urls = {s.url for s in state.servers}
    for srv_url in servers:
        if srv_url not in existing_urls:
            state.servers.append(ServerState(url=srv_url))

    queues: Dict[str, queue.Queue] = {}
    stop_event = threading.Event()
    workers: List[threading.Thread] = []

    for srv_state in state.servers:
        q: queue.Queue = queue.Queue()
        queues[srv_state.url] = q
        for cred in credentials:
            q.put(cred)
        for _ in range(min(num_workers, len(credentials) or 1)):
            t = threading.Thread(
                target=server_worker,
                args=(srv_state, q, state, stop_event),
                daemon=True,
            )
            t.start()
            workers.append(t)

    try:
        for q in queues.values():
            q.join()
    except KeyboardInterrupt:
        print("\n[SCAN] Interrupted — stopping…")
        stop_event.set()
        for srv_state in state.servers:
            try:
                srv_state.done = True
            except Exception:
                pass
    finally:
        stop_event.set()
        for t in workers:
            t.join(timeout=5)


# ---------------------------------------------------------------------------
# Dashboard (rich)
# ---------------------------------------------------------------------------


def build_dashboard_table(state: DashboardState) -> "Table":
    """Build a Rich Table with live dashboard information."""
    table = Table(title=f"Zeberus IPTV Checker {VERSION}", expand=True)
    table.add_column("Server", style="cyan", no_wrap=True)
    table.add_column("Threads", justify="right")
    table.add_column("Checks", justify="right")
    table.add_column("Hits", justify="right")
    table.add_column("Done", justify="center")

    for srv in state.servers:
        table.add_row(
            srv.url,
            str(srv.active_threads),
            str(srv.checks),
            str(srv.hits),
            "✅" if srv.done else "🔄",
        )

    table.caption = (
        f"Total: checks={state.checks}  hits={state.hits}  "
        f"goruntulu={state.goruntulu_hits}  adult={state.adult_hits}  "
        f"goruntusuz={state.goruntusuz_hits}"
    )
    return table


def run_dashboard(state: DashboardState, stop_event: threading.Event) -> None:
    """Run a Rich live dashboard until stop_event is set."""
    if not _RICH_AVAILABLE:
        return
    console = Console()
    with Live(console=console, refresh_per_second=4) as live:
        while not stop_event.is_set():
            live.update(Panel(build_dashboard_table(state)))
            time.sleep(0.25)
        live.update(Panel(build_dashboard_table(state)))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    wakelock_on = enable_wakelock()  # FIX 4: bool return value used

    stop_event = threading.Event()
    # FIX 10: create state once and pass it into run_scan
    state = DashboardState()

    try:
        print(f"[MAIN] Zeberus IPTV Checker {VERSION} starting…")

        # --- configuration (edit as needed) ---
        servers: List[str] = []
        credentials: List[Tuple[str, str]] = []
        output_dir = "output"

        if not servers:
            print("[MAIN] No servers configured — exiting.")
            return

        if _RICH_AVAILABLE:
            dash_thread = threading.Thread(
                target=run_dashboard,
                args=(state, stop_event),
                daemon=True,
            )
            dash_thread.start()

        # FIX 10: pass existing state into run_scan
        run_scan(servers, credentials, state)

        # Post-check
        post_check_all_hits(state, output_dir=output_dir)

        # Categorise and save
        hits_snapshot = list(state.all_hits or [])  # FIX 3
        buckets = categorize_and_save_hits(hits_snapshot, output_dir=output_dir)

        print(f"[MAIN] Done — {state.hits} hits / {state.checks} checked")
        for cat, bucket in buckets.items():
            print(f"  {cat}: {len(bucket)}")

    except KeyboardInterrupt:
        print("\n[MAIN] Interrupted by user.")
    finally:
        stop_event.set()
        if wakelock_on:
            disable_wakelock()


if __name__ == "__main__":
    main()
