#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeberussss_mobile_patch_wakelock_top20.py
Zeberus IPTV Checker — Termux/Android
Patches applied:
  - Termux wake lock activation at startup
  - auto_proxy_test: removed redundant test_single_proxy calls during sort
  - categorize_and_save_hits: goruntu_durumu set correctly for ADULT and GORUNTULU
  - DashboardState.classify_hit / categorize_and_save_hits: unified classification logic
  - post_check_all_hits: adult field preserved after post-check
  - format_categories_for_text: cleaned up unnecessary whitespace
  - save_master_file: uses runtime counters (state.goruntulu_hits, state.adult_hits, state.goruntusuz_hits)
  - run_scan: safe done=True assignment on KeyboardInterrupt
  - server_worker: Empty exception guard added
  - M3U links written in full, correct format
"""

from __future__ import annotations

import asyncio
import os
import queue
import subprocess
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from queue import Empty
from typing import Dict, List, Optional, Tuple

import aiohttp

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VERSION = "top20-wakelock"
MAX_WORKERS = 20
TIMEOUT = 10
RETRY = 2
M3U_HEADER = "#EXTM3U\n"
M3U_INF_TEMPLATE = '#EXTINF:-1 tvg-id="{tid}" tvg-name="{name}" group-title="{group}",{name}\n'
USER_AGENT = "Mozilla/5.0 (Linux; Android 11; Termux) AppleWebKit/537.36"

CATEGORY_GORUNTULU = "GORUNTULU"
CATEGORY_ADULT = "ADULT"
CATEGORY_GORUNTUSUZ = "GORUNTUSUZ"


# ---------------------------------------------------------------------------
# Termux wake lock
# ---------------------------------------------------------------------------

def enable_wakelock() -> None:
    """Activate Termux wake lock to prevent CPU sleep during scan."""
    try:
        result = subprocess.run(
            ["termux-wake-lock"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            print("[WAKELOCK] Termux wake lock activated.")
        else:
            print("[WAKELOCK] termux-wake-lock returned non-zero; continuing anyway.")
    except FileNotFoundError:
        print("[WAKELOCK] termux-wake-lock not found (not running on Termux?); skipping.")
    except Exception as exc:
        print(f"[WAKELOCK] Warning: could not activate wake lock: {exc}")


def disable_wakelock() -> None:
    """Release Termux wake lock."""
    try:
        subprocess.run(["termux-wake-unlock"], capture_output=True, timeout=5)
        print("[WAKELOCK] Termux wake lock released.")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ProxyResult:
    proxy: str
    latency: float = float("inf")
    alive: bool = False


@dataclass
class HitRecord:
    server: str
    username: str
    password: str
    streams: int = 0
    adult: bool = False
    alive: bool = True
    goruntu_durumu: str = CATEGORY_GORUNTUSUZ
    extra: Dict = field(default_factory=dict)


@dataclass
class DashboardState:
    goruntulu_hits: int = 0
    adult_hits: int = 0
    goruntusuz_hits: int = 0
    total_checked: int = 0
    total_hits: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, compare=False, repr=False)

    # ------------------------------------------------------------------
    # Classification — single authoritative method used everywhere
    # ------------------------------------------------------------------

    @staticmethod
    def classify_hit(hit: HitRecord) -> str:
        """Return one of ADULT / GORUNTULU / GORUNTUSUZ for *hit*.

        Rules (applied in priority order):
          1. adult flag set  → ADULT
          2. streams > 0     → GORUNTULU
          3. otherwise       → GORUNTUSUZ
        """
        if hit.adult:
            return CATEGORY_ADULT
        if hit.streams > 0:
            return CATEGORY_GORUNTULU
        return CATEGORY_GORUNTUSUZ

    def register_hit(self, hit: HitRecord) -> None:
        category = self.classify_hit(hit)
        with self._lock:
            self.total_hits += 1
            if category == CATEGORY_ADULT:
                self.adult_hits += 1
            elif category == CATEGORY_GORUNTULU:
                self.goruntulu_hits += 1
            else:
                self.goruntusuz_hits += 1

    def increment_checked(self) -> None:
        with self._lock:
            self.total_checked += 1


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------

async def test_single_proxy(
    session: aiohttp.ClientSession,
    proxy: str,
    test_url: str = "http://example.com",
) -> ProxyResult:
    """Test a single proxy and return its latency."""
    t0 = time.monotonic()
    try:
        async with session.get(
            test_url,
            proxy=f"http://{proxy}",
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
        ) as resp:
            await resp.read()
            latency = time.monotonic() - t0
            return ProxyResult(proxy=proxy, latency=latency, alive=resp.status < 400)
    except Exception:
        return ProxyResult(proxy=proxy, latency=float("inf"), alive=False)


async def auto_proxy_test(proxies: List[str], test_url: str = "http://example.com") -> List[str]:
    """Test all proxies concurrently and return them sorted by latency.

    FIX: Sorting is done on the results already collected — test_single_proxy is
    NOT called again during the sort key computation, eliminating 2 extra requests
    per proxy that the previous implementation incurred.
    """
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        tasks = [test_single_proxy(session, p, test_url) for p in proxies]
        results: List[ProxyResult] = await asyncio.gather(*tasks)

    # Sort by latency using the already-collected ProxyResult objects.
    results.sort(key=lambda r: r.latency)
    return [r.proxy for r in results if r.alive]


# ---------------------------------------------------------------------------
# Categorise & save hits
# ---------------------------------------------------------------------------

def categorize_and_save_hits(
    hits: List[HitRecord],
    state: DashboardState,
    output_dir: str = "hits",
) -> Dict[str, List[HitRecord]]:
    """Categorize hits and write them to per-category files.

    FIX: goruntu_durumu is set to ADULT when hit.adult is True, and to
    GORUNTULU when hit.streams > 0 — using the same rules as
    DashboardState.classify_hit so that both code paths are consistent.
    """
    os.makedirs(output_dir, exist_ok=True)

    buckets: Dict[str, List[HitRecord]] = defaultdict(list)

    for hit in hits:
        # Use the same authoritative classification logic as DashboardState
        category = DashboardState.classify_hit(hit)

        # FIX: goruntu_durumu is set correctly for both ADULT and GORUNTULU
        hit.goruntu_durumu = category

        buckets[category].append(hit)

    for category, bucket in buckets.items():
        filepath = os.path.join(output_dir, f"{category.lower()}_hits.txt")
        with open(filepath, "w", encoding="utf-8") as fh:
            for hit in bucket:
                fh.write(f"{hit.server}|{hit.username}|{hit.password}\n")

    return dict(buckets)


# ---------------------------------------------------------------------------
# Post-check
# ---------------------------------------------------------------------------

async def post_check_single(
    session: aiohttp.ClientSession,
    hit: HitRecord,
) -> HitRecord:
    """Re-validate a hit after the main scan.

    FIX: The adult field is preserved; previous code overwrote it with False
    after the post-check regardless of what was detected.
    """
    url = f"{hit.server}/player_api.php?username={hit.username}&password={hit.password}&action=get_live_categories"
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
        ) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                # Derive stream count from categories
                hit.streams = len(data) if isinstance(data, list) else 0
                # FIX: do NOT touch hit.adult here — preserve whatever was detected
                # during the main scan.
            else:
                hit.alive = False
    except Exception:
        pass
    return hit


async def post_check_all_hits(hits: List[HitRecord]) -> List[HitRecord]:
    """Run post-check for all hits concurrently."""
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        tasks = [post_check_single(session, hit) for hit in hits]
        updated: List[HitRecord] = await asyncio.gather(*tasks)
    return list(updated)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_categories_for_text(buckets: Dict[str, List[HitRecord]]) -> str:
    """Return a human-readable summary of categorised hits.

    FIX: Trailing/leading whitespace on each line removed.
    """
    lines: List[str] = []
    for category in (CATEGORY_ADULT, CATEGORY_GORUNTULU, CATEGORY_GORUNTUSUZ):
        count = len(buckets.get(category, []))
        lines.append(f"{category}: {count}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# M3U generation
# ---------------------------------------------------------------------------

def build_m3u_line(hit: HitRecord, channel_name: str, group: str) -> str:
    """Return a complete M3U entry (EXTINF + URL) for *hit*.

    FIX: URLs are written in the full, correct xtream-codes format.
    """
    inf = M3U_INF_TEMPLATE.format(
        tid=channel_name,
        name=channel_name,
        group=group,
    )
    url = f"{hit.server}/live/{hit.username}/{hit.password}/{channel_name}.ts\n"
    return inf + url


def write_m3u_file(
    hits: List[HitRecord],
    filepath: str,
    channels: Optional[List[Tuple[str, str]]] = None,
) -> None:
    """Write an M3U playlist file for the given hits.

    *channels* is a list of (channel_name, group) pairs.  If omitted a
    placeholder entry per hit is written.
    """
    if channels is None:
        channels = [(f"channel_{i}", "General") for i in range(len(hits))]

    with open(filepath, "w", encoding="utf-8") as fh:
        fh.write(M3U_HEADER)
        for hit, (ch_name, group) in zip(hits, channels):
            fh.write(build_m3u_line(hit, ch_name, group))


# ---------------------------------------------------------------------------
# Master file
# ---------------------------------------------------------------------------

def save_master_file(
    state: DashboardState,
    hits: List[HitRecord],
    filepath: str = "master_hits.txt",
) -> None:
    """Save all hits to a master file with runtime counter summary.

    FIX: Uses state.goruntulu_hits, state.adult_hits, state.goruntusuz_hits
    (runtime counters) instead of re-counting from the hits list, so the
    totals always match what the dashboard displayed.
    """
    with open(filepath, "w", encoding="utf-8") as fh:
        fh.write(f"# Total: {state.total_hits}\n")
        fh.write(f"# Goruntulu: {state.goruntulu_hits}\n")
        fh.write(f"# Adult: {state.adult_hits}\n")
        fh.write(f"# Goruntusuz: {state.goruntusuz_hits}\n")
        fh.write(f"# Checked: {state.total_checked}\n\n")
        for hit in hits:
            fh.write(
                f"{hit.server}|{hit.username}|{hit.password}"
                f"|{hit.goruntu_durumu}|streams={hit.streams}\n"
            )


# ---------------------------------------------------------------------------
# Server worker
# ---------------------------------------------------------------------------

async def check_server(
    session: aiohttp.ClientSession,
    server: str,
    credential: Tuple[str, str],
) -> Optional[HitRecord]:
    """Check a single (server, username, password) credential."""
    username, password = credential
    url = (
        f"{server}/player_api.php"
        f"?username={username}&password={password}"
    )
    for attempt in range(RETRY + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                headers={"User-Agent": USER_AGENT},
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                user_info = data.get("user_info", {})
                if user_info.get("auth") != 1:
                    return None
                streams_info = data.get("server_info", {})
                adult = bool(user_info.get("is_trial") == "0" and "adult" in str(data).lower())
                hit = HitRecord(
                    server=server,
                    username=username,
                    password=password,
                    streams=int(user_info.get("active_cons", 0)),
                    adult=adult,
                    extra=streams_info,
                )
                return hit
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < RETRY:
                await asyncio.sleep(0.5)
    return None


def server_worker(
    job_queue: queue.Queue,
    result_list: List[HitRecord],
    state: DashboardState,
    stop_event: threading.Event,
) -> None:
    """Worker thread: pulls (server, credential) jobs from the queue and checks them.

    FIX: Empty exception guard added so that the worker exits cleanly when the
    queue is drained and a timeout fires instead of raising an unhandled exception.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _run_check(server: str, credential: Tuple[str, str]) -> None:
        async with aiohttp.ClientSession() as session:
            hit = await check_server(session, server, credential)
        if hit:
            result_list.append(hit)
            state.register_hit(hit)
        state.increment_checked()

    try:
        while not stop_event.is_set():
            try:
                server, credential = job_queue.get(timeout=1.0)
            except Empty:
                # FIX: queue drained or timeout — check stop_event and loop
                continue
            try:
                loop.run_until_complete(_run_check(server, credential))
            finally:
                job_queue.task_done()
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Main scan orchestrator
# ---------------------------------------------------------------------------

@dataclass
class ServerEntry:
    url: str
    done: bool = False


def run_scan(
    servers: List[str],
    credentials: List[Tuple[str, str]],
    num_workers: int = MAX_WORKERS,
) -> Tuple[List[HitRecord], DashboardState]:
    """Orchestrate the full scan and return (hits, state).

    FIX: On KeyboardInterrupt every server's done flag is set to True inside a
    try/except so that one missing attribute or race condition does not mask the
    original interrupt.
    """
    state = DashboardState()
    hits: List[HitRecord] = []
    job_queue: queue.Queue = queue.Queue()
    stop_event = threading.Event()

    server_entries = [ServerEntry(url=s) for s in servers]

    # Populate the queue
    for entry in server_entries:
        for cred in credentials:
            job_queue.put((entry.url, cred))

    workers = []
    for _ in range(num_workers):
        t = threading.Thread(
            target=server_worker,
            args=(job_queue, hits, state, stop_event),
            daemon=True,
        )
        t.start()
        workers.append(t)

    try:
        job_queue.join()
    except KeyboardInterrupt:
        print("\n[SCAN] KeyboardInterrupt received — stopping workers…")
        stop_event.set()
        # FIX: safe done=True — wrapped individually so one failure doesn't
        # prevent the rest from being marked done.
        for entry in server_entries:
            try:
                entry.done = True
            except Exception:
                pass
    finally:
        stop_event.set()
        for t in workers:
            t.join(timeout=5)

    return hits, state


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # FIX: Wake lock must be the very first thing we do so the device
    # cannot sleep during a long scan.
    enable_wakelock()

    try:
        print(f"[MAIN] Zeberus IPTV Checker {VERSION} starting…")

        # --- configuration (edit as needed) ---
        servers: List[str] = []
        credentials: List[Tuple[str, str]] = []
        proxies: List[str] = []
        output_dir = "output"

        if not servers:
            print("[MAIN] No servers configured — exiting.")
            return

        # Proxy test (optional)
        if proxies:
            print(f"[MAIN] Testing {len(proxies)} proxies…")
            alive_proxies = asyncio.run(auto_proxy_test(proxies))
            print(f"[MAIN] {len(alive_proxies)} proxies alive.")

        # Main scan
        print(f"[MAIN] Scanning {len(servers)} servers × {len(credentials)} credentials…")
        hits, state = run_scan(servers, credentials)

        # Post-check
        if hits:
            print(f"[MAIN] Post-checking {len(hits)} hits…")
            hits = asyncio.run(post_check_all_hits(hits))

        # Categorise
        buckets = categorize_and_save_hits(hits, state, output_dir=output_dir)

        # Summary
        print(format_categories_for_text(buckets))

        # Master file
        save_master_file(state, hits, filepath=os.path.join(output_dir, "master_hits.txt"))

        # M3U
        m3u_path = os.path.join(output_dir, "playlist.m3u")
        write_m3u_file(hits, m3u_path)
        print(f"[MAIN] M3U written → {m3u_path}")

    finally:
        disable_wakelock()


if __name__ == "__main__":
    main()
