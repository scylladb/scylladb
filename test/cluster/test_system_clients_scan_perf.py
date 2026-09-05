# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Baseline/regression measurement for SCYLLADB-4230: system.clients scans
are quadratic in connection number, because clients_table::execute() (in
db/virtual_tables.cc) re-collects every connection on every shard, once
per shard, on every call.

Runs at concurrency=1 (single, isolated scan - not expected to move much
with the fix, since one query's shards already fan out concurrently with
each other) and at higher concurrency (the fix targets exactly this case:
many overlapping scans, e.g. several monitoring queries, coalescing onto
one in-flight collection instead of each re-triggering a full per-shard
collection). Also records server RSS before/after, since that's where the
concurrent-scan improvement actually shows up (see SCYLLADB-4230 notes).

Not a hard pass/fail perf gate (thresholds aren't portable across
machines) - meant to be run manually before/after a fix, with the printed
numbers compared by hand. Connection count is scaled down from the
500-800K/node mentioned in the ticket to keep this runnable on a laptop.
"""

import asyncio
import ipaddress
import logging
import os
import time

import aiohttp
import psutil
import pytest

from test.cluster.test_proxy_protocol import do_cql_handshake
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Kept modest on purpose - see module docstring. Override via env vars for
# manual before/after measurement runs with more connections/shards.
NUM_CONNECTIONS = int(os.environ.get("SCAN_PERF_CONNECTIONS", 2000))
NUM_SHARDS = int(os.environ.get("SCAN_PERF_SHARDS", 4))
NUM_CONCURRENT_SCANS = int(os.environ.get("SCAN_PERF_CONCURRENCY", 20))

RESULT_FILE = "/tmp/system_clients_scan_perf_result.txt"


async def open_idle_connection(host: str, port: int, local_addr: str | None = None):
    reader, writer = await asyncio.open_connection(host, port, local_addr=(local_addr, 0) if local_addr else None)
    await do_cql_handshake(reader, writer)
    return reader, writer


def server_rss_kb(pid: int) -> int:
    return psutil.Process(pid).memory_info().rss // 1024


async def allocated_memory_kb(ip_addr: str) -> int:
    # Actual malloc'd bytes (scylla_memory_allocated_memory) - steadier than RSS, which is noisy.
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{ip_addr}:9180/metrics") as resp:
            text = await resp.text()
    total = 0
    for line in text.splitlines():
        if line.startswith("scylla_memory_allocated_memory{"):
            total += int(float(line.rsplit(" ", 1)[1]))
    return total // 1024


async def sample_peaks(pid: int, ip_addr: str, stop: asyncio.Event, interval: float = 0.2) -> tuple[int, int]:
    # Before/after snapshots miss a transient spike freed before "after" is sampled - poll instead.
    peak_rss, peak_alloc = 0, 0
    while not stop.is_set():
        peak_rss = max(peak_rss, server_rss_kb(pid))
        peak_alloc = max(peak_alloc, await allocated_memory_kb(ip_addr))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass
    return peak_rss, peak_alloc


def record(label: str, values: list[float], rss_before: int, rss_after: int, rss_peak: int,
           alloc_before: int, alloc_after: int, alloc_peak: int) -> None:
    msg = (f"{label}: {NUM_CONNECTIONS} connections, smp={NUM_SHARDS}: {values}, "
           f"RSS before/after/peak (kB): {rss_before}/{rss_after}/{rss_peak}, "
           f"allocated before/after/peak (kB): {alloc_before}/{alloc_after}/{alloc_peak}")
    logger.info(msg)
    print(msg)
    # xdist swallows stdout/logging; drop a plain file so results survive test.py's capture.
    with open(RESULT_FILE, "a") as f:
        f.write(msg + "\n")


@pytest.mark.parametrize("concurrency", [1, NUM_CONCURRENT_SCANS])
async def test_system_clients_scan_perf(request, manager: ScyllaClusterManager, concurrency: int):
    # Default -m 1G / io-control-blocks=1000 can't sustain a fast burst of many
    # thousands of connections (AIO retry-queue assertion); scale both up here
    # rather than in the harness default, since this test alone opens this many.
    server = (await manager.servers_add(1, cmdline=[
        f'--smp={NUM_SHARDS}', '-m', '4G', '--max-networking-io-control-blocks', '10000',
    ]))[0]
    cql, _ = await manager.get_ready_cql([server])

    port = 9042
    # One source IP per connection (127.0.0.0/8 has 16M+ addresses) so the server
    # sees NUM_CONNECTIONS distinct client IPs, matching production - pooling many
    # connections onto a few IPs (as before) hides any per-client-IP cost/win.
    base_addr = int(ipaddress.IPv4Address("127.0.0.2"))
    local_addrs = [str(ipaddress.IPv4Address(base_addr + n)) for n in range(NUM_CONNECTIONS)]

    connections = []
    for i in range(0, NUM_CONNECTIONS, 500):
        connections += await asyncio.gather(*(
            open_idle_connection(server.ip_addr, port, local_addrs[j])
            for j in range(i, min(i + 500, NUM_CONNECTIONS))
        ))
    logger.info(f"Opened {len(connections)} idle CQL connections to {server.ip_addr}:{port}")

    try:
        rss_before = server_rss_kb(server.pid)
        alloc_before = await allocated_memory_kb(server.ip_addr)
        durations = []
        rows = []
        stop = asyncio.Event()
        peaks = asyncio.ensure_future(sample_peaks(server.pid, server.ip_addr, stop))
        for _ in range(5):
            start = time.monotonic()
            results = await asyncio.gather(*(
                cql.run_async("SELECT * FROM system.clients") for _ in range(concurrency)
            ))
            durations.append(time.monotonic() - start)
            rows = results[0]
        stop.set()
        rss_peak, alloc_peak = await peaks
        rss_after = server_rss_kb(server.pid)
        alloc_after = await allocated_memory_kb(server.ip_addr)
        record(f"concurrency={concurrency}", durations, rss_before, rss_after, rss_peak,
               alloc_before, alloc_after, alloc_peak)

        # Sanity check only - this is a perf measurement, not a correctness test
        # (test_clients in test_virtual_tables.py covers correctness).
        assert len(rows) >= NUM_CONNECTIONS
    finally:
        for _, writer in connections:
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for _, writer in connections), return_exceptions=True)
