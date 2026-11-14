# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Manual SCYLLADB-4230 before/after measurement of system.clients scans; perf thresholds aren't
portable, so it only prints numbers. Run with SCAN_PERF_ENABLE=1.
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
from test.pylib.skip_types import skip_env

logger = logging.getLogger(__name__)

# Small enough for a laptop; override for larger manual runs.
NUM_CONNECTIONS = int(os.environ.get("SCAN_PERF_CONNECTIONS", 2000))
NUM_SHARDS = int(os.environ.get("SCAN_PERF_SHARDS", 4))
NUM_CONCURRENT_SCANS = int(os.environ.get("SCAN_PERF_CONCURRENCY", 20))

# Per-worker, per-run name so the /tmp path isn't predictable.
RESULT_FILE = (
    f"/tmp/system_clients_scan_perf_result.{os.environ.get('PYTEST_XDIST_WORKER', 'master')}.{os.getpid()}.txt"
)


async def open_idle_connection(host: str, port: int, local_addr: str | None = None):
    reader, writer = await asyncio.open_connection(host, port, local_addr=(local_addr, 0) if local_addr else None)
    try:
        await do_cql_handshake(reader, writer)
    except BaseException:
        writer.close()
        await writer.wait_closed()
        raise
    return reader, writer


def server_rss_kb(pid: int) -> int:
    return psutil.Process(pid).memory_info().rss // 1024


def server_cpu_s(pid: int) -> float:
    # Server-side work is tiny next to client-dominated wall clock; CPU time resolves it.
    t = psutil.Process(pid).cpu_times()
    return t.user + t.system


async def allocated_memory_kb(ip_addr: str) -> int | None:
    # Allocated bytes are steadier than RSS; best-effort, since the metrics port can stall under
    # connection churn and this is not a correctness check.
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(f"http://{ip_addr}:9180/metrics") as resp:
                text = await resp.text()
    except (aiohttp.ClientError, TimeoutError):
        return None
    total = 0
    for line in text.splitlines():
        if line.startswith("scylla_memory_allocated_memory{"):
            total += int(float(line.rsplit(" ", 1)[1]))
    return total // 1024


async def sample_peaks(pid: int, ip_addr: str, stop: asyncio.Event, interval: float = 0.2) -> tuple[int, int | None]:
    # Before/after samples miss a spike freed before "after", so poll.
    peak_rss, peak_alloc = 0, None
    while not stop.is_set():
        peak_rss = max(peak_rss, server_rss_kb(pid))
        alloc = await allocated_memory_kb(ip_addr)
        if alloc is not None:
            peak_alloc = alloc if peak_alloc is None else max(peak_alloc, alloc)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass
    return peak_rss, peak_alloc


def record(label: str, values: list[float], rss_before: int, rss_after: int, rss_peak: int,
           alloc_before: int | None, alloc_after: int | None, alloc_peak: int | None, cpu_s: float) -> None:
    msg = (f"{label}: {NUM_CONNECTIONS} connections, smp={NUM_SHARDS}: {values}, server CPU (s): {cpu_s:.3f}, "
           f"RSS before/after/peak (kB): {rss_before}/{rss_after}/{rss_peak}, "
           f"allocated before/after/peak (kB): {alloc_before}/{alloc_after}/{alloc_peak}")
    logger.info(msg)
    print(msg)
    # xdist swallows stdout, so also append to a file; O_NOFOLLOW refuses a planted symlink, and no
    # O_EXCL because each parametrized run appends.
    fd = os.open(RESULT_FILE, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "a") as f:
        f.write(msg + "\n")


@pytest.mark.perf
@pytest.mark.manual
@pytest.mark.parametrize("concurrency", [1, NUM_CONCURRENT_SCANS])
async def test_system_clients_scan_perf(manager: ScyllaClusterManager, concurrency: int):
    if not os.environ.get("SCAN_PERF_ENABLE"):
        skip_env("manual perf test; set SCAN_PERF_ENABLE=1 to run")

    # The defaults can't absorb a burst of thousands of connections (AIO retry-queue assertion),
    # and only this test needs more.
    server = (await manager.servers_add(1, cmdline=[
        f'--smp={NUM_SHARDS}', '-m', '4G',
        # One control block per pending read; must cover every connection on a shard.
        '--max-networking-io-control-blocks', str(max(10000, NUM_CONNECTIONS)),
    ]))[0]
    cql, _ = await manager.get_ready_cql([server])

    port = 9042
    # One source IP per connection, as in production; pooling onto a few IPs hides per-IP costs.
    base_addr = int(ipaddress.IPv4Address("127.0.0.2"))
    local_addrs = [str(ipaddress.IPv4Address(base_addr + n)) for n in range(NUM_CONNECTIONS)]

    connections = []
    try:
        # gather() doesn't cancel the rest on error, so collect every result to close what did open.
        for i in range(0, NUM_CONNECTIONS, 500):
            batch = await asyncio.gather(*(
                open_idle_connection(server.ip_addr, port, local_addrs[j])
                for j in range(i, min(i + 500, NUM_CONNECTIONS))
            ), return_exceptions=True)
            # Track every opened connection before raising, or later successes in the batch leak.
            exc = None
            for item in batch:
                if isinstance(item, BaseException):
                    exc = exc or item
                else:
                    connections.append(item)
            if exc is not None:
                raise exc
        logger.info(f"Opened {len(connections)} idle CQL connections to {server.ip_addr}:{port}")

        rss_before = server_rss_kb(server.pid)
        alloc_before = await allocated_memory_kb(server.ip_addr)
        cpu_before = server_cpu_s(server.pid)
        durations = []
        rows = []
        stop = asyncio.Event()
        peaks = asyncio.ensure_future(sample_peaks(server.pid, server.ip_addr, stop))
        try:
            for _ in range(5):
                start = time.monotonic()
                results = await asyncio.gather(*(
                    cql.run_async("SELECT * FROM system.clients") for _ in range(concurrency)
                ))
                durations.append(time.monotonic() - start)
                rows = results[0]
        finally:
            # Stop the sampler even on failure, or it keeps polling a server being torn down.
            stop.set()
            rss_peak, alloc_peak = await peaks
        cpu_s = server_cpu_s(server.pid) - cpu_before
        rss_after = server_rss_kb(server.pid)
        alloc_after = await allocated_memory_kb(server.ip_addr)
        record(f"concurrency={concurrency}", durations, rss_before, rss_after, rss_peak,
               alloc_before, alloc_after, alloc_peak, cpu_s)

        # Sanity check only; test_virtual_tables.py covers correctness.
        assert len(rows) >= NUM_CONNECTIONS
    finally:
        for _, writer in connections:
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for _, writer in connections), return_exceptions=True)
