# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import socket
import subprocess
import time
import logging
import asyncio
import pytest
from test import path_to
from test.pylib.host_registry import HostRegistry

logger = logging.getLogger(__name__)

# Testing whether perf-simple-query, perf-cql-raw, perf-alternator
# tools work correctly in in-process and remote modes, in various workloads

async def run(args):
    loop = asyncio.get_running_loop()
    def _run():
        return subprocess.run(args, check=True, text=True, timeout=600)
    try:
        output = await loop.run_in_executor(None, _run)
    except Exception as e:
        logger.error(e)
        raise e


@pytest.fixture(scope="module")
def scylla_path(build_mode):
    return path_to(build_mode, "scylla")


@pytest.mark.parametrize("mode", ["read"])
async def test_perf_simple_query(scylla_path, mode, tmp_path):
    args = [scylla_path, "perf-simple-query", "--duration", "1", "--partitions", "1000"]
    await run(args)


@pytest.mark.parametrize("workload", ["read", "write"])
async def test_perf_cql_raw(scylla_path, tmp_path, workload):
    hosts = HostRegistry()
    host = await hosts.lease_host()
    cmd = [
        scylla_path, "perf-cql-raw",
        '--api-address', host,
        '--rpc-address', host,
        '--listen-address', host,
        '--prometheus-address', host,
        '--seed-provider-parameters', 'seeds=' + host,
        "--workload", workload,
        "--duration", "1",
        "--smp", "2",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
        "--partitions", "1000"
    ]
    try:
        await run(cmd)
    finally:
         await hosts.release_host(host)


@pytest.mark.parametrize("workload", ["write"])
async def test_perf_alternator(scylla_path, tmp_path, workload):
    hosts = HostRegistry()
    host = await hosts.lease_host()
    cmd = [
        scylla_path, "perf-alternator",
        '--api-address', host,
        '--rpc-address', host,
        '--listen-address', host,
        '--prometheus-address', host,
        '--alternator-address', host,
        '--seed-provider-parameters', 'seeds=' + host,
        "--alternator-port", "8000",
        "--alternator-write-isolation", "only_rmw_uses_lwt",
        "--workload", workload,
        "--duration", "1",
        "--smp", "2",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
        "--partitions", "1000"
    ]
    try:
        await run(cmd)
    finally:
         await hosts.release_host(host)


@pytest.mark.parametrize("workload", ["read"])
async def test_perf_cql_raw_remote(scylla_path, tmp_path, workload, manager):
    await manager.server_add()
    servers = await manager.running_servers()
    await manager.get_ready_cql(servers)
    host = servers[0].ip_addr
    client_cmd = [
        scylla_path, "perf-cql-raw",
        "--workload", workload,
        "--duration", "1",
        "--remote-host", host,
        "--smp", "1",
        "--partitions", "1000"
    ]
    await run(client_cmd)


@pytest.mark.parametrize("workload", ["read"])
async def test_perf_alternator_remote(scylla_path, tmp_path, workload, manager):
    await manager.server_add(cmdline=[
        "--alternator-port", "8000",
        "--alternator-write-isolation", "only_rmw_uses_lwt"
    ])
    servers = await manager.running_servers()
    await manager.get_ready_cql(servers)
    host = servers[0].ip_addr
    client_cmd = [
        scylla_path, "perf-alternator",
        "--workload", workload,
        "--duration", "1",
        "--remote-host", host,
        "--smp", "1",
        "--partitions", "1000"
    ]
    await run(client_cmd)
