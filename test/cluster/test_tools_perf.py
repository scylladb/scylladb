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

logger = logging.getLogger(__name__)

async def run_and_log(args, **kwargs):
    loop = asyncio.get_running_loop()
    def _run():
        return subprocess.check_output(args, **kwargs)
    try:
        output = await loop.run_in_executor(None, _run)
    except Exception as e:
        logger.error(e)
        raise e

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

@pytest.fixture(scope="module")
def scylla_path(build_mode):
    return path_to(build_mode, "scylla")

# Testing whether perf-simple-query, perf-cql-raw, perf-alternator
# tools work correctly in in-process and remote modes, in various workloads

@pytest.mark.parametrize("mode", ["read", "write", "delete"])
async def test_perf_simple_query(scylla_path, mode, tmp_path):
    args = [scylla_path, "perf-simple-query", "--duration", "1"]
    if mode == "write":
        args.append("--write")
    elif mode == "delete":
        args.append("--delete")
    await run_and_log(args, text=True, timeout=600, stderr=subprocess.STDOUT)


@pytest.mark.parametrize("workload", ["read", "write", "connect"])
async def test_perf_cql_raw(scylla_path, tmp_path, workload):
    port = find_free_port()
    cmd = [
        scylla_path, "perf-cql-raw",
        "--workload", workload,
        "--duration", "1",
        "--smp", "2",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
        "--native-transport-port", str(port),
    ]
    await run_and_log(cmd, text=True, timeout=600, stderr=subprocess.STDOUT)


@pytest.mark.parametrize("workload", ["read", "scan", "write", "write_gsi", "write_rmw"])
async def test_perf_alternator(scylla_path, tmp_path, workload):
    port = find_free_port()
    cmd = [
        scylla_path, "perf-alternator",
        "--alternator-port", str(port),
        "--alternator-write-isolation", "only_rmw_uses_lwt",
        "--workload", workload,
        "--duration", "1",
        "--smp", "2",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
    ]
    await run_and_log(cmd, text=True, timeout=600, stderr=subprocess.STDOUT)


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
    ]
    await run_and_log(client_cmd, text=True, timeout=600, stderr=subprocess.STDOUT)


@pytest.mark.parametrize("workload", ["read"])
async def test_perf_alternator_remote(scylla_path, tmp_path, workload, manager):
    port = find_free_port()
    await manager.server_add(cmdline=[
        "--alternator-port", str(port),
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
        "--remote-port", str(port),
        "--smp", "1",
    ]
    await run_and_log(client_cmd, text=True, timeout=600, stderr=subprocess.STDOUT)
