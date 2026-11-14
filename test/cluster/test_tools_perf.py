# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import boto3
from botocore.config import Config
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
    args = [scylla_path, "perf-simple-query", "--duration", "1", "--partitions", "100", "--stop-on-error", "false", "--memory", "2G", "--smp", "2", "--overprovisioned"]
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
        "--memory", "2G",
        "--smp", "2",
        "--overprovisioned",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
        # A single node has no one to announce its shutdown to.
        "--shutdown-announce-in-ms", "0",
        "--partitions", "100",
        "--continue-after-error", "true"
    ]
    try:
        await run(cmd)
    finally:
         await hosts.release_host(host)


@pytest.mark.parametrize("workload", ["write", "batch_read"])
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
        "--memory", "2G",
        "--smp", "2",
        "--overprovisioned",
        "--workdir", str(tmp_path),
        "--developer-mode", "1",
        # A single node has no one to announce its shutdown to.
        "--shutdown-announce-in-ms", "0",
        "--partitions", "100",
        "--continue-after-error", "true"
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
        "--memory", "1G",
        "--smp", "1",
        "--overprovisioned",
        "--partitions", "100",
        "--continue-after-error", "true"
    ]
    await run(client_cmd)


@pytest.mark.parametrize("workload", ["read", "batch_read"])
async def test_perf_alternator_remote(scylla_path, tmp_path, workload, manager):
    server = await manager.server_add(cmdline=[
        "--alternator-port", "8000",
        "--alternator-write-isolation", "only_rmw_uses_lwt"
    ])
    host = server.ip_addr
    client_cmd = [
        scylla_path, "perf-alternator",
        "--workload", workload,
        "--duration", "1",
        "--remote-host", host,
        "--memory", "1G",
        "--smp", "1",
        "--overprovisioned",
        "--partitions", "100",
        "--continue-after-error", "true"
    ]
    await run(client_cmd)


def item_count(ddb, table):
    """The number of items in the table, or None if there is no such table."""
    try:
        pages = ddb.get_paginator('scan').paginate(TableName=table, Select='COUNT', ConsistentRead=True)
        return sum(page['Count'] for page in pages)
    except ddb.exceptions.ResourceNotFoundException:
        return None


async def test_perf_alternator_populate(scylla_path, manager):
    """The populate workload prepares tables with data. Runs with
    --prepopulate-partitions 0 read and write that data and keep the tables.
    A default run still creates and deletes its own table."""
    server = await manager.server_add(cmdline=[
        "--alternator-port", "8000",
        "--alternator-write-isolation", "only_rmw_uses_lwt"
    ])
    host = server.ip_addr
    # The test checks which tables exist and that the reads get items, not
    # the throughput, so a small table and a few requests per run are enough.
    partitions = 100
    # The server does not check the credentials.
    ddb = boto3.client('dynamodb', endpoint_url=f"http://{host}:8000", region_name='us-east-1',
        aws_access_key_id='alternator', aws_secret_access_key='secret_pass',
        config=Config(retries={"max_attempts": 0}, read_timeout=300))

    def client_cmd(workload, *extra):
        # No --continue-after-error: a failed request fails the run.
        return [
            scylla_path, "perf-alternator",
            "--workload", workload,
            "--remote-host", host,
            "--memory", "1G", "--smp", "1", "--overprovisioned",
            "--partitions", str(partitions),
            "--operations-per-shard", "10",
            "--concurrency", "10",
            *extra,
        ]
    use_tables = ["--prepopulate-partitions", "0"]

    async def scanned_items():
        # perf-alternator counts a Scan that returns no items as a success.
        # The '{' keeps the histogram with the same name prefix out of the sum.
        return (await manager.metrics.query(host)).get('scylla_alternator_returned_items{') or 0

    # Without the tables, a run that uses them fails.
    with pytest.raises(subprocess.CalledProcessError):
        await run(client_cmd("read", *use_tables))

    await run(client_cmd("populate"))
    assert item_count(ddb, "workloads_test") == partitions
    assert item_count(ddb, "workloads_test_gsi") == 0

    # Each key has an item before and after each run, so the reads got items
    # and no run made a new table.
    before = await scanned_items()
    for workload in ["read", "batch_read", "scan", "write_rmw"]:
        await run(client_cmd(workload, *use_tables))
        assert item_count(ddb, "workloads_test") == partitions
    assert await scanned_items() > before

    # A GSI workload writes to the other table and keeps it.
    await run(client_cmd("write_gsi", *use_tables))
    assert item_count(ddb, "workloads_test_gsi") > 0
    assert item_count(ddb, "workloads_test") == partitions

    # A default run owns its table: it creates it, fills it for a read
    # workload and deletes it. It does not change the other table.
    before = await scanned_items()
    await run(client_cmd("scan"))
    assert await scanned_items() > before
    assert item_count(ddb, "workloads_test") is None
    assert item_count(ddb, "workloads_test_gsi") > 0
    await run(client_cmd("write_gsi"))
    assert item_count(ddb, "workloads_test_gsi") is None
