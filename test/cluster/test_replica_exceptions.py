# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import re
import requests
import time

from test.cluster.conftest import cluster_con, manager
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql, wait_for_cql_and_get_hosts

from .util import new_test_keyspace

def get_database_rate_limit_error_metrics(host, op) -> int:
    assert op in ["reads", "writes"]

    metrics = requests.get(f"http://{host}:9180/metrics").text
    metric_name = f"scylla_database_total_{op}_rate_limited"
    pattern = re.compile(r'^' + re.escape(metric_name) + r'\{shard="\d+"\} (\d+)')

    result = 0
    for metric_line in metrics.split('\n'):
        match = pattern.match(metric_line)
        if match:
            count = int(match.group(1))
            result += count

    return result

def get_cpp_exceptions_metrics(host) -> int:
    metrics = requests.get(f"http://{host}:9180/metrics").text
    pattern = re.compile(r'^scylla_reactor_cpp_exceptions\{shard="\d+"\} (\d+)')

    result = 0
    for metric_line in metrics.split('\n'):
        match = pattern.match(metric_line)
        if match:
            count = int(match.group(1))
            result += count

    return result

async def _test_replica_rate_limit_no_cpp_exceptions(manager: ManagerClient, servers: list, op: str, max_per_second: int = 1):
    assert len(servers) == 1
    hosts = await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)

    host_ip = servers[0].ip_addr
    host = hosts[0]
    cql = cluster_con([host_ip]).connect()
    await wait_for_cql(cql, host, time.time() + 60)

    run_count = 1000
    cpp_exception_threshold = 20

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host_ip)
    rate_limit_exception_before = get_database_rate_limit_error_metrics(host_ip, op)

    limit_spec = ""
    if op == "reads":
        limit_spec = f"'max_reads_per_second': {max_per_second}"
    else:
        limit_spec = f"'max_writes_per_second': {max_per_second}"

    # Create a temporary keyspace and table with a very small per-partition write limit.
    # Use the cqlpy helper new_test_keyspace which provides an isolated keyspace name.
    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", host) as keyspace:
        ks = keyspace
        tbl = "t"

        try:
            cql.execute(f"CREATE TABLE IF NOT EXISTS {ks}.{tbl} (p int, c int, PRIMARY KEY (p)) WITH per_partition_rate_limit = {{{limit_spec}}}")

            # For reads ensure there is at least one row to read repeatedly
            if op == "reads":
                cql.execute(f"INSERT INTO {ks}.{tbl} (p, c) VALUES (1, 0)")

            futures = []
            for i in range(run_count):
                q = ""
                if op == "reads":
                    q = f"SELECT * FROM {ks}.{tbl} WHERE p = 1"
                else:
                    q = f"INSERT INTO {ks}.{tbl} (p, c) VALUES (1, {2*i})"
                try:
                    futures.append(cql.execute_async(q))
                except Exception:
                    # ignore client-side exceptions; we verify protocol errors via metrics
                    pass
            
            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    pass
        finally:
            cql.execute(f"DROP TABLE IF EXISTS {ks}.{tbl}")

    rate_limit_exception_after = get_database_rate_limit_error_metrics(host_ip, op)
    rate_limit_exception_diff = rate_limit_exception_after - rate_limit_exception_before
    assert rate_limit_exception_diff > run_count * 0.9, "Expected rate limit errors to increase close to run count"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host_ip)
    cpp_exception_metrics_diff = cpp_exception_metrics_after - cpp_exception_metrics_before
    assert cpp_exception_metrics_diff <= cpp_exception_threshold, "Expected C++ exceptions to not increase"

async def test_replica_do_apply_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    servers = await manager.servers_add(1)
    host_ip = servers[0].ip_addr

    try:
        await manager.api.enable_injection(host_ip, "rate_limit_force_defer", one_shot=False)
        await _test_replica_rate_limit_no_cpp_exceptions(manager, servers, "writes")
    finally:
        await manager.api.disable_injection(host_ip, "rate_limit_force_defer")

async def test_replica_query_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    servers = await manager.servers_add(1)
    host_ip = servers[0].ip_addr

    try:
        await manager.api.enable_injection(host_ip, "rate_limit_force_defer", one_shot=False)
        await _test_replica_rate_limit_no_cpp_exceptions(manager, servers, "reads")
    finally:
        await manager.api.disable_injection(host_ip, "rate_limit_force_defer")
