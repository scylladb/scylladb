# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import re
import requests

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient

from .util import new_test_keyspace, unique_name

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

def get_database_timedout_error_metrics(host) -> int:
    metrics = requests.get(f"http://{host}:9180/metrics").text
    metric_name = f"scylla_database_total_writes_timedout"
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
    cql, hosts = await manager.get_ready_cql(servers)

    host_ip = servers[0].ip_addr
    host = hosts[0]

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
        tbl = unique_name()

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

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_do_apply_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    servers = await manager.servers_add(1)
    host_ip = servers[0].ip_addr

    try:
        await manager.api.enable_injection(host_ip, "rate_limit_force_defer", one_shot=False)
        await _test_replica_rate_limit_no_cpp_exceptions(manager, servers, "writes")
    finally:
        await manager.api.disable_injection(host_ip, "rate_limit_force_defer")

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_query_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    servers = await manager.servers_add(1)
    host_ip = servers[0].ip_addr

    try:
        await manager.api.enable_injection(host_ip, "rate_limit_force_defer", one_shot=False)
        await _test_replica_rate_limit_no_cpp_exceptions(manager, servers, "reads")
    finally:
        await manager.api.disable_injection(host_ip, "rate_limit_force_defer")


async def _test_replica_counter_timeout(manager: ManagerClient, servers: list, run_count: int):
    cql, hosts = await manager.get_ready_cql(servers)

    host = hosts[0]

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' } AND TABLETS = { 'enabled': false }", host) as keyspace:
        ks = keyspace
        tbl = unique_name()

        try:
            cql.execute(f"CREATE TABLE IF NOT EXISTS {ks}.{tbl} (p int, c counter, PRIMARY KEY (p))")

            futures = []
            for _ in range(run_count):
                try:
                    f = (cql.execute_async(f"UPDATE {ks}.{tbl} SET c = c + 1 WHERE p = 1"))
                    futures.append(f)
                except Exception:
                    pass

            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass
        finally:
            cql.execute(f"DROP TABLE IF EXISTS {ks}.{tbl}")

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_writes_apply_counter_update_timeout(manager: ManagerClient):
    servers = await manager.servers_add(1)
    host_ip = servers[0].ip_addr

    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + 10 * run_count

    writes_timedout_exception_before = get_database_timedout_error_metrics(host_ip)
    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host_ip)

    try:
        await manager.api.enable_injection(host_ip, "database_apply_counter_update_force_timeout", one_shot=False)
        await _test_replica_counter_timeout(manager, servers, run_count)
    finally:
        await manager.api.disable_injection(host_ip, "database_apply_counter_update_force_timeout")

    writes_timedout_exception_after = get_database_timedout_error_metrics(host_ip)
    writes_timedout_exception_diff = writes_timedout_exception_after - writes_timedout_exception_before
    assert writes_timedout_exception_diff == run_count

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host_ip)
    cpp_exception_metrics_diff = cpp_exception_metrics_after - cpp_exception_metrics_before
    assert cpp_exception_metrics_diff <= cpp_exception_threshold

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_writes_do_apply_counter_update_timeout(manager: ManagerClient):
    config = { "counter_write_request_timeout_in_ms": 100 }
    servers = await manager.servers_add(1, config=config)
    host_ip = servers[0].ip_addr

    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + run_count * 10
    writes_timedout_exception_threshold = run_count * 0.9

    writes_timedout_exception_before = get_database_timedout_error_metrics(host_ip)
    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host_ip)

    try:
        await manager.api.enable_injection(host_ip, "apply_counter_update_delay_ms", one_shot=False, parameters={"value": "200"})
        await _test_replica_counter_timeout(manager, servers, run_count)
    finally:
        await manager.api.disable_injection(host_ip, "apply_counter_update_delay_ms")

    writes_timedout_exception_after = get_database_timedout_error_metrics(host_ip)
    writes_timedout_exception_diff = writes_timedout_exception_after - writes_timedout_exception_before
    assert writes_timedout_exception_diff >= writes_timedout_exception_threshold

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host_ip)
    cpp_exception_metrics_diff = cpp_exception_metrics_after - cpp_exception_metrics_before
    assert cpp_exception_metrics_diff <= cpp_exception_threshold

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_database_apply_timeout(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql, hosts = await manager.get_ready_cql(servers)

    host_ip = servers[0].ip_addr
    host = hosts[0]

    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + run_count * 10
    writes_timedout_exception_threshold = run_count * 0.9

    writes_timedout_exception_before = get_database_timedout_error_metrics(host_ip)
    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host_ip)

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' } AND TABLETS = { 'enabled': false }", host) as keyspace:
        ks = keyspace
        tbl = unique_name()

        try:
            cql.execute(f"CREATE TABLE IF NOT EXISTS {ks}.{tbl} (p int, c int, PRIMARY KEY (p))")

            await manager.api.enable_injection(host_ip, "database_apply_force_timeout", one_shot=False)

            try:
                futures = []
                for i in range(run_count):
                    try:
                        f = (cql.execute_async(f"INSERT INTO {ks}.{tbl} (p, c) VALUES ({i}, {2*i})"))
                        futures.append(f)
                    except Exception:
                        pass

                for f in futures:
                    try:
                        f.result()
                    except Exception:
                        pass
                futures = []
            finally:
                await manager.api.disable_injection(host_ip, "database_apply_force_timeout")

        finally:
            cql.execute(f"DROP TABLE IF EXISTS {ks}.{tbl}")

    writes_timedout_exception_after = get_database_timedout_error_metrics(host_ip)
    writes_timedout_exception_diff = writes_timedout_exception_after - writes_timedout_exception_before
    assert writes_timedout_exception_diff >= writes_timedout_exception_threshold

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host_ip)
    cpp_exception_metrics_diff = cpp_exception_metrics_after - cpp_exception_metrics_before
    assert cpp_exception_metrics_diff <= cpp_exception_threshold
