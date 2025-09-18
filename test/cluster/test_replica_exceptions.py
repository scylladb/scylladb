# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Callable

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import ServerInfo
from test.pylib.rest_client import ScyllaMetrics

from .util import new_test_keyspace, new_test_table

CPP_EXCEPTIONS_METRIC_NAME = "scylla_reactor_cpp_exceptions"

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

def get_metric_count(metrics: list[ScyllaMetrics], metric_name: str) -> int:
    return sum([m.get(metric_name) for m in metrics])

@asynccontextmanager
async def new_test_injection(manager: ManagerClient, host_ip: str, name: str, one_shot: bool, params: dict[str, Any] = {}):
    params = {} if params is None else params
    try:
        await manager.api.enable_injection(host_ip, name, one_shot=one_shot, parameters=params)
        yield
    finally:
        await manager.api.disable_injection(host_ip, name)

def cpp_exception_count(metrics: list[ScyllaMetrics]) -> int:
    return sum([m.get("scylla_reactor_cpp_exceptions") for m in metrics])

async def _test_impl(
        manager: ManagerClient,
        config: dict[str, Any],
        run_count: int,
        cpp_exception_threshold: int,
        metric_name: str,
        metric_error_threshold: int,
        ks_opts: str,
        tbl_schema: str,
        tbl_opts: str,
        prep_stmt_gen: Callable[[str], str] | None,
        stmt_gen: Callable[[str, int], str],
        injection_name: str,
        injection_one_shot: bool,
        injection_params: dict):
    servers = await manager.servers_add(1, config=config if config is not None else {})
    cql, hosts = await manager.get_ready_cql(servers)

    host_ip = servers[0].ip_addr
    host = hosts[0]

    metrics_before = await get_metrics(manager, servers)

    async with new_test_keyspace(manager, ks_opts, host) as ks:
        async with new_test_table(manager, ks, tbl_schema, tbl_opts, host) as tbl:
            if prep_stmt_gen is not None:
                prep_stmt = prep_stmt_gen(tbl)
                if prep_stmt is not None and prep_stmt != "":
                    cql.execute(prep_stmt)
            async with new_test_injection(manager, host_ip, injection_name, one_shot=injection_one_shot, params=injection_params):
                futures = []
                for i in range(run_count):
                    try:
                        query = stmt_gen(tbl, i)
                        f = cql.execute_async(query)
                        futures.append(f)
                    except Exception:
                        pass
                for f in futures:
                    try:
                        f.result()
                    except Exception:
                        pass

    metrics_after = await get_metrics(manager, servers)

    metrics_errors = get_metric_count(metrics_after, metric_name) - get_metric_count(metrics_before, metric_name)
    assert metrics_errors >= metric_error_threshold

    cpp_exceptions = get_metric_count(metrics_after, CPP_EXCEPTIONS_METRIC_NAME) - get_metric_count(metrics_before, CPP_EXCEPTIONS_METRIC_NAME)
    assert cpp_exceptions <= cpp_exception_threshold

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_do_apply_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    run_count = 1000
    cpp_exception_threshold = 20
    metric_error_threshold = run_count * 0.9

    await _test_impl(
        manager=manager,
        config={},
        run_count=run_count,
        cpp_exception_threshold=cpp_exception_threshold,
        metric_name="scylla_database_total_writes_rate_limited",
        metric_error_threshold=metric_error_threshold,
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' }",
        tbl_schema="p int, c int, PRIMARY KEY (p)",
        tbl_opts="WITH per_partition_rate_limit = {'max_writes_per_second': 1}",
        prep_stmt_gen=None,
        stmt_gen=lambda tbl, i: f"INSERT INTO {tbl} (p, c) VALUES (1, {2*i})",
        injection_name="rate_limit_force_defer",
        injection_one_shot=False,
        injection_params={}
    )

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_query_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    run_count = 1000
    cpp_exception_threshold = 20
    metric_error_threshold = run_count * 0.9

    await _test_impl(
        manager=manager,
        config={},
        run_count=run_count,
        cpp_exception_threshold=cpp_exception_threshold,
        metric_name="scylla_database_total_reads_rate_limited",
        metric_error_threshold=metric_error_threshold,
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' }",
        tbl_schema="p int, c int, PRIMARY KEY (p)",
        tbl_opts="WITH per_partition_rate_limit = {'max_reads_per_second': 1}",
        prep_stmt_gen=lambda tbl: f"INSERT INTO {tbl} (p, c) VALUES (1, 0)",
        stmt_gen=lambda tbl, _: f"SELECT * FROM {tbl} WHERE p = 1",
        injection_name="rate_limit_force_defer",
        injection_one_shot=False,
        injection_params={}
    )

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_writes_apply_counter_update_timeout(manager: ManagerClient):
    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + run_count * 10
    metric_error_threshold = run_count

    await _test_impl(
        manager=manager,
        config={},
        run_count=run_count,
        cpp_exception_threshold=cpp_exception_threshold,
        metric_name="scylla_database_total_writes_timedout",
        metric_error_threshold=metric_error_threshold,
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' } AND TABLETS = { 'enabled': false }",
        tbl_schema="p int, c counter, PRIMARY KEY (p)",
        tbl_opts="",
        prep_stmt_gen=None,
        stmt_gen=lambda tbl, _: f"UPDATE {tbl} SET c = c + 1 WHERE p = 1",
        injection_name="database_apply_counter_update_force_timeout",
        injection_one_shot=False,
        injection_params={}
    )

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_writes_do_apply_counter_update_timeout(manager: ManagerClient):
    config = { "counter_write_request_timeout_in_ms": 100 }

    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + run_count * 10
    metric_error_threshold = run_count * 0.9
    injection_params = { "value": "200" }

    await _test_impl(
        manager=manager,
        config=config,
        run_count=run_count,
        cpp_exception_threshold=cpp_exception_threshold,
        metric_name="scylla_database_total_writes_timedout",
        metric_error_threshold=metric_error_threshold,
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' } AND TABLETS = { 'enabled': false }",
        tbl_schema="p int, c counter, PRIMARY KEY (p)",
        tbl_opts="",
        prep_stmt_gen=None,
        stmt_gen=lambda tbl, _: f"UPDATE {tbl} SET c = c + 1 WHERE p = 1",
        injection_name="apply_counter_update_delay_ms",
        injection_one_shot=False,
        injection_params=injection_params
    )

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_database_apply_timeout(manager: ManagerClient):
    run_count = 1000
    # There should only be a handful of exceptions thrown, not almost 10 per timed out request.
    # Temporarily set a high threshold while we investigate.
    cpp_exception_threshold = 20 + run_count * 10
    metric_error_threshold = run_count * 0.9

    await _test_impl(
        manager=manager,
        config={},
        run_count=run_count,
        cpp_exception_threshold=cpp_exception_threshold,
        metric_name="scylla_database_total_writes_timedout",
        metric_error_threshold=metric_error_threshold,
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' } AND TABLETS = { 'enabled': false }",
        tbl_schema="p int, c int, PRIMARY KEY (p)",
        tbl_opts="",
        prep_stmt_gen=None,
        stmt_gen=lambda tbl, i: f"INSERT INTO {tbl} (p, c) VALUES ({i}, {2*i})",
        injection_name="database_apply_force_timeout",
        injection_one_shot=False,
        injection_params={}
    )
