# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
from typing import Any, Callable, NamedTuple

import pytest

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import ServerInfo
from test.pylib.rest_client import ScyllaMetrics, inject_error

from .util import new_test_keyspace, new_test_table

CPP_EXCEPTIONS_METRIC_NAME = "scylla_reactor_cpp_exceptions"

class Measurement(NamedTuple):
    run_count: int = 1000
    cpp_exception_threshold: int = 20
    metric_name: str = ""
    metric_error_threshold: int = 1000

class DB(NamedTuple):
    ks_opts: str = "WITH REPLICATION = { 'replication_factor' : '1' }"
    tbl_schema: str = "p int, c int, PRIMARY KEY (p)"
    tbl_opts: str = ""
    prep_stmt_gen: Callable[[str], str] | None = None
    stmt_gen: Callable[[str, int], str] | None = None

class Injection(NamedTuple):
    name: str = ""

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

def get_metric_count(metrics: list[ScyllaMetrics], metric_name: str) -> int:
    return sum([m.get(metric_name) for m in metrics])

async def _test_impl(
        manager: ManagerClient,
        config: dict[str, Any],
        measurement: Measurement,
        db: DB,
        injection: Injection):
    servers = await manager.servers_add(1, config=config if config is not None else {})
    cql, hosts = await manager.get_ready_cql(servers)

    host_ip = servers[0].ip_addr
    host = hosts[0]

    metrics_before = await get_metrics(manager, servers)

    async with new_test_keyspace(manager, db.ks_opts, host) as ks:
        async with new_test_table(manager, ks, db.tbl_schema, db.tbl_opts, host) as tbl:
            if db.prep_stmt_gen is not None:
                prep_stmt = db.prep_stmt_gen(tbl)
                if prep_stmt is not None and prep_stmt != "":
                    cql.execute(prep_stmt)
            async with inject_error(manager.api, host_ip, injection.name):
                futures = []
                for i in range(measurement.run_count):
                    try:
                        query = db.stmt_gen(tbl, i)
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

    metrics_errors = get_metric_count(metrics_after, measurement.metric_name) - get_metric_count(metrics_before, measurement.metric_name)
    assert metrics_errors >= measurement.metric_error_threshold

    cpp_exceptions = get_metric_count(metrics_after, CPP_EXCEPTIONS_METRIC_NAME) - get_metric_count(metrics_before, CPP_EXCEPTIONS_METRIC_NAME)
    assert cpp_exceptions <= measurement.cpp_exception_threshold

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replica_do_apply_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    measurement = (m := Measurement())._replace(
        metric_name = "scylla_database_total_writes_rate_limited",
        metric_error_threshold = m.run_count * 0.9
    )
    db = DB(
        tbl_opts = "WITH per_partition_rate_limit = {'max_writes_per_second': 1}",
        stmt_gen = lambda tbl, i: f"INSERT INTO {tbl} (p, c) VALUES (1, {2*i})"
    )
    injection = Injection(
        name = "rate_limit_force_defer"
    )

    await _test_impl(
        manager=manager,
        config={},
        measurement=measurement,
        db=db,
        injection=injection
    )

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replica_query_rate_limit_no_cpp_exceptions(manager: ManagerClient):
    measurement = (m := Measurement())._replace(
        metric_name = "scylla_database_total_reads_rate_limited",
        metric_error_threshold = m.run_count * 0.9
    )
    db = DB(
        tbl_opts = "WITH per_partition_rate_limit = {'max_reads_per_second': 1}",
        stmt_gen = lambda tbl, i: f"SELECT * FROM {tbl} WHERE p = 1"
    )
    injection = Injection(
        name = "rate_limit_force_defer"
    )

    await _test_impl(
        manager=manager,
        config={},
        measurement=measurement,
        db=db,
        injection=injection
    )

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replica_writes_apply_counter_update_timeout(manager: ManagerClient):
    measurement = (m := Measurement())._replace(
        cpp_exception_threshold = 20 + m.run_count * 10,
        metric_name = "scylla_database_total_writes_timedout",
        metric_error_threshold = m.run_count
    )
    db = DB(
        tbl_schema = "p int, c counter, PRIMARY KEY (p)",
        stmt_gen = lambda tbl, _: f"UPDATE {tbl} SET c = c + 1 WHERE p = 1"
    )
    injection = Injection(
        name = "database_apply_counter_update_force_timeout"
    )

    await _test_impl(
        manager=manager,
        config={},
        measurement=measurement,
        db=db,
        injection=injection
    )

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replica_writes_do_apply_counter_update_timeout(manager: ManagerClient):
    config = { "counter_write_request_timeout_in_ms": 50 }

    measurement = (m := Measurement())._replace(
        cpp_exception_threshold = 20 + m.run_count * 10,
        metric_name = "scylla_database_total_writes_timedout",
        metric_error_threshold = m.run_count * 0.9
    )
    db = DB(
        tbl_schema = "p int, c counter, PRIMARY KEY (p)",
        stmt_gen = lambda tbl, _: f"UPDATE {tbl} SET c = c + 1 WHERE p = 1"
    )
    injection = Injection(
        name = "apply_counter_update_delay_100ms",
    )

    await _test_impl(
        manager=manager,
        config=config,
        measurement=measurement,
        db=db,
        injection=injection
    )

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replica_database_apply_timeout(manager: ManagerClient):
    measurement = (m := Measurement())._replace(
        cpp_exception_threshold = 20 + m.run_count * 10,
        metric_name = "scylla_database_total_writes_timedout",
        metric_error_threshold = m.run_count * 0.9
    )
    db = DB(
        stmt_gen = lambda tbl, i: f"INSERT INTO {tbl} (p, c) VALUES ({i}, {2*i})"
    )
    injection = Injection(
        name = "database_apply_force_timeout"
    )

    await _test_impl(
        manager=manager,
        config={},
        measurement=measurement,
        db=db,
        injection=injection
    )
