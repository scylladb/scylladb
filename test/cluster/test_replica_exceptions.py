# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Callable

import pytest

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import ServerInfo
from test.pylib.rest_client import ScyllaMetrics

from .util import new_test_keyspace, new_test_table

CPP_EXCEPTIONS_METRIC_NAME = "scylla_reactor_cpp_exceptions"

class Measurement:
    def __init__(self, run_count: int, cpp_exception_threshold: int, metric_name: str, metric_error_threshold: int):
        self.run_count = run_count
        self.cpp_exception_threshold = cpp_exception_threshold
        self.metric_name = metric_name
        self.metric_error_threshold = metric_error_threshold

@pytest.fixture(scope="function")
def measurement():
    return Measurement(
        run_count=1000,
        cpp_exception_threshold=20,
        metric_name="",
        metric_error_threshold=1000,
    )

class DB:
    def __init__(self, *, ks_opts: str, tbl_schema: str, tbl_opts: str = "", prep_stmt_gen: Callable[[str], str] | None = None, stmt_gen: Callable[[str, int], str]):
        self.ks_opts = ks_opts
        self.tbl_schema = tbl_schema
        self.tbl_opts = tbl_opts
        self.prep_stmt_gen = prep_stmt_gen
        self.stmt_gen = stmt_gen

@pytest.fixture(scope="function")
def db():
    return DB(
        ks_opts="WITH REPLICATION = { 'replication_factor' : '1' } AND tablets = { 'enabled' : false }",
        tbl_schema="p int, c int, PRIMARY KEY (p)",
        tbl_opts="",
        prep_stmt_gen=None,
        stmt_gen=None
    )

class Injection:
    def __init__(self, *, name: str, one_shot: bool = False, params: dict = {}):
        self.name = name
        self.one_shot = one_shot
        self.params = params

@pytest.fixture(scope="function")
def injection():
    return Injection(
        name="",
        one_shot=False,
        params={}
    )

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

async def _test_impl(
        manager: ManagerClient,
        measurement: Measurement,
        db: DB,
        injection: Injection):
    servers = await manager.servers_add(1)
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
            async with new_test_injection(manager, host_ip, injection.name, one_shot=injection.one_shot, params=injection.params):
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

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_do_apply_rate_limit_no_cpp_exceptions(manager: ManagerClient, measurement: Measurement, db: DB, injection: Injection):
    measurement.metric_name = "scylla_database_total_writes_rate_limited"
    measurement.metric_error_threshold = measurement.run_count * 0.9

    db.tbl_opts = "WITH per_partition_rate_limit = {'max_writes_per_second': 1}"
    db.stmt_gen = lambda tbl, i: f"INSERT INTO {tbl} (p, c) VALUES (1, {2*i})"

    injection.name = "rate_limit_force_defer"

    await _test_impl(
        manager=manager,
        measurement=measurement,
        db=db,
        injection=injection
    )

@skip_mode("release", "error injections are not supported in release mode")
async def test_replica_query_rate_limit_no_cpp_exceptions(manager: ManagerClient, measurement: Measurement, db: DB, injection: Injection):
    measurement.metric_name = "scylla_database_total_reads_rate_limited"
    measurement.metric_error_threshold = measurement.run_count * 0.9

    db.tbl_opts = "WITH per_partition_rate_limit = {'max_reads_per_second': 1}"
    db.stmt_gen = lambda tbl, i: f"SELECT * FROM {tbl} WHERE p = 1"

    injection.name = "rate_limit_force_defer"

    await _test_impl(
        manager=manager,
        measurement=measurement,
        db=db,
        injection=injection
    )
