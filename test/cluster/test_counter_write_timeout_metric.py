# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

"""
Test that counter write timeouts properly update the
scylla_storage_proxy_coordinator_write_timeouts metric.

This test verifies the fix for SCYLLADB-245 where counter write timeouts
were not being counted in the coordinator write_timeouts metric.
"""

import asyncio
import pytest

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error

from .util import new_test_keyspace, new_test_table


COORDINATOR_WRITE_TIMEOUTS_METRIC = "scylla_storage_proxy_coordinator_write_timeouts"


@skip_mode("release", "error injections are not supported in release mode")
async def test_counter_write_timeout_updates_coordinator_metric(manager: ManagerClient):
    """
    Test that when a counter write times out, the coordinator write_timeouts
    metric is incremented.

    This verifies the fix for SCYLLADB-245: counter write timeouts were not
    updating the scylla_storage_proxy_coordinator_write_timeouts metric because
    the mutate_counters code path did not call get_stats().write_timeouts.mark()
    when throwing mutation_write_timeout_exception.
    """
    # Use a standard timeout
    config = {"counter_write_request_timeout_in_ms": 500}

    servers = await manager.servers_add(1, config=config)
    cql, hosts = await manager.get_ready_cql(servers)

    host_ip = servers[0].ip_addr
    host = hosts[0]

    # Get initial metric value
    metrics_before = await manager.metrics.query(host_ip)
    timeouts_before = metrics_before.get(COORDINATOR_WRITE_TIMEOUTS_METRIC) or 0

    run_count = 100
    timeout_count = 0

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", host) as ks:
        async with new_test_table(manager, ks, "p int, c counter, PRIMARY KEY (p)", "", host) as tbl:
            # Inject a forced timeout to simulate backend timeout
            async with inject_error(manager.api, host_ip, "database_apply_counter_update_force_timeout"):
                for i in range(run_count):
                    try:
                        await cql.run_async(f"UPDATE {tbl} SET c = c + 1 WHERE p = {i}")
                    except Exception:
                        timeout_count += 1

    # Get final metric value
    metrics_after = await manager.metrics.query(host_ip)
    timeouts_after = metrics_after.get(COORDINATOR_WRITE_TIMEOUTS_METRIC) or 0

    timeouts_delta = timeouts_after - timeouts_before

    # We should have recorded some timeouts
    # Allow for some variance since not every request may timeout
    assert timeout_count > 0, "Expected some counter write operations to timeout"
    assert timeouts_delta > 0, (
        f"Expected coordinator write_timeouts metric to increase, "
        f"but it went from {timeouts_before} to {timeouts_after} (delta={timeouts_delta})"
    )
    # The metric should roughly match the number of observed timeouts
    # Allow for some variance due to timing
    assert timeouts_delta >= timeout_count * 0.5, (
        f"Expected at least half of the {timeout_count} timeouts to be recorded in metric, "
        f"but only {timeouts_delta} were recorded"
    )
