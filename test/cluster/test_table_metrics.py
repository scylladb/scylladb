#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import time

import pytest
from cassandra.protocol import ConfigurationException  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import ScyllaMetrics, ScyllaMetricsLine
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

SMP = 2
PER_SHARD_ONLY_METRIC = 'scylla_column_family_memtable_row_tombstone_reads'
VIEW_METRIC = 'scylla_column_family_view_updates_pushed_local'
COMMON_METRIC = 'scylla_column_family_live_sstable'


def table_metric_lines(metrics: ScyllaMetrics, ks: str, cf: str) -> list[ScyllaMetricsLine]:
    lines = [ScyllaMetricsLine.from_string(l) for l in metrics.lines_by_prefix('scylla_column_family_')]
    return [l for l in lines if l and l.labels.get('ks') == ks and l.labels.get('cf') == cf]


def reporting_mode(metrics: ScyllaMetrics, ks: str, cf: str) -> str:
    """Tells from the exported series how the table reports its metrics: per_shard, per_node or none."""
    lines = table_metric_lines(metrics, ks, cf)
    names = {l.name for l in lines}
    if not lines:
        return 'none'
    if PER_SHARD_ONLY_METRIC in names:
        # All shards switched already.
        assert {l.labels['shard'] for l in lines if l.name == PER_SHARD_ONLY_METRIC} == {str(s) for s in range(SMP)}
        # Registered for every base table, with or without views.
        assert VIEW_METRIC in names
        return 'per_shard'
    assert COMMON_METRIC in names
    # Summed over the shards.
    assert all('shard' not in l.labels for l in lines if l.name == COMMON_METRIC)
    assert VIEW_METRIC not in names
    return 'per_node'


class TableMetricsChecker:
    def __init__(self, manager: ScyllaClusterManager, server):
        self.manager = manager
        self.server = server

    async def expect(self, ks: str, cf: str, expected: str):
        async def mode_matches():
            metrics = await self.manager.metrics.query(self.server.ip_addr)
            mode = reporting_mode(metrics, ks, cf)
            logger.debug(f"Table metrics of {ks}.{cf}: {mode}")
            return True if mode == expected else None
        await wait_for(mode_matches, time.time() + 60)


@pytest.mark.asyncio
async def test_table_metrics_cluster_config(manager: ScyllaClusterManager):
    """The table_metrics option switches the metrics of a table at runtime, the narrowest scope
    that sets it winning, and the yaml options deciding when no scope sets it. Every switch
    removes the metrics registered before, or registering them again would fail."""
    server = await manager.server_add(cmdline=['--smp', str(SMP)])
    cql = manager.get_cql()
    log = await manager.server_open_log(server.server_id)
    check = TableMetricsChecker(manager, server)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.t WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, ck, v) VALUES ({i}, {i}, {i})")

        async def expect_t(mode: str):
            await check.expect(ks, 't', mode)

        # The yaml defaults.
        await expect_t('per_node')
        await check.expect('system', 'local', 'none')
        mark = await log.mark()

        for _ in range(2):
            await cql.run_async("ALTER CLUSTER WITH table_metrics = 'per_shard'")
            await expect_t('per_shard')
            await cql.run_async("ALTER CLUSTER WITH table_metrics = 'none'")
            await expect_t('none')

        logger.info("The narrowest scope that sets the option wins")
        await cql.run_async("ALTER CLUSTER WITH table_metrics = 'per_shard'")
        await expect_t('per_shard')
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH table_metrics = 'none'")
        await expect_t('none')
        await cql.run_async(f"ALTER TABLE {ks}.t WITH table_metrics = 'per_node'")
        await expect_t('per_node')
        await cql.run_async(f"ALTER TABLE {ks}.t WITH table_metrics = null")
        await expect_t('none')
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH table_metrics = null")
        await expect_t('per_shard')

        logger.info("The internal tables follow the cluster scope, and can be set per table")
        await check.expect('system', 'local', 'per_shard')
        await cql.run_async("ALTER TABLE system.local WITH table_metrics = 'per_node'")
        await check.expect('system', 'local', 'per_node')
        await cql.run_async("ALTER TABLE system.local WITH table_metrics = 'none'")
        await check.expect('system', 'local', 'none')
        await cql.run_async("ALTER TABLE system.local WITH table_metrics = null")
        await check.expect('system', 'local', 'per_shard')

        logger.info("A dropped table must not keep its metrics, or re-creating it would register them twice")
        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv")
        await cql.run_async(f"DROP TABLE {ks}.t")
        await check.expect(ks, 't', 'none')
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        await check.expect(ks, 't', 'per_shard')

        await cql.run_async("ALTER CLUSTER WITH table_metrics = null")
        await check.expect(ks, 't', 'per_node')

        for bad in ("'PER_NODE'", "'per-node'", "''", "1"):
            with pytest.raises(ConfigurationException):
                await cql.run_async(f"ALTER TABLE {ks}.t WITH table_metrics = {bad}")

        errors = await log.grep_for_errors(distinct_errors=True, from_mark=mark)
        assert not errors, f"Errors logged while switching the table metrics: {errors}"


@pytest.mark.asyncio
async def test_table_metrics_cluster_config_is_reapplied_after_restart(manager: ScyllaClusterManager):
    """A table registers its metrics with the yaml options when it is created at boot, and
    switches to the stored value once the cluster config is loaded."""
    server = await manager.server_add(cmdline=['--smp', str(SMP)])
    cql = manager.get_cql()
    check = TableMetricsChecker(manager, server)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        await cql.run_async(f"CREATE TABLE {ks}.u (pk int PRIMARY KEY)")
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH table_metrics = 'per_shard'")
        await cql.run_async(f"ALTER TABLE {ks}.u WITH table_metrics = 'none'")
        await check.expect(ks, 't', 'per_shard')
        await check.expect(ks, 'u', 'none')

        await manager.server_restart(server.server_id)
        await manager.driver_connect()
        await manager.get_ready_cql([server])
        await check.expect(ks, 't', 'per_shard')
        await check.expect(ks, 'u', 'none')
