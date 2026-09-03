#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for strongly consistent keyspaces that need a server started with
# a specific configuration or an error injection.

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace, new_test_table
from cassandra.protocol import InvalidRequest

DEFAULT_CONFIG = {'experimental_features': ['strongly-consistent-tables']}

SC_KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
              "AND tablets = {'initial': 1} AND consistency = 'global'")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_counter_table_creation_during_upgrade(manager: ScyllaClusterManager):
    """
    The strongly consistent check runs before the counters-with-tablets
    feature check. With the feature suppressed the cluster looks
    mid-upgrade and both conditions hold. The permanent strongly
    consistent error must be the one thrown, not the transient upgrade
    hint.
    """
    config = DEFAULT_CONFIG | {'error_injections_at_startup': [
        {'name': 'suppress_features', 'value': 'COUNTERS_WITH_TABLETS'}]}
    server = await manager.server_add(config=config)
    cql, _ = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        with pytest.raises(InvalidRequest, match="counters are not yet supported in strongly consistent keyspaces"):
            await cql.run_async(f"CREATE TABLE {ks}.counters (pk int PRIMARY KEY, c counter)")


async def test_cell_only_writes_on_sc_logstor_table(manager: ScyllaClusterManager):
    """
    UPDATE and column deletes on a logstor table in a strongly consistent
    keyspace are rejected. logstor needs a row marker or a partition
    tombstone on every mutation. A write without one fails inside raft
    apply and aborts the node.
    """
    config = {'experimental_features': ['strongly-consistent-tables', 'logstor']}
    server = await manager.server_add(config=config)
    cql, _ = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int", " WITH storage_engine = 'logstor'") as table:
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (1, 1)")

            update_msg = "UPDATE is not supported on logstor tables in strongly consistent keyspaces"
            with pytest.raises(InvalidRequest, match=update_msg):
                await cql.run_async(f"UPDATE {table} SET v = 2 WHERE pk = 1")
            with pytest.raises(InvalidRequest, match=update_msg):
                await cql.run_async(f"BEGIN BATCH UPDATE {table} SET v = 2 WHERE pk = 1; APPLY BATCH")
            with pytest.raises(InvalidRequest, match="Deleting individual columns is not supported on logstor tables in strongly consistent keyspaces"):
                await cql.run_async(f"DELETE v FROM {table} WHERE pk = 1")

            await cql.run_async(f"DELETE FROM {table} WHERE pk = 1")
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (1, 3)")
            rows = await cql.run_async(f"SELECT v FROM {table} WHERE pk = 1")
            assert [r.v for r in rows] == [3]
