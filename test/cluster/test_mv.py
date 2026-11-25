#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tests for materialized views that need a three-node cluster.
Note that most materialized-view functional tests can make due with a single
node, and belong in cqlpy. We also have topology_experimental_raft/
test_mv_tablets.py for tests that each needs a cluster of a different
size, and/or special Scylla parameters.
"""
import logging
import pytest

from test.cluster.util import new_test_keyspace, new_test_table, new_materialized_view
from test.pylib.rest_client import read_barrier

ksdef = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 } AND TABLETS = {'enabled': false }"

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.prepare_3_racks_cluster


@pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/24816", reason="tombstone_gc repair mode is not supported yet for MVs due to #24816")
async def test_mv_tombstone_gc_setting(manager):
    """
    Test that the tombstone_gc parameter can be set on a materialized view,
    during CREATE MATERIALIZED VIEW and later with ALTER MATERIALIZED VIEW.
    Note that the test only tests the ability to set this parameter - not
    its actual function after it's set.
    The tombstone_gc feature is Scylla-only, so the test is scylla_only.

    tombstone_gc=repair is not supported on a table with RF=1, and RF>1
    is not supported on a single node, which is why this test needs to
    be here and not in the single-node cqlpy.
    """
    cql = manager.cql
    async with new_test_keyspace(manager, ksdef) as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int") as table:
            # Adding "WITH tombstone_gc = ..." In the CREATE MATERIALIZED VIEW:
            async with new_materialized_view(manager, table, "*", "p, x", "p is not null and x is not null", "WITH tombstone_gc = {'mode': 'repair'}") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert "'mode': 'repair'" in s
            # Adding "WITH tombstone_gc = ..." In the ALTER MATERIALIZED VIEW:
            async with new_materialized_view(manager, table, "*", "p, x", "p is not null and x is not null") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert not "'mode': 'repair'" in s
                await cql.run_async("ALTER MATERIALIZED VIEW " + mv + " WITH tombstone_gc = {'mode': 'repair'}")
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                assert "'mode': 'repair'" in s

async def test_mv_tombstone_gc_not_inherited(manager):
    """
    Test that the tombstone_gc parameter set on a base table is NOT inherited
    by a view created on it later.
    This behavior is not explicitly documented anywhere, but this test
    demonstrates the existing behavior.
    """
    cql = manager.cql
    async with new_test_keyspace(manager, ksdef) as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int", "WITH tombstone_gc = {'mode': 'immediate'}") as table:
            s = list(cql.execute(f"DESC {table}"))[0].create_statement
            assert "'mode': 'immediate'" in s
            async with new_materialized_view(manager, table, "*", "p, x", "p is not null and x is not null") as mv:
                s = list(cql.execute(f"DESC {mv}"))[0].create_statement
                # Base's setting is NOT inherited to the view:
                assert not "'mode': 'immediate'" in s

# A view created while a node is down gets a virtual column for the base
# column it does not select, and the node has the same rows of
# system_schema.view_virtual_columns once it is back. Reproduces #4339.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_virtual_columns_reach_restarted_node(manager):
    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, ksdef) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (a int PRIMARY KEY, b int, c int)")
        await manager.server_stop_gracefully(servers[1].server_id)
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT a, b FROM {ks}.tab WHERE a IS NOT NULL PRIMARY KEY (a)")
        query = f"SELECT * FROM system_schema.view_virtual_columns WHERE keyspace_name = '{ks}'"
        expected = list(await cql.run_async(query))
        assert len(expected) == 1

        await manager.server_start(servers[1].server_id)
        # The node may serve CQL before it catches up with the schema.
        await read_barrier(manager.api, servers[1].ip_addr)
        cql1 = await manager.get_cql_exclusive(servers[1])
        assert list(await cql1.run_async(query)) == expected
