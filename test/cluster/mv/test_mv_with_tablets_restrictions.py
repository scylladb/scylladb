#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest

from cassandra.cluster import Session as CassandraSession
from cassandra.protocol import InvalidRequest

from test.cluster.conftest import skip_mode
from test.pylib.async_cql import _wrap_future
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.parametrize("rf_kind", ["numeric", "rack_list"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_create_mv_and_index_restrictions_in_tablet_keyspaces(manager: ManagerClient, rf_kind: str):
    """
    Verify that creating a materialized view or a secondary index in a tablet-based keyspace
    is allowed only when RF equals the number of racks, even if `rf_rack_valid_keyspaces` is false.

    The constraint is relevant only for numeric-RF keyspaces. For rack-list keyspaces, it should
    be always allowed.
    """

    async def create_mv_or_index(cql: CassandraSession, schema_kind: str, expected_warning: str = None):
        if schema_kind == "view":
            result = cql.execute_async("CREATE MATERIALIZED VIEW ks.mv "
                                "AS SELECT * FROM ks.t "
                                "WHERE p IS NOT NULL AND v IS NOT NULL "
                                "PRIMARY KEY (v, p)")
        elif schema_kind == "index":
            result = cql.execute_async("CREATE INDEX myindex ON ks.t(v)")
        else:
            assert False, "Unknown schema kind"

        await _wrap_future(result)

        if expected_warning:
            assert any(expected_warning in w for w in result.warnings), f"Expected warning '{expected_warning}' not found in {result.warnings}"

    async def test_create_mv_or_index_with_rf(cql: CassandraSession, schema_kind: str, rf: int, expected_error: str = None, expected_warning: str = None):
        if rf_kind == "numeric":
            rf_str = str(rf)
        else:
            rf_str = "[" + ", ".join([f"'rack{i+1}'" for i in range(rf)]) + "]"

        try:
            await cql.run_async(f"CREATE KEYSPACE ks WITH replication = "
                                 f"{{'class': 'NetworkTopologyStrategy', 'dc1': {rf_str}}} "
                                 "AND tablets = {'enabled': true}")
            await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
            if expected_error:
                with pytest.raises(InvalidRequest, match=expected_error):
                    await create_mv_or_index(cql, schema_kind, expected_warning)
            else:
                await create_mv_or_index(cql, schema_kind, expected_warning)
        finally:
            await cql.run_async("DROP KEYSPACE IF EXISTS ks")

    config = {'rf_rack_valid_keyspaces': False}
    if rf_kind == "numeric":
        config = config | {'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}

    servers = await manager.servers_add(3, config=config, cmdline=['--logger-log-level', 'tablets=debug'], property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'},
    ])
    cql, _ = await manager.get_ready_cql(servers)

    for schema_kind in ["view", "index"]:
        # Create MV/index with RF=Racks - should always succeed
        if rf_kind == "numeric":
            expected_warning = "requires the keyspace to remain RF-rack-valid"
        else:
            expected_warning = None
        await test_create_mv_or_index_with_rf(cql, schema_kind, 3, expected_warning=expected_warning)

        # Create MV/index with RF!=Racks - should fail for numeric RF
        if rf_kind == "numeric":
            expected_error = "required to be RF-rack-valid"
        else:
            expected_error = None
        await test_create_mv_or_index_with_rf(cql, schema_kind, 2, expected_error=expected_error)


@pytest.mark.asyncio
@pytest.mark.parametrize("rf_kind", ["numeric", "rack_list"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_alter_keyspace_rf_rack_restriction_with_mv_and_index(manager: ManagerClient, rf_kind: str):
    """
    Verify that ALTER KEYSPACE fails if it changes RF so that RF != number of racks
    for a tablets-based keyspace while it has a materialized view or a secondary index, even if
    `rf_rack_valid_keyspaces` is false.
    It should fail when it has MV/index and succeed if it doesn't.

    The constraint is relevant only for numeric-RF keyspaces. For rack-list keyspaces, it should
    be always allowed.
    """
    config = {'rf_rack_valid_keyspaces': False}
    if rf_kind == "numeric":
        config = config | {'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}

    servers = await manager.servers_add(3, config=config, cmdline=['--logger-log-level', 'tablets=debug'], property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'},
    ])
    cql, _ = await manager.get_ready_cql(servers)

    for schema_kind in ["view", "index"]:
        # Create a keyspace and MV/index with RF=Racks
        ks = f"ks_{schema_kind}"

        if rf_kind == "numeric":
            rf_str = "3"
        else:
            rf_str = "['rack1', 'rack2', 'rack3']"

        await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = "
                            f"{{'class': 'NetworkTopologyStrategy', 'dc1': {rf_str}}} "
                            "AND tablets = {'enabled': true}")
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        if schema_kind == "view":
            await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv "
                                f"AS SELECT * FROM {ks}.t "
                                "WHERE p IS NOT NULL AND v IS NOT NULL "
                                "PRIMARY KEY (v, p)")
        elif schema_kind == "index":
            await cql.run_async(f"CREATE INDEX myindex ON {ks}.t(v)")
        else:
            assert False, "Unknown schema kind"

        if rf_kind == "numeric":
            # Try to ALTER KEYSPACE to RF!=Racks - should fail because it has MV/index
            with pytest.raises(InvalidRequest, match="required to be RF-rack-valid"):
                await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                                    f"{{'class': 'NetworkTopologyStrategy', 'dc1': 2}}")

            # drop the view/index and verify that ALTER KEYSPACE is now allowed
            if schema_kind == "view":
                await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv")
            elif schema_kind == "index":
                await cql.run_async(f"DROP INDEX {ks}.myindex")

            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                                f"{{'class': 'NetworkTopologyStrategy', 'dc1': 2}}")
        else:
            # For rack-list RF, ALTER KEYSPACE should succeed
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                                f"{{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2']}}")

        await cql.run_async(f"DROP KEYSPACE {ks}")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_add_node_in_new_rack_restriction_with_mv(manager: ManagerClient):
    """
    Test adding a node to a new rack is rejected when there is a keyspace with RF=Racks and a materialized view
    that would make the keyspace become RF-rack-invalid, even if `rf_rack_valid_keyspaces` is false.

    Creates a cluster with 3 racks and a keyspace with RF=3 and a MV, then attempts to add a 4th node
    in a new rack which would make the keyspace RF-rack-invalid.
    """
    cfg = {'rf_rack_valid_keyspaces': False, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")
    await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, p)")

    # Node should be rejected
    await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r4"}, expected_error="would make some existing keyspace RF-rack-invalid")

    # add a node in an existing rack - should succeed
    await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r1"})


@pytest.mark.parametrize("op", ["remove", "decommission"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_remove_node_violating_rf_rack(manager: ManagerClient, op: str):
    """
    Test removing a node is rejected when there is a keyspace with RF=Racks and a materialized view
    that would make the keyspace become RF-rack-invalid, even if `rf_rack_valid_keyspaces` is false.

    Creates a cluster with 3 racks and a keyspace with RF=3 and a MV.
    Remove a node from a rack that has two nodes - should succeed.
    Then attempts to remove the other node from the same rack, which would eliminate
    that rack entirely and make the keyspace RF-rack-invalid - should be rejected.
    """
    cfg = {'rf_rack_valid_keyspaces': False, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    async def remove_node(server_id: str, expected_error: str = None):
        if op == "remove":
            await manager.server_stop_gracefully(server_id)
            await manager.remove_node(servers[0].server_id, server_id, expected_error=expected_error)
        elif op == "decommission":
            await manager.decommission_node(server_id, expected_error=expected_error)

    servers = await manager.servers_add(4, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")
    await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, p)")

    # First removal: Remove one node from rack r3 (should always succeed)
    await remove_node(servers[3].server_id)

    # Second removal: Try to remove the other node from rack r3
    # This would eliminate rack r3 entirely, violating RF-rack constraints
    await remove_node(servers[2].server_id, expected_error=f"node {op} rejected: Cannot remove the node because its removal would make some existing keyspace RF-rack-invalid")

    # Drop the materialized view and verify we can now remove the rack
    await cql.run_async("DROP MATERIALIZED VIEW ks.mv")
    await cql.run_async("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}")
    await remove_node(servers[2].server_id)
