#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient


import asyncio
import logging
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.parametrize("enforce", [True, False])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_add_node_in_new_rack_violating_rf_rack(manager: ManagerClient, enforce: bool):
    """
    Test adding a node to a new rack when it would violate RF-rack constraints.

    Creates a cluster with 3 racks and a keyspace with RF=3, then attempts to add a 4th node
    in a new rack which would make the keyspace RF-rack-invalid.

    When enforce=True: Node addition should be rejected with an error
    When enforce=False: Node addition should succeed but with a warning log
    """
    cfg = {'rf_rack_valid_keyspaces': enforce, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")

    if enforce:
        # Node should be rejected
        await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r4"}, expected_error="would make some existing keyspace RF-rack-invalid")

        # add a node in an existing rack - should succeed
        await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r1"})

        # zero token node should be accepted
        zero_token_cfg = {'join_ring': False}
        await manager.server_add(config=cfg | zero_token_cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r4"})
    else:
        logs = [await manager.server_open_log(s.server_id) for s in servers]

        # Node should be accepted but with a warning
        await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r4"})

        matches = [log.grep('makes some existing keyspaces RF-rack-invalid') for log in logs]
        assert any(matches)


@pytest.mark.parametrize("enforce", [True, False])
@pytest.mark.parametrize("op", ["remove", "decommission"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_remove_node_violating_rf_rack(manager: ManagerClient, enforce: bool, op: str):
    """
    Test removing a node when it would violate RF-rack constraints.

    Creates a cluster with 3 racks (2 nodes per rack) and a keyspace with RF=3.
    First removes one node from the last rack (should succeed).
    Then attempts to remove the other node from the same rack, which would eliminate
    that rack entirely and make the keyspace RF-rack-invalid (fewer racks than RF).

    When enforce=True: Second node removal should be rejected with an error
    When enforce=False: Second node removal should succeed but with a warning log
    """
    cfg = {'rf_rack_valid_keyspaces': enforce, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    async def remove_node(server_id: str, expected_error: str = None):
        if op == "remove":
            await manager.server_stop_gracefully(server_id)
            await manager.remove_node(servers[0].server_id, server_id, expected_error=expected_error)
        elif op == "decommission":
            await manager.decommission_node(server_id, expected_error=expected_error)

    # Create 6 servers: 2 in each of 3 racks
    servers = await manager.servers_add(6, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")

    # First removal: Remove one node from rack r3 (should always succeed)
    await remove_node(servers[5].server_id)

    # Second removal: Try to remove the other node from rack r3
    # This would eliminate rack r3 entirely, violating RF-rack constraints
    if enforce:
        # Node removal should be rejected
        await remove_node(servers[4].server_id, expected_error=f"node {op} rejected: Cannot remove the node because its removal would make some existing keyspace RF-rack-invalid")

        # Remove a node from rack r1 - should succeed since r1 will still have one node left
        await remove_node(servers[1].server_id)
    else:
        logs = [await manager.server_open_log(s.server_id) for s in servers]

        # Node removal should succeed but with a warning
        await remove_node(servers[4].server_id)

        matches = [log.grep('makes some existing keyspaces RF-rack-invalid') for log in logs]
        assert any(matches)
