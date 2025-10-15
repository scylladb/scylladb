#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.cluster.conftest import skip_mode
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


@pytest.mark.parametrize("injection", ["before_bootstrap", "after_bootstrap"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_keyspace_creation_during_node_join(manager: ManagerClient, injection: str):
    """
    Test keyspace creation behavior during node join at different stages.

    Creates a cluster with 3 nodes and 3 racks, then starts adding a new node in a 4th rack.
    The node join is paused at different points to test keyspace creation behavior.

    When injection="before_bootstrap":
    - Pause after node is accepted but before bootstrap starts
    - Creating keyspace with RF=3 should succeed (topology hasn't changed yet)
    - But then node bootstrap should fail due to RF-rack violation

    When injection="after_bootstrap":
    - Pause after bootstrap completes but before final steps
    - Creating keyspace with RF=3 should fail (4 racks would violate RF-rack constraints)
    - Node join should complete successfully
    """
    cfg = {'rf_rack_valid_keyspaces': True, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    # Create initial 3-node cluster with 3 racks
    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    inj_before_bootstrap = "topology_coordinator_pause_after_accept_node"
    inj_after_bootstrap = "topology_coordinator_pause_after_updating_cdc_generation"

    if injection == "before_bootstrap":
        inj = inj_before_bootstrap
        server_add_expected_error = "would make some existing keyspace RF-rack-invalid"
    else:
        inj = inj_after_bootstrap
        server_add_expected_error = None

    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers])

    # Start adding a new node in a new rack (this will pause due to injection)
    logger.info(f"Starting to add new node in rack r4 (will pause {injection})")
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()
    add_node_task = asyncio.create_task(
        manager.server_add(config=cfg, cmdline=cmdline,
                           property_file={"dc": "dc1", "rack": "r4"},
                           expected_error=server_add_expected_error)
    )
    await log.wait_for(f"{inj}: waiting for message", from_mark=mark, timeout=60)

    if injection == "before_bootstrap":
        # Node is accepted but bootstrap hasn't started yet
        # Creating keyspace with RF=3 should succeed (topology hasn't changed yet)
        logger.info("Creating keyspace with RF=3 (should succeed - topology not changed yet)")
        await cql.run_async("CREATE KEYSPACE test_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")

        # Unpause the join process - bootstrap should fail due to RF-rack violation
        await asyncio.gather(*[manager.api.message_injection(s.ip_addr, inj) for s in servers])
        logger.info("Waiting for node join to fail")
        await add_node_task
    else:
        # Node has bootstrapped but join isn't complete yet
        # Creating keyspace with RF=3 should fail (4 racks would violate RF-rack constraints)
        logger.info("Attempting to create keyspace with RF=3 (should fail - 4 racks detected)")
        with pytest.raises(Exception, match="required to be RF-rack-valid"):
            await cql.run_async("CREATE KEYSPACE test_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")

        # Creating keyspace with RF=4 should also fail, because the node join could still fail and rollback.
        logger.info("Attempting to create keyspace with RF=4 (should fail - number of racks undetermined)")
        with pytest.raises(Exception, match="required to be RF-rack-valid"):
            await cql.run_async("CREATE KEYSPACE test_rf4 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 4} AND tablets = {'enabled': true}")

        # Unpause the join process and wait for node join to complete successfully
        await asyncio.gather(*[manager.api.message_injection(s.ip_addr, inj) for s in servers])
        logger.info("Waiting for node join to complete")
        servers += [await add_node_task]

        # Now RF=4 should be allowed
        logger.info("Creating keyspace with RF=4 (should succeed - 4 racks now present)")
        await cql.run_async("CREATE KEYSPACE test_rf4 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 4} AND tablets = {'enabled': true}")

        # Test adding a second node to the same rack (r4) - keyspace creation should succeed
        # because the number of racks remains constant (4)
        logger.info("Testing second node addition to existing rack r4")

        # Re-enable injection for remaining servers (including the newly added one)
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers])

        # Start adding second node to rack r4
        mark = await log.mark()
        add_node2_task = asyncio.create_task(
            manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r4"})
        )
        await log.wait_for(f"{inj}: waiting for message", from_mark=mark, timeout=60)

        # Creating keyspace should succeed now because rack count remains at 4
        logger.info("Creating keyspace with RF=4 while second node joins r4 (should succeed - rack count unchanged)")
        await cql.run_async("CREATE KEYSPACE test_rf4_second WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 4} AND tablets = {'enabled': true}")

        # Complete the second node join
        await asyncio.gather(*[manager.api.message_injection(s.ip_addr, inj) for s in servers])
        logger.info("Waiting for second node join to complete")
        await add_node2_task


@pytest.mark.parametrize("op", ["remove", "decommission"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_keyspace_creation_during_node_remove(manager: ManagerClient, op: str):
    """
    Test keyspace creation behavior during node removal or decommission.

    Creates a cluster with 3 nodes in 3 racks, then starts removing/decommissioning one node.
    The node operation is paused after it starts but before completion.

    While paused, attempts to create a keyspace with RF=3 should fail because:
    - The cluster currently has 3 racks (RF=3 would be valid now)
    - But when the operation completes, there will be only 2 racks
    - RF=3 with 2 racks would violate RF-rack constraints
    """
    cfg = {'rf_rack_valid_keyspaces': True, 'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'RACK_LIST_RF'}]}
    cmdline = ['--logger-log-level', 'tablets=debug', '--logger-log-level', 'raft_topology=debug']

    # Create initial 3-node cluster with 3 racks
    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    if op == "remove":
        inj = "topology_coordinator_pause_after_start_removenode"
    elif op == "decommission":
        inj = "topology_coordinator_pause_after_start_decommission"

    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:2]])

    # Start the node operation (this will pause due to injection)
    logger.info(f"Starting to {op} node from rack r3 (will pause during {op})")
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    if op == "remove":
        await manager.server_stop_gracefully(servers[2].server_id)
        node_op_task = asyncio.create_task(
            manager.remove_node(servers[0].server_id, servers[2].server_id)
        )
    elif op == "decommission":
        node_op_task = asyncio.create_task(
            manager.decommission_node(servers[2].server_id)
        )

    await log.wait_for(f"{inj}: waiting for message", from_mark=mark, timeout=60)

    # While the node operation is paused, try to create keyspace with RF=3
    # Should fail because operation would leave only 2 racks, making RF=3 invalid
    logger.info(f"Attempting to create keyspace with RF=3 (should fail - {op} would leave 2 racks)")
    with pytest.raises(Exception, match="required to be RF-rack-valid"):
        await cql.run_async("CREATE KEYSPACE test_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3} AND tablets = {'enabled': true}")

    # While the node operation is paused, try to create keyspace with RF=2
    # Should also fail because the operation may still rollback and go back to 3 racks.
    logger.info(f"Attempting to create keyspace with RF=2 (should fail - {op} is in progress)")
    with pytest.raises(Exception, match="required to be RF-rack-valid"):
        await cql.run_async("CREATE KEYSPACE test_rf2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2} AND tablets = {'enabled': true}")

    # Unpause the operation and wait for it to complete
    await asyncio.gather(*[manager.api.message_injection(s.ip_addr, inj) for s in servers[:2]])
    logger.info(f"Waiting for node {op} to complete")
    await node_op_task


@pytest.mark.parametrize("op", ["remove", "decommission"])
@pytest.mark.asyncio
async def test_remove_node_violating_rf_rack_with_rack_list(manager: ManagerClient, op: str):
    """
    Test removing a node when it would violate RF-rack constraints with explicit rack list.

    Creates a cluster with 4 racks (r1, r2, r3, r4) and a keyspace that explicitly
    specifies RF as a list of racks ['r1', 'r2', 'r4'].

    Tests that:
    - Removing a node from r4 (listed rack) should be rejected - would violate RF-rack constraints
    - Removing a node from r3 (unlisted rack) should succeed - doesn't affect RF-rack validity
    """
    cfg = {}
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
        {"dc": "dc1", "rack": "r4"},
    ])
    cql = manager.get_cql()

    # Create keyspace with explicit rack list - only uses racks r1, r2, r4
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r2', 'r4']} AND tablets = {'enabled': true}")
    await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t WHERE p IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, p)")

    # Try to remove node from r4 (listed rack) - should be rejected
    # This would eliminate rack r4 from the available racks, violating RF-rack constraints
    await remove_node(servers[3].server_id, expected_error=f"node {op} rejected: Cannot remove the node because its removal would make some existing keyspace RF-rack-invalid")

    # Remove node from r3 (unlisted rack) - should succeed
    # This doesn't affect RF-rack validity since r3 is not in the keyspace's rack list
    await remove_node(servers[2].server_id)
