#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import time
import asyncio
import logging

import pytest

from test.pylib.internal_types import ServerUpState
from test.pylib.rest_client import HTTPError, inject_error_one_shot
from test.pylib.manager_client import ManagerClient
from test.cluster.util import parse_replication_options, wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode


logger = logging.getLogger(__name__)

KS = "system_distributed_tablets"
TABLES = {
    "snapshots",
}


async def verify_schema(cql, expected_replication: dict[str, list[str]], timeout: int = 10, retry_interval: int = 1) -> None:
    async def _check():
        # Verify keyspace exists
        rows = await cql.run_async(f"SELECT replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{KS}'")
        assert len(rows) == 1, f"Keyspace {KS} not found"

        # Verify replication options
        replication = parse_replication_options(rows[0].replication_v2)
        expected_repl_strategy = 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        assert replication.get('class') == expected_repl_strategy, f"Invalid replication class for keyspace {KS}: expected = {expected_repl_strategy}, actual = {replication.get('class')}"
        replication.pop('class')
        assert replication == expected_replication, f"Invalid replication options for keyspace {KS}: expected = {expected_replication}, actual = {replication}"

        # Verify tablets are enabled
        rows = await cql.run_async(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{KS}'")
        assert len(rows) == 1 and rows[0].initial_tablets is not None, f"Tablets not enabled for keyspace {KS}"

        # Verify tables exist
        rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KS}'")
        found_tables = {row.table_name for row in rows}
        assert found_tables == TABLES

    start = time.time()
    last_error = None
    while True:
        try:
            await _check()
            return
        except AssertionError as exc:
            last_error = exc

        if timeout is None or time.time() >= start + timeout:
            raise last_error
        await asyncio.sleep(retry_interval)


@pytest.mark.asyncio
async def test_auto_rf(manager: ManagerClient):
    """
    Verify that system_distributed_tablets keyspace and tables are created automatically
    and that its replication options are automatically expanded as nodes in new racks join the cluster.
    """

    logger.info("Create first node and verify that schema is created")
    servers = [await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})]
    cql = manager.get_cql()
    await verify_schema(cql, {'dc1': ['r1']})

    logger.info("Check schema after restart")
    await asyncio.gather(*[manager.server_stop(s.server_id) for s in servers])
    await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await verify_schema(cql, {'dc1': ['r1']})

    logger.info("Add a node in an existing rack")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}))
    await verify_schema(cql, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a second rack with two nodes and verify it is added to the RF")
    r2_servers = await manager.servers_add(2, property_file=[{"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r2"}])
    servers.extend(r2_servers)
    await verify_schema(cql, {'dc1': ['r1', 'r2']})

    logger.info("Add a third rack and verify it is added to the RF")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r3"}))
    await verify_schema(cql, {'dc1': ['r1', 'r2', 'r3']})

    logger.info("Add a fourth rack and verify it is not added to the RF (RF goal 3 has been reached)")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r4"}))
    await verify_schema(cql, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)
    rows = await cql.run_async(f"SELECT * FROM system.topology_requests WHERE request_type='keyspace_rf_change' AND new_keyspace_rf_change_ks_name='{KS}' AND done=False ALLOW FILTERING")
    assert len(rows) == 0, f"Unexpected pending RF change requests for keyspace {KS}"

    logger.info("Add a node in a new dc and verify it is added to the RF of the new DC")
    servers.append(await manager.server_add(property_file={"dc": "dc2", "rack": "r1"}))
    await verify_schema(cql, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

    logger.info("Add a zero-token node in a new dc and verify the RF is not changed")
    cfg_zero_token = {"join_ring": "false"}
    servers.append(await manager.server_add(property_file={"dc": "zero-token-dc", "rack": "zero-token-rack"}, config=cfg_zero_token))
    await verify_schema(cql, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

    logger.info("Decommission a node from a rack with multiple nodes")
    await manager.decommission_node(r2_servers[0].server_id)

    logger.info("Decommission the last node from a rack (expected to fail because inter-rack tablet migrations are not supported)")
    with pytest.raises(HTTPError, match="Decommission failed"):
        logger.info("Decommission another node")
        await manager.decommission_node(r2_servers[1].server_id)

    logger.info("Remove the rack from the replication options and retry decommission (expected to succeed)")
    await cql.run_async(f"ALTER KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r3'], 'dc2': ['r1']}}")
    await verify_schema(cql, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})
    await manager.decommission_node(r2_servers[1].server_id)

    logger.info("Drop the tables and verify they are recreated when some node (re)starts")
    for table in TABLES:
        await cql.run_async(f"DROP TABLE {KS}.{table}")
    rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KS}'")
    assert len(rows) == 0, f"Tables not dropped in keyspace {KS}"
    await manager.server_restart(servers[0].server_id)
    await verify_schema(cql, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_upgrades(manager: ManagerClient):
    """
    Test that the system_distributed_tablets keyspace and tables are created
    when the cluster is upgraded to a version that supports the RACK_LIST_RF feature.

    - Start a 2-node cluster where one node has the RACK_LIST_RF feature
      suppressed (thus simulating a node running an older version).
    - Decommission the node with the suppressed feature to let the remaining
      node enable the feature.
    - Confirm that the remaining node automatically creates the keyspace and
      tables without requiring a restart.
    """

    logger.info("Create first node with the RACK_LIST_RF feature disabled")
    cfg = {
        "error_injections_at_startup": [
            {"name": "suppress_features", "value": "RACK_LIST_RF"}
        ]
    }
    server_with_suppressed_feature = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg)

    logger.info("Add a second node")
    await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})

    logger.info(f"Verify that the {KS} keyspace is not created")
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{KS}'")
    assert len(rows) == 0, f"Keyspace {KS} found"

    logger.info("Remove the first node to enable the RACK_LIST_RF feature")
    await manager.decommission_node(server_with_suppressed_feature.server_id)

    logger.info(f"Verify that the {KS} keyspace and its tables are now created")
    await verify_schema(cql, {'dc1': ['r1']})


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_concurrent_rack_additions(manager: ManagerClient):
    """
    Test that if we add multiple racks in parallel, the new nodes will
    serialize their RF change requests for the system_distributed_tablets
    keyspace. Specifically, each node should wait for any ongoing RF change on
    that keyspace to finish before issuing a new RF change for adding its
    own rack to the rack-list RF.

    This limitation stems from the numeric-to-rack-list conversion feature
    (https://github.com/scylladb/scylladb/commit/c077283352). Before this feature,
    there was no such limitation; RF changes were just queued and executed, so
    multiple RF changes for one keyspace could safely sit in the global
    topology requests queue. With the numeric-to-rack-list conversion feature,
    the topology coordinator can "pause" an RF change to run tablet colocations
    and later "resume" it by re-queuing it, so the queue now mixes new and
    resumed RF changes without prioritization. To avoid interleaving resumed and
    new RF changes for the same keyspace, the nodes must ensure not to enqueue
    an RF change if another one is ongoing for the same keyspace. By "ongoing"
    we mean queued, paused, or under execution.
    """
    logger.info("Create first node")
    servers = [await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})]
    cql = manager.get_cql()
    await verify_schema(cql, {'dc1': ['r1']})

    logger.info(f"Injecting wait-before-committing-rf-change-event into the leader node {servers[0]}")
    injection_handler = await inject_error_one_shot(manager.api, servers[0].ip_addr,
                                                    'wait-before-committing-rf-change-event')

    leader_log_file = await manager.server_open_log(servers[0].server_id)

    cmdline=['--logger-log-level', 'topology_state_machine=debug:raft_topology=trace:load_balancer=trace']
    block_cfg = {"error_injections_at_startup": ["block-system-distributed-tablets-create-tables"]}

    logger.info("Add second rack with create_tables blocked")
    server2 = await manager.server_add(property_file={"dc": "dc1", "rack": "r2"}, config=block_cfg, cmdline=cmdline, connect_driver=False, expected_server_up_state=ServerUpState.HOST_ID_QUERIED)

    logger.info("Waiting until second rack pauses on the injected error")
    server2_log = await manager.server_open_log(server2.server_id)
    await server2_log.wait_for("block-system-distributed-tablets-create-tables: waiting for message")

    logger.info("Add third rack with create_tables blocked")
    server3 = await manager.server_add(property_file={"dc": "dc1", "rack": "r3"}, config=block_cfg, cmdline=cmdline, connect_driver=False, expected_server_up_state=ServerUpState.HOST_ID_QUERIED)

    logger.info("Waiting until third rack pauses on the injected error")
    server3_log = await manager.server_open_log(server3.server_id)
    await server3_log.wait_for("block-system-distributed-tablets-create-tables: waiting for message")

    logger.info("Unblock create_tables on second node to kick off the first RF change")
    await manager.api.message_injection(server2.ip_addr, "block-system-distributed-tablets-create-tables")

    logger.info(f"Waiting for the leader node {servers[0]} to reach the RF-change commit barrier")
    await leader_log_file.wait_for("wait-before-committing-rf-change-event: waiting", timeout=20)

    logger.info("Unblock create_tables on third node; it should observe the ongoing RF change and wait")
    await manager.api.message_injection(server3.ip_addr, "block-system-distributed-tablets-create-tables")
    await server3_log.wait_for("Detected ongoing RF change for system_distributed_tablets", timeout=20)

    logger.info("Release the topology coordinator injection and verify RF includes both racks")
    await injection_handler.message()
    await verify_schema(cql, {'dc1': ['r1', 'r2', 'r3']})