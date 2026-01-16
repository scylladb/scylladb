#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import time
import asyncio
import logging

import pytest

from test.pylib.rest_client import HTTPError
from test.pylib.manager_client import ManagerClient
from test.cluster.util import parse_replication_options, wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)

SYSTEM_TRACES_KS = "system_traces"
SYSTEM_TRACES_TABLES = {
    "events",
    "node_slow_log",
    "node_slow_log_time_idx",
    "sessions",
    "sessions_time_idx",
}

AUDIT_KS = "audit"
AUDIT_TABLES = {
    "audit_log",
}

AUTO_RF_KEYSPACES = (
    (AUDIT_KS, AUDIT_TABLES, 3),
    (SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, 2),
)

async def verify_schema(cql, ks: str, tables: set[str], expected_replication: dict[str, list[str]], timeout: int = 10, retry_interval: int = 1) -> None:
    async def _check():
        # Verify keyspace exists
        rows = await cql.run_async(f"SELECT replication, replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
        assert len(rows) == 1, f"Keyspace {ks} not found"

        # Verify replication options
        replication = parse_replication_options(rows[0].replication_v2 or rows[0].replication)
        expected_repl_strategy = 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        assert replication.get('class') == expected_repl_strategy, f"Invalid replication class for keyspace {ks}: expected = {expected_repl_strategy}, actual = {replication.get('class')}"
        replication.pop('class')
        assert replication == expected_replication, f"Invalid replication options for keyspace {ks}: expected = {expected_replication}, actual = {replication}"

        # Verify tablets are enabled
        rows = await cql.run_async(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
        assert len(rows) == 1 and rows[0].initial_tablets is not None, f"Tablets not enabled for keyspace {ks}"

        # Verify tables exist
        rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'")
        found_tables = {row.table_name for row in rows}
        assert found_tables == tables

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
async def test_auto_rf_ks_coverage(manager: ManagerClient):
    """
    Verify that Scylla applies the automatic replication factor to all eligible system keyspaces.
    The list of eligible keyspaces is currently hardcoded.
    Note: This is a coverage test, not a full behavioral test.
          The full auto RF functionality is tested in `test_auto_rf_behavior`.
    """
    cfg_audit = {"audit": "table"}

    logger.info("Create first rack and verify that the schemas are created")
    await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg_audit)
    cql = manager.get_cql()
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, ks, tables, {'dc1': ['r1']})

    logger.info("Add a second rack and verify it is added to the RF")
    await manager.server_add(property_file={"dc": "dc1", "rack": "r2"}, config=cfg_audit)
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2']})

    logger.info("Add a second dc with two racks and verify it is added to the RF")
    await manager.servers_add(2, property_file=[{"dc": "dc2", "rack": "r1"}, {"dc": "dc2", "rack": "r2"}], config=cfg_audit)
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']})


@pytest.mark.asyncio
async def test_auto_rf_behavior(manager: ManagerClient):
    """
    Verify all aspects of the automatic replication factor mechanism:
    * Per-DC replication factors are automatically expanded as nodes in new racks join the cluster.
    * Replication options are expanded to add new DCs when nodes in new DCs join the cluster.
    * Replication factors are not expanded beyond the RF goal.
    * Zero-token nodes do not trigger RF expansions.
    * Rack decommission by ALTER KEYSPACE works correctly.

    This test uses the audit keyspace as the test subject.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = {"audit": "table"}

    logger.info("Create first rack and verify that schema is created")
    servers = [await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg_audit)]
    cql = manager.get_cql()
    await verify_schema(cql, ks, tables, {'dc1': ['r1']})

    logger.info("Check schema after restart")
    await asyncio.gather(*[manager.server_stop(s.server_id) for s in servers])
    await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await verify_schema(cql, ks, tables, {'dc1': ['r1']})

    logger.info("Add a node in an existing rack")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg_audit))
    await verify_schema(cql, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a second rack with two nodes and verify it is added to the RF")
    r2_servers = await manager.servers_add(2, property_file=[{"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r2"}], config=cfg_audit)
    servers.extend(r2_servers)
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2']})

    logger.info("Add a third rack and verify it is added to the RF")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r3"}, config=cfg_audit))
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2', 'r3']})

    logger.info("Add a fourth rack and verify it is not added to the RF (RF goal 3 has been reached)")
    servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r4"}, config=cfg_audit))
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)
    rows = await cql.run_async(f"SELECT * FROM system.topology_requests WHERE request_type='keyspace_rf_change' AND new_keyspace_rf_change_ks_name='{ks}' AND done=False ALLOW FILTERING")
    assert len(rows) == 0, f"Unexpected pending RF change requests for keyspace {ks}"

    logger.info("Add a node in a new dc and verify it is added to the RF of the new DC")
    servers.append(await manager.server_add(property_file={"dc": "dc2", "rack": "r1"}, config=cfg_audit))
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

    logger.info("Add a zero-token node in a new dc and verify the RF is not changed")
    cfg_zero_token = {"join_ring": "false"}
    servers.append(await manager.server_add(property_file={"dc": "zero-token-dc", "rack": "zero-token-rack"}, config=cfg_audit | cfg_zero_token))
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

    logger.info("Remove the second rack from the replication options and verify auto-RF will add the fourth rack to the rack list")
    await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r3'], 'dc2': ['r1']}}")
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r3', 'r4'], 'dc2': ['r1']})

    logger.info("Remove the fourth rack from the replication options and bring back the second rack")
    await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r3'], 'dc2': ['r1']}}")
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})
    await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']}}")
    await verify_schema(cql, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

    logger.info("Decommission a node from a rack with multiple nodes")
    await manager.decommission_node(r2_servers[0].server_id)

    logger.info("Decommission the last node from a rack (expected to fail because inter-rack tablet migrations are not supported)")
    with pytest.raises(HTTPError, match="Decommission failed"):
        logger.info("Decommission another node")
        await manager.decommission_node(r2_servers[1].server_id)

    logger.info("Remove the rack from the replication options of all auto-RF-enabled keyspaces and retry decommission (expected to succeed)")
    await cql.run_async(f"ALTER KEYSPACE {AUDIT_KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r3'], 'dc2': ['r1']}}")
    await cql.run_async(f"ALTER KEYSPACE {SYSTEM_TRACES_KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1'], 'dc2': ['r1']}}")
    await manager.decommission_node(r2_servers[1].server_id)