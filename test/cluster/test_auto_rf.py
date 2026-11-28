#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time
import asyncio
import logging

import pytest
from cassandra.protocol import InvalidRequest

from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.util import (create_new_test_keyspace, get_topology_coordinator, parse_replication_options,
                               wait_for_cql_and_get_hosts)


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

# The auto-RF keyspaces are created on tablets, with rack lists, only where
# enforce_rack_list is set; the tests start every node with it.
RACK_LIST_CFG = {"enforce_rack_list": True}
AUDIT_CFG = {"audit": "table"} | RACK_LIST_CFG


def sorted_racks(replication: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Normalize replication options for comparison.

    A rack list is semantically a set: auto-RF appends the rack it picks, so the
    order of the racks in system_schema.keyspaces is not significant.
    """
    return {dc: sorted(racks) for dc, racks in replication.items()}


async def _replication(cql, ks: str) -> dict:
    """The replication options of `ks`, without the class."""
    rows = await cql.run_async(f"SELECT replication, replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
    assert len(rows) == 1, f"Keyspace {ks} not found"
    replication = parse_replication_options(rows[0].replication_v2 or rows[0].replication)
    replication.pop('class', None)
    return replication


def rack_list_opts(racks_per_dc: dict[str, list[str]]) -> str:
    return ", ".join(f"'{dc}': {racks}" for dc, racks in racks_per_dc.items())


async def create_rack_anchor_keyspace(cql, racks_per_dc: dict[str, list[str]]) -> str:
    """
    Create a non-auto-RF tablets keyspace replicating to exactly `racks_per_dc`.

    Auto-RF only expands into the racks such a keyspace lists (get_auto_rf_racks()
    in the topology coordinator), so this "anchor" makes exactly those racks
    eligible and lets a test assert which rack auto-RF picks.
    """
    return await create_new_test_keyspace(
        cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', {rack_list_opts(racks_per_dc)}}}")


async def set_anchor_racks(cql, ks: str, racks_per_dc: dict[str, list[str]]) -> None:
    """
    Set the rack lists of an anchor keyspace, i.e. the eligible racks.

    `racks_per_dc` has to list every DC the anchor currently replicates to, and a
    single ALTER may change only one DC, by one rack, see
    cql3::statements::alter_keyspace_statement::validate().
    """
    await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', {rack_list_opts(racks_per_dc)}}}")


async def rf_change_requests(cql, ks: str, pending_only: bool = True) -> list:
    """
    The keyspace_rf_change requests of `ks` in system.topology_requests. The rows have
    a one month TTL, so with pending_only=False completed and failed requests are
    returned too.
    """
    query = f"SELECT id FROM system.topology_requests WHERE request_type='keyspace_rf_change' AND new_keyspace_rf_change_ks_name='{ks}'"
    if pending_only:
        query += " AND done=False"
    return list(await cql.run_async(query + " ALLOW FILTERING"))


async def wait_for_auto_rf_settled(manager: ScyllaClusterManager, deadline: float, server: ServerInfo | None = None) -> None:
    """
    Wait until auto-RF has nothing left to do. quiesce_topology() makes the coordinator
    evaluate auto-RF against the current schema and topology, schedules a due change
    and waits for it (and for the tablet load balancer). It needs every node alive, and
    does not wait for a change held back by the backoff.

    Creating the anchor keyspace is what makes racks eligible, so call this after the
    test's own keyspace setup. `server` is the node asked, the first running one by default.
    """
    node = server or (await manager.running_servers())[0]
    remaining = deadline - time.time()
    try:
        await asyncio.wait_for(manager.api.quiesce_topology(node.ip_addr), timeout=max(remaining, 1))
    except asyncio.TimeoutError:
        raise AssertionError(f"auto-RF did not settle within {remaining:.0f}s")


async def rf_change_task_ids(manager: ScyllaClusterManager, server: ServerInfo, ks: str) -> set:
    """
    The keyspace_rf_change tasks of `ks` the task manager of `server` knows: pending,
    running and finished ones. The listing is a local read, hence the read barrier.
    """
    await read_barrier(manager.api, server.ip_addr)
    tasks = await TaskManagerClient(manager.api).list_tasks(server.ip_addr, "global_topology_requests", keyspace=ks)
    return {t.task_id for t in tasks if t.type == "keyspace_rf_change"}


async def uses_tablets(cql, ks: str) -> bool:
    rows = await cql.run_async(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
    return len(rows) == 1 and rows[0].initial_tablets is not None


async def durable_writes(cql, ks: str) -> bool:
    rows = await cql.run_async(f"SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")
    assert len(rows) == 1, f"Keyspace {ks} not found"
    return rows[0].durable_writes


async def assert_no_pending_rf_change(cql, ks: str) -> None:
    """Assert that auto-RF has not queued an RF change for `ks`."""
    rows = await rf_change_requests(cql, ks)
    assert not rows, f"Unexpected pending RF change requests for keyspace {ks}: {rows}"


async def add_servers_and_update_map(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                     host_to_dc_rack: dict[HostID, tuple[str, str]], count: int,
                                     property_file: list[dict[str, str]] | dict[str, str],
                                     config: dict[str, str] | None = None) -> list[ServerInfo]:
    """Add multiple servers and update the host_to_dc_rack map incrementally."""
    new_servers = await manager.servers_add(count, property_file=property_file, config=config)
    servers.extend(new_servers)
    for server in new_servers:
        host_id = await manager.get_host_id(server.server_id)
        host_to_dc_rack[host_id] = (server.datacenter, server.rack)
    return new_servers


async def add_server_and_update_map(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                    host_to_dc_rack: dict[HostID, tuple[str, str]], property_file: dict[str, str],
                                    config: dict[str, str] | None = None) -> ServerInfo:
    """Add a server and update the host_to_dc_rack map incrementally."""
    new_servers = await add_servers_and_update_map(manager, servers, host_to_dc_rack, 1, [property_file], config)
    return new_servers[0]


async def verify_schema(cql, manager: ScyllaClusterManager, servers: list[ServerInfo],
                        host_to_dc_rack: dict[HostID, tuple[str, str]], ks: str, tables: set[str],
                        expected_replication: dict[str, list[str]], timeout: int = 10, retry_interval: int = 1) -> None:
    """
    Check `ks`'s replication options, that it is on tablets, its tables, and that every
    tablet's replicas are on exactly the expected racks; retried for up to `timeout` seconds.
    """
    async def _check():
        # Verify keyspace exists
        rows = await cql.run_async(f"SELECT replication, replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
        assert len(rows) == 1, f"Keyspace {ks} not found"

        # Verify replication options
        replication = parse_replication_options(rows[0].replication_v2 or rows[0].replication)
        expected_repl_strategy = 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        assert replication.get('class') == expected_repl_strategy, \
            f"Invalid replication class for keyspace {ks}: expected = {expected_repl_strategy}, actual = {replication.get('class')}"
        replication.pop('class')
        assert sorted_racks(replication) == sorted_racks(expected_replication), \
            f"Invalid replication options for keyspace {ks}: expected = {expected_replication}, actual = {replication}"

        assert await uses_tablets(cql, ks), f"Tablets not enabled for keyspace {ks}"

        # Verify tables exist
        rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'")
        found_tables = {row.table_name for row in rows}
        assert found_tables == tables

        # Verify tablet replicas
        for table in tables:
            tablets = await get_all_tablet_replicas(manager, servers[0], ks, table)
            assert tablets, f"No tablets found for {ks}.{table}"
            for tablet in tablets:
                dc_to_racks: dict[str, set[str]] = {}
                for host_id, _ in tablet.replicas:
                    dc, rack = host_to_dc_rack[host_id]
                    dc_to_racks.setdefault(dc, set()).add(rack)
                # Over the expected DCs, not the DCs with replicas: a DC the schema already
                # names but no replica has reached yet is a mismatch too.
                expected_dcs = {dc for dc, racks in expected_replication.items() if racks}
                assert set(dc_to_racks) == expected_dcs, \
                    f"Tablet replicas mismatch for {ks}.{table}: expected DCs {sorted(expected_dcs)}, got {sorted(dc_to_racks)}"
                for dc in expected_dcs:
                    expected_racks = set(expected_replication[dc])
                    racks = dc_to_racks.get(dc, set())
                    assert racks == expected_racks, f"Tablet replicas mismatch for {ks}.{table} in DC {dc}: expected racks {expected_racks}, got {racks}"

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
async def test_auto_rf_ks_coverage(manager: ScyllaClusterManager):
    """
    Every auto-RF keyspace (AUTO_RF_KEYSPACES) follows the anchor; the behaviour itself
    is covered by test_auto_rf_behavior.
    """
    cfg_audit = AUDIT_CFG

    logger.info("Create first rack and verify that the schemas are created")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=120)

    logger.info("Add a second rack and verify it is added to the RF")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=120)

    logger.info("Add a second dc with two racks and verify it is added to the RF")
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2, [{"dc": "dc2", "rack": "r1"}, {"dc": "dc2", "rack": "r2"}], cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2'], 'dc2': ['r1']})
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']})
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']}, timeout=120)


@pytest.mark.asyncio
async def test_auto_rf_behavior(manager: ScyllaClusterManager):
    """
    Verify all aspects of the automatic replication factor mechanism:
    * A node joining changes nothing, in an existing rack or a new one.
    * A rack is added once the anchor keyspace lists it, up to the goal and not beyond.
    * A DC is added once the anchor lists it.
    * Racks which the anchor no longer lists are given up again, and replaced where possible.
    * A rack's last node can be decommissioned once the rack is dropped from the anchor.

    The test subject is the audit keyspace; the anchor keyspace drives the eligible
    racks, see create_rack_anchor_keyspace().
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = AUDIT_CFG

    logger.info("Create first rack and verify that schema is created")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=120)

    logger.info("Check schema after restart")
    await asyncio.gather(*[manager.server_stop(s.server_id, convict=False) for s in servers])
    await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']})

    # The negative checks below make the reconciler evaluate the new state
    # (wait_for_auto_rf_settled) and compare the keyspace_rf_change tasks with those
    # known before the step: a plain schema read right after it could run before the
    # reconciler has looked, and would miss a change which was scheduled and failed.
    server0 = servers[0]
    logger.info("Add a node in an existing rack")
    tasks = await rf_change_task_ids(manager, server0, ks)
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    assert await rf_change_task_ids(manager, server0, ks) == tasks
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a second rack with two nodes and verify it is not added to the RF while it is not eligible")
    r2_servers = await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2, [{"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r2"}], cfg_audit)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    assert await rf_change_task_ids(manager, server0, ks) == tasks
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Make the second rack eligible and verify it is added to the RF")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=120)

    logger.info("Add a third rack and verify it is added to the RF")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r3"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)

    logger.info("Add a fourth rack and verify it is not added to the RF (RF goal 3 has been reached)")
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    tasks = await rf_change_task_ids(manager, server0, ks)
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r4"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3', 'r4']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    assert await rf_change_task_ids(manager, server0, ks) == tasks
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)

    logger.info("Add a node in a new dc and verify it is added to the RF of the new DC")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc2", "rack": "r1"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3', 'r4'], 'dc2': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']}, timeout=120)

    # The auto-RF keyspaces follow the anchor in both directions: a rack which
    # no eligible keyspace uses any more is given up, and the freed RF slot is
    # filled from the racks which are still eligible.
    logger.info("Drop the second rack from the anchor and verify auto-RF swaps it for the fourth rack")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3', 'r4'], 'dc2': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r3', 'r4'], 'dc2': ['r1']}, timeout=120)

    logger.info("Drop the fourth rack from the anchor and verify auto-RF gives it up and stays below the goal")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r3'], 'dc2': ['r1']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r3'], 'dc2': ['r1']}, timeout=0)
    await assert_no_pending_rf_change(cql, ks)

    logger.info("Make the second rack eligible again and verify auto-RF expands into it without an ALTER")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']}, timeout=120)

    logger.info("Decommission a node from a rack with multiple nodes")
    await manager.decommission_node(r2_servers[0].server_id)

    logger.info("Decommission the last node from a rack (expected to fail while the rack is still in a rack list)")
    await manager.decommission_node(
        r2_servers[1].server_id,
        expected_error="replicate to rack r2 of dc1")

    logger.info("Drop the rack from the anchor and retry decommission once the auto-RF keyspaces have given it up (expected to succeed)")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})
    # Wait until both auto-RF keyspaces have settled without r2, so that the
    # decommission below does not race an in-flight RF change.
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r3'], 'dc2': ['r1']}, timeout=120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, {'dc1': ['r1', 'r3'], 'dc2': ['r1']}, timeout=120)
    await manager.decommission_node(r2_servers[1].server_id)


@pytest.mark.asyncio
async def test_auto_rf_audit_ks_late_creation(manager: ScyllaClusterManager):
    """
    Verify that the audit keyspace can be created and auto-expanded on an existing cluster.

    The audit keyspace is not necessarily created by the first node in the cluster,
    but only when audit is enabled in the configuration. This can happen anytime,
    so make sure that auto RF works correctly even in such late creation scenarios.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES

    logger.info("Create a cluster with 2 DCs, 2 racks per DC, 1 node per rack, audit disabled")
    servers = []
    host_to_dc_rack = {}
    property_files = [
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc2", "rack": "r1"},
        {"dc": "dc2", "rack": "r2"},
    ]
    # Audit is enabled by default, it has to be disabled explicitly to
    # postpone the creation of the audit keyspace.
    cfg = {"tablet_load_stats_refresh_interval_in_seconds": "1", "audit": "none"} | RACK_LIST_CFG
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 4, property_files, config=cfg)

    logger.info("Verify the audit schema does not exist yet")
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
    assert len(rows) == 0, f"Keyspace {ks} should not exist yet"

    logger.info("Create the eligibility anchor keyspace covering both DCs")
    await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']})

    logger.info("Add a new node with audit enabled")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg | AUDIT_CFG)

    logger.info("Verify the audit schema is created with correct RF")
    # Two keyspaces, four racks, one rack per RF change: slow in debug.
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']}, timeout=300)


# ---------------------------------------------------------------------------
# Gating tests: a rack or DC is eligible only once some non-auto-RF tablets keyspace
# places replicas there (get_auto_rf_racks() in the topology coordinator); audit/system_traces follow
# the anchor keyspace.
# ---------------------------------------------------------------------------


async def wait_for_rf_change_task(manager: ScyllaClusterManager, server: ServerInfo, ks: str,
                                  before_tasks: set, timeout: float = 120.0) -> None:
    """
    Wait until a keyspace_rf_change task for `ks` which is not in `before_tasks`
    is scheduled AND completes successfully. Use this in positive tests that
    expect auto-RF to act.

    Note that the task may already be in the "done" state when we first see it:
    auto-RF changes are scheduled by the topology coordinator in parallel with
    the operation that triggered them, so a quick RF change can complete before
    the triggering statement returns to the client. Hence we only require that
    a *new* task shows up, not that we catch it in a non-final state.
    """
    task_mgr = TaskManagerClient(manager.api)
    deadline = time.time() + timeout

    new_tasks: set = set()
    while not new_tasks:
        new_tasks = await rf_change_task_ids(manager, server, ks) - before_tasks
        if new_tasks:
            break
        if time.time() >= deadline:
            raise AssertionError(f"no keyspace_rf_change task appeared for {ks} within {timeout}s")
        await asyncio.sleep(0.2)

    for task_id in new_tasks:
        logger.info(f"Waiting for rf_change task {task_id} on {ks} to complete")
        status = await task_mgr.wait_for_task(server.ip_addr, task_id)
        assert status.state == "done", (
            f"keyspace_rf_change task {task_id} for {ks} ended in state "
            f"{status.state}: {status.error}")
    # Nothing else may still be pending.
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server)


@pytest.mark.asyncio
async def test_auto_rf_expansion_gated_by_user_rack_list(manager: ScyllaClusterManager):
    """
    Verify that auto-RF on the audit keyspace only expands to racks that are
    already present in the rack list of some other (non-auto-RF) tablets
    keyspace.

    Scenario:
      1. Start cluster with dc1/r1, audit enabled. Create a user tablets
         keyspace restricted to dc1:['r1']. Wait for any initial auto-RF
         activity to settle.
      2. Add a node in dc1/r2. Assert that auto-RF does NOT schedule any
         keyspace_rf_change task for audit, and audit's replication is
         unchanged.
      3. ALTER the user keyspace to include r2. Wait for the auto-RF task to
         be scheduled and completed, and assert audit now includes r2.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = AUDIT_CFG

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a user tablets keyspace pinned to dc1:['r1']")
    user_ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r1']}")

    logger.info("Wait for any initial auto-RF activity on audit to settle")
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a node in dc1/r2 -- auto-RF must NOT schedule any task for audit")
    before_tasks = await rf_change_task_ids(manager, server0, ks)
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)
    # The wait makes the coordinator evaluate auto-RF, so a due change would show here.
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    after_tasks = await rf_change_task_ids(manager, server0, ks)
    new_tasks = after_tasks - before_tasks
    assert not new_tasks, (
        f"auto-RF unexpectedly scheduled {len(new_tasks)} keyspace_rf_change task(s) "
        f"for {ks} after adding r2 (task ids: {new_tasks})")
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info(f"ALTER {user_ks} to include r2 -- auto-RF must now add r2 to audit")
    await cql.run_async(
        f"ALTER KEYSPACE {user_ks} WITH replication = "
        f"{{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r2']}}")
    await wait_for_rf_change_task(manager, server0, ks, after_tasks)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=60)

    await cql.run_async(f"DROP KEYSPACE {user_ks}")


@pytest.mark.asyncio
async def test_auto_rf_expansion_gated_by_user_dc(manager: ScyllaClusterManager):
    """
    Same gating rule, but at the DC dimension: auto-RF must not add a new DC
    to audit's replication options until some non-auto-RF tablets keyspace
    has data in that DC.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = AUDIT_CFG

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a user tablets keyspace restricted to dc1:['r1']")
    user_ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r1']}")
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a node in a brand-new dc2/r1 -- auto-RF must NOT schedule any task for audit")
    before_tasks = await rf_change_task_ids(manager, server0, ks)
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc2", "rack": "r1"}, cfg_audit)
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    after_tasks = await rf_change_task_ids(manager, server0, ks)
    new_tasks = after_tasks - before_tasks
    assert not new_tasks, (
        f"auto-RF unexpectedly scheduled {len(new_tasks)} keyspace_rf_change task(s) "
        f"for {ks} after adding a node in dc2 (task ids: {new_tasks})")
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info(f"ALTER {user_ks} to include dc2 -- auto-RF must now add dc2 to audit")
    await cql.run_async(
        f"ALTER KEYSPACE {user_ks} WITH replication = "
        f"{{'class': 'NetworkTopologyStrategy', 'dc1': ['r1'], 'dc2': ['r1']}}")
    await wait_for_rf_change_task(manager, server0, ks, after_tasks)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables,
                        {'dc1': ['r1'], 'dc2': ['r1']}, timeout=60)

    await cql.run_async(f"DROP KEYSPACE {user_ks}")


@pytest.mark.asyncio
async def test_auto_rf_no_expansion_without_user_tablet_keyspace(manager: ScyllaClusterManager):
    """
    Without any non-auto-RF tablets keyspace nothing is eligible, so audit and
    system_traces stay at their initial RF 1 as racks and DCs are added. This is the
    intended behaviour, not a gap.
    """
    ks = AUDIT_KS
    cfg_audit = AUDIT_CFG

    logger.info("Start cluster with multiple racks across two DCs, audit enabled, no user tablet keyspace")
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(
        manager, servers, host_to_dc_rack, 4,
        [{"dc": "dc1", "rack": "r1"},
         {"dc": "dc1", "rack": "r2"},
         {"dc": "dc2", "rack": "r1"},
         {"dc": "dc2", "rack": "r2"}],
        cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Wait for audit keyspace to appear")
    async def wait_audit_exists():
        deadline = time.time() + 60
        while time.time() < deadline:
            rows = await cql.run_async(
                f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
            if len(rows) == 1:
                return
            await asyncio.sleep(1)
        raise AssertionError(f"audit keyspace did not appear within 60s")
    await wait_audit_exists()

    for auto_rf_ks, _, _ in AUTO_RF_KEYSPACES:
        assert await uses_tablets(cql, auto_rf_ks), f"{auto_rf_ks} must be on tablets, or the test checks nothing"

    # Read, not hardcoded: the point is that auto-RF leaves the replication alone.
    initial_replication = await _replication(cql, ks)
    logger.info(f"Initial replication options of {ks}: {initial_replication}")
    # All the nodes are up by now, so an expansion could be over already.
    assert all(len(racks) == 1 for racks in initial_replication.values()), (
        f"{ks} must list one rack per DC, got {initial_replication}")

    logger.info("Wait for the coordinator to settle, then assert no RF change task ran for audit")
    before_tasks = await rf_change_task_ids(manager, server0, ks)
    assert not before_tasks, f"auto-RF scheduled RF change task(s) for {ks} during the cluster start: {before_tasks}"
    # The wait makes the coordinator evaluate auto-RF, so a due change would show here.
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    after_tasks = await rf_change_task_ids(manager, server0, ks)
    new_tasks = after_tasks - before_tasks
    assert not new_tasks, (
        f"Without any non-auto-RF tablet keyspace, auto-RF should not schedule "
        f"any task for {ks}, but observed {len(new_tasks)} new task(s): {new_tasks}")

    replication = await _replication(cql, ks)
    assert replication == initial_replication, (
        f"Auto-RF unexpectedly modified {ks} replication: got {replication}, "
        f"expected {initial_replication}. Without any non-auto-RF tablet "
        f"keyspace, no rack should be eligible.")


@pytest.mark.asyncio
async def test_auto_rf_deferred_while_a_node_is_dead(manager: ScyllaClusterManager):
    """
    Auto-RF must not schedule an RF change while a normal node is dead.

    An RF change is carried out by tablet migrations, and every migration
    passes through global_tablet_token_metadata_barrier(), which drains all
    normal nodes. With a dead node the barrier cannot pass; the coordinator
    retries the migration once a second and does nothing else meanwhile (no
    resize decisions, no load balancing, no other requests). An operator can
    choose to ALTER KEYSPACE into that situation, auto-RF must not.

    Scenario:
      1. Three nodes, one per rack, audit enabled, no user tablets keyspace,
         so nothing is eligible and audit stays at dc1:['r1'].
      2. Stop node 3 and wait until the others see it down.
      3. Create a user keyspace on all three racks, the dead one included,
         which makes every rack eligible. Without the deferral auto-RF would
         immediately schedule audit r1 -> r1+r2 and wedge on the barrier.
         Assert that it logs the deferral and schedules nothing for audit.
      4. Restart node 3. Auto-RF must now expand audit to all three racks.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = AUDIT_CFG

    logger.info("Start cluster with three nodes in dc1/r1, r2, r3, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(
        manager, servers, host_to_dc_rack, 3,
        [{"dc": "dc1", "rack": "r1"},
         {"dc": "dc1", "rack": "r2"},
         {"dc": "dc1", "rack": "r3"}],
        cfg_audit)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    server0, server1, server2 = servers

    logger.info("Without a user tablets keyspace nothing is eligible; audit must stay at dc1:['r1']")
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Stop node 3 and wait until the other nodes see it down")
    await manager.server_stop(server2.server_id, convict=True)
    await manager.server_not_sees_other_server(server0.ip_addr, server2.ip_addr)
    await manager.server_not_sees_other_server(server1.ip_addr, server2.ip_addr)

    logger.info("Create a user keyspace covering all three racks: all live racks become eligible")
    before_tasks = await rf_change_task_ids(manager, server0, ks)
    before_requests = await count_rf_change_requests(cql, ks)
    logs = [await manager.server_open_log(s.server_id) for s in (server0, server1)]
    marks = [await log.mark() for log in logs]
    user_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await cql.run_async(f"CREATE TABLE {user_ks}.t (pk int PRIMARY KEY, c int)")

    logger.info("The coordinator evaluates auto-RF when the keyspace is created; it must defer, not schedule")
    # quiesce_topology() needs every node alive, so wait for the coordinator's log line.
    async def deferred():
        for log, mark in zip(logs, marks):
            if await log.grep("auto-rf: deferring RF changes .* until dead node\\(s\\) .* are alive again", from_mark=mark):
                return True
        return None
    await wait_for(deferred, time.time() + 60)
    after_tasks = await rf_change_task_ids(manager, server0, ks)
    new_tasks = after_tasks - before_tasks
    assert not new_tasks, (
        f"auto-RF scheduled {len(new_tasks)} keyspace_rf_change task(s) for {ks} "
        f"while a node was dead (task ids: {new_tasks})")
    # system.topology_requests is group0 state, so this check does not depend
    # on which node is the coordinator.
    new_requests = await count_rf_change_requests(cql, ks) - before_requests
    assert new_requests == 0, (
        f"auto-RF scheduled {new_requests} keyspace_rf_change request(s) for {ks} while a node was dead")
    await verify_schema(cql, manager, [server0, server1], host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Restart node 3; auto-RF must now expand audit to all three racks")
    await manager.server_start(server2.server_id)
    await manager.server_sees_other_server(server0.ip_addr, server2.ip_addr)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await wait_for_rf_change_task(manager, server0, ks, after_tasks)
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)

    await cql.run_async(f"DROP KEYSPACE {user_ks}")


# ---------------------------------------------------------------------------
# Bounded scheduling.
# ---------------------------------------------------------------------------


async def count_rf_change_requests(cql, ks: str) -> int:
    """Return the number of keyspace_rf_change requests ever scheduled for `ks`.

    system.topology_requests rows are written with a one month TTL, so this
    counts completed (including failed) requests as well as pending ones.
    """
    return len(await rf_change_requests(cql, ks, pending_only=False))


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_auto_rf_rejected_change_is_not_rescheduled_in_a_loop(manager: ScyllaClusterManager):
    """
    A keyspace_rf_change which the request handler rejects leaves no trace in group0,
    so the reconciler sees the same shortfall again on its next iteration. Without a
    backoff it would re-issue the request in a tight loop and starve every other
    topology operation.

    The rejection comes from the keyspace_rf_change_fail injection. Asserted: only a
    handful of requests over a fixed window, and convergence once the rejection is gone.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = AUDIT_CFG

    # The backoff (1 s, doubling) allows at most six requests in this window; a loop
    # would schedule hundreds.
    window = 30  # seconds
    max_scheduled = 10

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a rack-list anchor keyspace pinned to dc1:['r1'] and let auto-RF settle")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=server0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=120)

    logger.info("Add a node in dc1/r2, still ineligible for auto-RF")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)

    # The injection is keyspace-filtered because user ALTERs go through the same
    # request handler: the anchor ALTER below has to keep working. Every node
    # gets it - the coordinator may move.
    logger.info(f"Make every keyspace_rf_change for {ks} fail")
    injection = "keyspace_rf_change_fail"
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, injection, one_shot=False, parameters={"keyspace": ks})

    before = await count_rf_change_requests(cql, ks)

    logger.info(f"Make r2 eligible, so that auto-RF wants to expand {ks} into it")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})

    logger.info(f"Let the coordinator run for {window}s with the RF change failing")
    await asyncio.sleep(window)
    scheduled = await count_rf_change_requests(cql, ks) - before

    logger.info(f"auto-RF scheduled {scheduled} keyspace_rf_change request(s) for {ks} in {window}s")
    assert scheduled >= 2, (
        f"auto-RF scheduled {scheduled} keyspace_rf_change request(s) for {ks}, expected it to "
        f"attempt and retry the expansion into r2 - the test is not exercising the loop")
    assert scheduled <= max_scheduled, (
        f"auto-RF re-scheduled a rejected keyspace_rf_change for {ks} {scheduled} times in "
        f"{window}s, expected at most {max_scheduled}: failed requests are not being backed off")

    logger.info("Stop failing the RF change and verify auto-RF still converges")
    for s in servers:
        await manager.api.disable_injection(s.ip_addr, injection)
    # The coordinator has to come back on its own once the backoff elapses:
    # disabling an injection is not a topology event.
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=180)


async def _rack_list_sizes(cql) -> dict[str, int]:
    """Size of the dc1 rack list of every auto-RF keyspace."""
    sizes = {}
    for ks, _, _ in AUTO_RF_KEYSPACES:
        rf = (await _replication(cql, ks)).get('dc1', [])
        sizes[ks] = len(rf)
    return sizes


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quiesce_topology_waits_for_auto_rf(manager: ScyllaClusterManager):
    """
    A single quiesce_topology() issued right after the racks became eligible must not
    return before auto-RF has reached its goal, and must not be starved by a client that
    keeps resubmitting it: the reconciler only runs on an empty request queue, so the
    quiesce handler has to schedule the pending change itself. The tests rely on this
    (wait_for_auto_rf_settled()).

    The idle-path reconciler would otherwise make the changes before the quiesce arrives
    (creating the anchor keyspace waits for schema agreement), so it is held back with
    the auto_rf_skip_idle_path injection: every change has to come from the quiesce.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3,
                                     [{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}],
                                     cfg_audit)
    cql = manager.get_cql()
    await manager.api.quiesce_topology(servers[0].ip_addr)
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 1, SYSTEM_TRACES_KS: 1}

    injection = "auto_rf_skip_idle_path"
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)
    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    coordinator_log = await manager.server_open_log(coordinator.server_id)

    logger.info("Make all three racks eligible: with the idle path held back, nothing changes")
    await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 1, SYSTEM_TRACES_KS: 1}

    logger.info("A single quiesce_topology() schedules the changes and waits for them")
    mark = await coordinator_log.mark()
    start = time.time()
    await manager.api.quiesce_topology(servers[0].ip_addr)
    logger.info(f"quiesce_topology returned after {time.time() - start:.1f}s")
    assert await coordinator_log.grep("quiesce request deferred \\(1 request\\(s\\)\\): auto-RF change in progress", from_mark=mark), \
        "the quiesce was not deferred while the change it scheduled ran"
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 3, SYSTEM_TRACES_KS: 2}, "quiesce returned before auto-RF reached its goal"

    # Nothing may be left for auto-RF to do once quiesce has returned: the idle path
    # must not run another change.
    tasks = {ks: await rf_change_task_ids(manager, servers[0], ks) for ks, _, _ in AUTO_RF_KEYSPACES}
    for s in servers:
        await manager.api.disable_injection(s.ip_addr, injection)
    await wait_for_auto_rf_settled(manager, time.time() + 60)
    assert {ks: await rf_change_task_ids(manager, servers[0], ks) for ks, _, _ in AUTO_RF_KEYSPACES} == tasks, \
        "quiesce returned before auto-RF was done"
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_auto_rf_operator_rf_change_does_not_back_auto_rf_off(manager: ScyllaClusterManager):
    """
    Only the reconciler's own rejected changes back auto-RF off and are logged as
    warnings. An operator's rejected ALTER of an auto-RF keyspace is logged as an
    error, like any operator ALTER, and auto-RF acts at once when a change is due.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2,
                                     [{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}], cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1']}, timeout=0)

    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    coordinator_log = await manager.server_open_log(coordinator.server_id)
    mark = await coordinator_log.mark()

    logger.info(f"An operator's ALTER of {AUDIT_KS} is rejected")
    injection = "keyspace_rf_change_fail"
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, injection, one_shot=False, parameters={"keyspace": AUDIT_KS})
    with pytest.raises(InvalidRequest, match=injection):
        await cql.run_async(f"ALTER KEYSPACE {AUDIT_KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': ['r1', 'r2']}}")
    await coordinator_log.wait_for(f"ERROR.*Couldn't process global_topology_request::keyspace_rf_change for keyspace {AUDIT_KS}",
                                   from_mark=mark, timeout=60)
    for s in servers:
        await manager.api.disable_injection(s.ip_addr, injection)

    logger.info("Make r2 eligible: auto-RF extends audit without backing off")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=0)
    assert not await coordinator_log.grep(f"auto-rf: keyspace_rf_change for {AUDIT_KS} failed", from_mark=mark), \
        "the operator's rejected ALTER backed auto-RF off"


@pytest.mark.asyncio
async def test_auto_rf_expansion_is_bounded(manager: ScyllaClusterManager):
    """
    Guard against the reconciler doing more than it should: expanding two keyspaces
    from one rack to their goals takes exactly one keyspace_rf_change per added rack
    (audit 1->3: two, system_traces 1->2: one), and a bounded amount of group0 traffic.
    """
    cfg_audit = AUDIT_CFG
    servers = await manager.servers_add(3, config=cfg_audit, property_file=[
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}])
    cql = manager.get_cql()
    await manager.api.quiesce_topology(servers[0].ip_addr)

    async def topology_version() -> int:
        return (await cql.run_async("SELECT version FROM system.topology WHERE key='topology'"))[0].version

    before = {ks: await count_rf_change_requests(cql, ks) for ks, _, _ in AUTO_RF_KEYSPACES}
    version_before = await topology_version()

    await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await wait_for_auto_rf_settled(manager, time.time() + 300)
    await manager.api.quiesce_topology(servers[0].ip_addr)
    await wait_for_auto_rf_settled(manager, time.time() + 300)

    requests = {ks: await count_rf_change_requests(cql, ks) - before[ks] for ks, _, _ in AUTO_RF_KEYSPACES}
    version_bumps = await topology_version() - version_before
    logger.info(f"keyspace_rf_change requests: {requests}, topology version bumps: {version_bumps}")
    assert requests == {AUDIT_KS: 2, SYSTEM_TRACES_KS: 1}
    # A loose bound: it only has to catch re-issued changes or a step retried in a loop.
    assert version_bumps <= 120, f"{version_bumps} topology version bumps for three RF-change steps"
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 3, SYSTEM_TRACES_KS: 2}


@pytest.mark.asyncio
async def test_auto_rf_gives_up_racks_which_stop_being_eligible(manager: ScyllaClusterManager):
    """
    Auto-RF follows the non-auto-RF tablets keyspaces in both directions: a rack
    which none of them uses any more is dropped from the auto-RF keyspaces again,
    and the freed RF slot is left empty when no other rack is eligible. Dropping
    the last such keyspace altogether says nothing about where the operator wants
    the system keyspaces, so it leaves them alone.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()

    logger.info("Anchor on r1 and r2 first, so that system_traces (goal 2) takes r2, then add r3")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    # An automatic step must not put the keyspace's other options back to their defaults.
    await cql.run_async(f"ALTER KEYSPACE {AUDIT_KS} WITH durable_writes = false")
    assert not await durable_writes(cql, AUDIT_KS)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)
    assert not await durable_writes(cql, AUDIT_KS), f"adding a rack reset durable_writes of {AUDIT_KS}"
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    traces_racks = set((await _replication(cql, SYSTEM_TRACES_KS))['dc1'])
    assert traces_racks == {'r1', 'r2'}, f"system_traces expected on r1 and r2, got {traces_racks}"

    logger.info("Drop r2 from the anchor: audit has to give it up, system_traces has to leave it for r3")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r3']}, timeout=120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, {'dc1': ['r1', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await assert_no_pending_rf_change(cql, AUDIT_KS)
    await assert_no_pending_rf_change(cql, SYSTEM_TRACES_KS)
    assert not await durable_writes(cql, AUDIT_KS), f"giving up a rack reset durable_writes of {AUDIT_KS}"

    logger.info("Drop the anchor altogether: with nothing eligible the auto-RF keyspaces are left alone")
    await cql.run_async(f"DROP KEYSPACE {anchor_ks}")
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r3']}, timeout=0)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, {'dc1': ['r1', 'r3']}, timeout=0)
    await assert_no_pending_rf_change(cql, AUDIT_KS)
    await assert_no_pending_rf_change(cql, SYSTEM_TRACES_KS)


@pytest.mark.asyncio
async def test_auto_rf_lets_a_lost_rack_be_removed(manager: ScyllaClusterManager):
    """
    The rack-loss recovery procedure: the nodes of a lost rack are excluded, the
    rack is dropped from the keyspaces, and then the nodes are removed. The
    auto-RF keyspaces have to drop the rack on their own, while its nodes are
    dead, or the removenode cannot drain them ("No candidate nodes in dc1/r3 to
    drain ...") and fails. This is dtest's test_rack_loss_recovery in small.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()
    live = servers[0]
    lost = servers[2]

    logger.info("Anchor on r1 and r3 first, so that system_traces (goal 2) takes r3, then add r2")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r3']})
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=live)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=live)
    traces_racks = set((await _replication(cql, SYSTEM_TRACES_KS))['dc1'])
    assert traces_racks == {'r1', 'r3'}, f"system_traces does not replicate to r3: {traces_racks}"

    logger.info("Lose r3 for good: stop its node and exclude it")
    lost_host_id = await manager.get_host_id(lost.server_id)
    await manager.server_stop(lost.server_id, convict=True)
    await manager.api.exclude_node(live.ip_addr, [lost_host_id])

    logger.info("Drop r3 from the anchor: the auto-RF keyspaces have to follow although r3's node is dead")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, {'dc1': ['r1', 'r2']}, timeout=120)

    logger.info("Remove the lost node: nothing may reference r3 any more, so the drain has nothing to do")
    await manager.remove_node(live.server_id, lost.server_id)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=0)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_auto_rf_decommission_before_the_rack_is_given_up(manager: ScyllaClusterManager):
    """
    An operator who drops a rack from their keyspaces and decommissions its last node
    before auto-RF has given the rack up gets a rejection which names the system
    keyspaces still on the rack and says to retry; the retry succeeds once auto-RF is
    done. The hold-up is made explicit by rejecting audit's RF changes.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)

    logger.info("Hold audit on r3, drop r3 from the anchor and decommission its node")
    injection = "keyspace_rf_change_fail"
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, injection, one_shot=False, parameters={"keyspace": AUDIT_KS})
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    leaving = servers[2]
    await manager.decommission_node(leaving.server_id, expected_error="The system keyspaces audit")

    logger.info("Let auto-RF give r3 up; the decommission then succeeds")
    for s in servers:
        await manager.api.disable_injection(s.ip_addr, injection)
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=180)
    await manager.decommission_node(leaving.server_id)


@pytest.mark.asyncio
async def test_auto_rf_shrinks_a_dc_no_eligible_keyspace_uses_to_one_rack(manager: ScyllaClusterManager):
    """
    When the last non-auto-RF tablets keyspace stops replicating to a DC, the
    auto-RF keyspaces give that DC's racks up down to one: DCs are never dropped
    automatically, but everything beyond the last rack is, exactly as for a rack
    that stops being eligible in a DC which is still in use.
    """
    cfg_audit = AUDIT_CFG
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 4, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"},
        {"dc": "dc2", "rack": "r1"}, {"dc": "dc2", "rack": "r2"}], cfg_audit)
    cql = manager.get_cql()

    logger.info("Anchor on both racks of both DCs and let auto-RF expand into them")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)

    logger.info("Take the anchor out of dc2: audit keeps one rack there, system_traces too")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2'], 'dc2': 0})

    async def one_rack_left_in_dc2():
        audit = await _replication(cql, AUDIT_KS)
        traces = await _replication(cql, SYSTEM_TRACES_KS)
        return True if len(audit['dc2']) == 1 and len(traces['dc2']) == 1 else None
    await wait_for(one_rack_left_in_dc2, time.time() + 120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    audit = await _replication(cql, AUDIT_KS)
    traces = await _replication(cql, SYSTEM_TRACES_KS)
    assert sorted(audit['dc1']) == ['r1', 'r2'], f"dc1 must be untouched: {audit}"
    assert len(audit['dc2']) == 1, f"dc2 must keep exactly one rack: {audit}"
    assert sorted(traces['dc1']) == ['r1', 'r2'], f"dc1 must be untouched: {traces}"
    assert len(traces['dc2']) == 1, f"dc2 must keep exactly one rack: {traces}"
    await assert_no_pending_rf_change(cql, AUDIT_KS)
    await assert_no_pending_rf_change(cql, SYSTEM_TRACES_KS)
    # The replicas follow the schema: both racks of dc1, only the kept rack of dc2.
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2'], 'dc2': audit['dc2']}, timeout=120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES,
                        {'dc1': ['r1', 'r2'], 'dc2': traces['dc2']}, timeout=120)


@pytest.mark.asyncio
async def test_auto_rf_keyspace_creation_skips_a_dc_without_token_owners(manager: ScyllaClusterManager):
    """
    A DC made of zero-token nodes only (an arbiter DC) cannot host replicas, and a
    tablets keyspace which names it cannot be created. The audit keyspace is created
    while the node that enables auditing starts, so a failure would stop the node.
    The creation has to leave such a DC out.
    """
    no_audit = RACK_LIST_CFG | {"audit": "none"}
    await manager.server_add(config=no_audit, property_file={"dc": "dc1", "rack": "r1"})
    await manager.server_add(config=no_audit | {"join_ring": False}, property_file={"dc": "dc2", "rack": "r1"})
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{AUDIT_KS}'")
    assert not rows, "the audit keyspace must not exist before auditing is enabled"

    logger.info("Enable auditing on a new node: it creates the audit keyspace while starting")
    await manager.server_add(config=AUDIT_CFG, property_file={"dc": "dc1", "rack": "r2"})

    replication = await _replication(cql, AUDIT_KS)
    logger.info(f"audit replication: {replication}")
    assert set(replication) == {"dc1"}, f"dc2 has no token owners and must not be named: {replication}"
    assert len(replication["dc1"]) == 1, replication


@pytest.mark.asyncio
async def test_auto_rf_keyspaces_stay_on_vnodes_without_enforce_rack_list(manager: ScyllaClusterManager):
    """
    Without enforce_rack_list the auto-RF keyspaces are created on vnodes, with a fixed
    replication factor, and auto-RF leaves them alone, whatever the other tablets
    keyspaces do. Setting enforce_rack_list later does not move them to tablets, and
    being on vnodes they do not keep the nodes from starting with the option.
    """
    cfg = {"audit": "table", "enforce_rack_list": False}
    servers = await manager.servers_add(3, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}])
    cql = manager.get_cql()
    await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})

    async def check():
        await wait_for_auto_rf_settled(manager, time.time() + 120)
        for ks, _, _ in AUTO_RF_KEYSPACES:
            assert not await uses_tablets(cql, ks), f"{ks} must be on vnodes without enforce_rack_list"
            assert not await rf_change_task_ids(manager, servers[0], ks)
    initial = {ks: await _replication(cql, ks) for ks, _, _ in AUTO_RF_KEYSPACES}
    await check()

    logger.info("Restart the nodes with enforce_rack_list: the keyspaces stay on vnodes, untouched")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_update_config(s.server_id, "enforce_rack_list", True)
        await manager.server_start(s.server_id)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await check()
    assert {ks: await _replication(cql, ks) for ks, _, _ in AUTO_RF_KEYSPACES} == initial


@pytest.mark.asyncio
async def test_auto_rf_does_not_run_without_enforce_rack_list(manager: ScyllaClusterManager):
    """
    Auto-RF runs only where enforce_rack_list is set. If the option is turned off
    after the keyspaces were created on tablets, they are no longer managed, and the
    coordinator says so.
    """
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2,
                                     [{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}], AUDIT_CFG)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1']}, timeout=0)

    logger.info("Restart the nodes without enforce_rack_list and make r2 eligible")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_update_config(s.server_id, "enforce_rack_list", False)
        await manager.server_start(s.server_id)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    tasks = await rf_change_task_ids(manager, servers[0], AUDIT_KS)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await wait_for_auto_rf_settled(manager, time.time() + 120)

    assert await rf_change_task_ids(manager, servers[0], AUDIT_KS) == tasks
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1']}, timeout=0)
    # Whichever node was the coordinator when the reconciler looked.
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    assert any([await log.grep(f"auto-rf: {AUDIT_KS}, {SYSTEM_TRACES_KS} use tablets but enforce_rack_list is not set") for log in logs])
