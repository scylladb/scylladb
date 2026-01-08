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
