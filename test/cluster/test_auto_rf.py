#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time
import asyncio
import logging

import pytest

from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo, HostID
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.util import alter_keyspace_retry_ongoing_rf_change, create_new_test_keyspace, get_replica_count, parse_replication_options, rf_change_requests, wait_for_cql_and_get_hosts, wait_for_auto_rf_settled


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

# Auto-RF only expands to racks/DCs where some *non-auto-RF* tablets keyspace
# already places replicas, see
# service::topology_coordinator::get_racks_for_auto_rf_change().
# Tests which expect auto-RF to expand therefore have to provide such a
# keyspace as the "eligibility anchor". There are two flavors:
#
# * A numeric RF marks every rack of the listed DCs eligible. That is the
#   convenient anchor for tests which only care *that* expansion happens, but
#   it requires disabling rf_rack_valid_keyspaces (NUMERIC_RF_CFG): otherwise
#   CREATE/ALTER KEYSPACE expands the numeric RF into a rack list, which would
#   make only that single rack eligible.
# * An explicit rack list marks exactly the listed racks eligible. Tests which
#   assert *which* rack auto-RF picks have to use this flavor, because it is the
#   only one which gives the test control over the eligible set. It needs no
#   config override: prepare_options() passes an explicit rack list through
#   unchanged, so it works with the default rf_rack_valid_keyspaces=true.
NUMERIC_RF_CFG = {"rf_rack_valid_keyspaces": "false"}


def sorted_racks(replication: dict[str, list[str] | str]) -> dict[str, list[str] | str]:
    """
    Normalize replication options for comparison.

    A rack list is semantically a set: auto-RF appends the rack it picks, so the
    order of the racks in system_schema.keyspaces is not significant.
    """
    return {dc: sorted(rf) if isinstance(rf, list) else rf for dc, rf in replication.items()}


async def _replication(cql, ks: str) -> dict:
    """The replication options of `ks`, without the class."""
    rows = await cql.run_async(f"SELECT replication, replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
    assert len(rows) == 1, f"Keyspace {ks} not found"
    replication = parse_replication_options(rows[0].replication_v2 or rows[0].replication)
    replication.pop('class', None)
    return replication


def rack_list_opts(racks_per_dc: dict[str, list[str]]) -> str:
    return ", ".join(f"'{dc}': {racks}" for dc, racks in racks_per_dc.items())


async def create_anchor_keyspace(cql, dcs: list[str]) -> str:
    """Create a non-auto-RF tablets keyspace with numeric RF=1 in every DC in `dcs`."""
    opts = ", ".join(f"'{dc}': 1" for dc in dcs)
    return await create_new_test_keyspace(
        cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', {opts}}}")


async def alter_anchor_keyspace(cql, ks: str, dcs: list[str]) -> None:
    """Extend the anchor keyspace to cover every DC in `dcs`."""
    opts = ", ".join(f"'{dc}': 1" for dc in dcs)
    await cql.run_async(
        f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', {opts}}}")


async def create_rack_anchor_keyspace(cql, racks_per_dc: dict[str, list[str]]) -> str:
    """Create a non-auto-RF tablets keyspace replicating to exactly `racks_per_dc`."""
    return await create_new_test_keyspace(
        cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', {rack_list_opts(racks_per_dc)}}}")


async def set_anchor_racks(cql, ks: str, racks_per_dc: dict[str, list[str]]) -> None:
    """
    Set the rack lists of a rack-list anchor keyspace, i.e. the set of racks
    auto-RF is allowed to expand into.

    `racks_per_dc` has to list every DC the anchor currently replicates to:
    implicitly dropping a DC is rejected for tablets keyspaces. A single ALTER
    may change only one DC, and only by one rack, see
    cql3::statements::alter_keyspace_statement::validate().
    """
    await alter_keyspace_retry_ongoing_rf_change(
        cql, f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', {rack_list_opts(racks_per_dc)}}}")


async def assert_no_pending_rf_change(cql, ks: str) -> None:
    """Assert that auto-RF has not queued an RF change for `ks`."""
    rows = await rf_change_requests(cql, ks)
    assert not rows, f"Unexpected pending RF change requests for keyspace {ks}: {rows}"


async def add_servers_and_update_map(manager: ScyllaClusterManager, servers: list[ServerInfo], host_to_dc_rack: dict[HostID, tuple[str, str]], count: int, property_file: list[dict[str, str]] | dict[str, str], config: dict[str, str] | None = None) -> list[ServerInfo]:
    """Add multiple servers and update the host_to_dc_rack map incrementally."""
    new_servers = await manager.servers_add(count, property_file=property_file, config=config)
    servers.extend(new_servers)
    for server in new_servers:
        host_id = await manager.get_host_id(server.server_id)
        host_to_dc_rack[host_id] = (server.datacenter, server.rack)
    return new_servers


async def add_server_and_update_map(manager: ScyllaClusterManager, servers: list[ServerInfo], host_to_dc_rack: dict[HostID, tuple[str, str]], property_file: dict[str, str], config: dict[str, str] | None = None) -> ServerInfo:
    """Add a server and update the host_to_dc_rack map incrementally."""
    new_servers = await add_servers_and_update_map(manager, servers, host_to_dc_rack, 1, [property_file], config)
    return new_servers[0]


async def verify_schema(cql, manager: ScyllaClusterManager, servers: list[ServerInfo], host_to_dc_rack: dict[HostID, tuple[str, str]], ks: str, tables: set[str], expected_replication: dict[str, list[str]], timeout: int = 10, retry_interval: int = 1) -> None:
    async def _check():
        # Verify keyspace exists
        rows = await cql.run_async(f"SELECT replication, replication_v2 FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
        assert len(rows) == 1, f"Keyspace {ks} not found"

        # Verify replication options
        replication = parse_replication_options(rows[0].replication_v2 or rows[0].replication)
        expected_repl_strategy = 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        assert replication.get('class') == expected_repl_strategy, f"Invalid replication class for keyspace {ks}: expected = {expected_repl_strategy}, actual = {replication.get('class')}"
        replication.pop('class')
        assert sorted_racks(replication) == sorted_racks(expected_replication), f"Invalid replication options for keyspace {ks}: expected = {expected_replication}, actual = {replication}"

        # Verify tablets are enabled
        rows = await cql.run_async(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
        assert len(rows) == 1 and rows[0].initial_tablets is not None, f"Tablets not enabled for keyspace {ks}"

        # Verify tables exist
        rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'")
        found_tables = {row.table_name for row in rows}
        assert found_tables == tables

        # Verify tablet replicas
        for table in tables:
            tablets = await get_all_tablet_replicas(manager, servers[0], ks, table)
            for tablet in tablets:
                # Group replicas by DC and collect their racks
                dc_to_racks: dict[str, set[str]] = {}
                for host_id, _ in tablet.replicas:
                    dc, rack = host_to_dc_rack[host_id]
                    dc_to_racks.setdefault(dc, set()).add(rack)
                # Verify racks match expected replication options for each DC
                for dc, racks in dc_to_racks.items():
                    expected_racks = set(expected_replication.get(dc, []))
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
    Verify that Scylla applies the automatic replication factor to all eligible system keyspaces.
    The list of eligible keyspaces is currently hardcoded.
    Note: This is a coverage test, not a full behavioral test.
          The full auto RF functionality is tested in `test_auto_rf_behavior`.
    """
    cfg_audit = {"audit": "table"} | NUMERIC_RF_CFG

    logger.info("Create first rack and verify that the schemas are created")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_anchor_keyspace(cql, ['dc1'])
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=120)

    logger.info("Add a second rack and verify it is added to the RF")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=120)

    logger.info("Add a second dc with two racks and verify it is added to the RF")
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2, [{"dc": "dc2", "rack": "r1"}, {"dc": "dc2", "rack": "r2"}], cfg_audit)
    await alter_anchor_keyspace(cql, anchor_ks, ['dc1', 'dc2'])
    for ks, tables, _ in AUTO_RF_KEYSPACES:
        await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']}, timeout=120)


@pytest.mark.asyncio
async def test_auto_rf_behavior(manager: ScyllaClusterManager):
    """
    Verify all aspects of the automatic replication factor mechanism:
    * Per-DC replication factors are automatically expanded as nodes in new racks join the cluster.
    * Auto-RF only expands into racks which already hold non-auto-RF replicas.
    * Replication options are expanded to add new DCs when nodes in new DCs join the cluster.
    * Replication factors are not expanded beyond the RF goal.
    * Zero-token nodes do not trigger RF expansions.
    * Racks which no non-auto-RF tablets keyspace uses any more are given up again.
    * Rack decommission works once the rack is dropped from the anchor.

    This test uses the audit keyspace as the test subject. It drives the set of
    eligible racks through a rack-list anchor keyspace, which is what lets it
    assert *which* rack auto-RF picks - see the anchor comment above.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = {"audit": "table"}

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

    logger.info("Add a node in an existing rack")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a second rack with two nodes and verify it is not added to the RF while it is not eligible")
    r2_servers = await add_servers_and_update_map(manager, servers, host_to_dc_rack, 2, [{"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r2"}], cfg_audit)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)
    await assert_no_pending_rf_change(cql, ks)

    logger.info("Make the second rack eligible and verify it is added to the RF")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=120)

    logger.info("Add a third rack and verify it is added to the RF")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r3"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)

    logger.info("Add a fourth rack and verify it is not added to the RF (RF goal 3 has been reached)")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r4"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3', 'r4']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)
    await assert_no_pending_rf_change(cql, ks)

    logger.info("Add a node in a new dc and verify it is added to the RF of the new DC")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc2", "rack": "r1"}, cfg_audit)
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2', 'r3', 'r4'], 'dc2': ['r1']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']}, timeout=120)

    logger.info("Add a zero-token node in a new dc and verify the RF is not changed")
    cfg_zero_token = {"join_ring": "false"}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "zero-token-dc", "rack": "zero-token-rack"}, cfg_audit | cfg_zero_token)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2', 'r3'], 'dc2': ['r1']})

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
        expected_error="its removal would make some existing keyspace RF-rack-invalid")

    logger.info("Drop the rack from the anchor and retry decommission once the auto-RF keyspaces have given it up (expected to succeed)")
    # Dropping the rack from the anchor is all the operator has to do: the
    # auto-RF keyspaces give up a rack no eligible keyspace uses any more.
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3'], 'dc2': ['r1']})
    # Wait until both auto-RF keyspaces have settled without r2, so that the
    # decommission below does not race an in-flight RF change. system_traces has
    # an RF goal of 2, so it refills the freed slot with the remaining eligible rack.
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

    # 2 racks per DC is a reasonable number.
    # 3 would be closer to production setups, but would make the test slower (by ~60%).
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
    cfg = {"tablet_load_stats_refresh_interval_in_seconds": "1", "audit": "none"} | NUMERIC_RF_CFG
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 4, property_files, config=cfg)

    logger.info("Verify the audit schema does not exist yet")
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='{ks}'")
    assert len(rows) == 0, f"Keyspace {ks} should not exist yet"

    logger.info("Create the eligibility anchor keyspace covering both DCs")
    await create_anchor_keyspace(cql, ['dc1', 'dc2'])

    logger.info("Add a new node with audit enabled")
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg | {"audit": "table"})

    logger.info("Verify the audit schema is created with correct RF")
    # audit and system_traces interleave through the FIFO request queue one rack at a
    # time, and across two DCs that measured 130-150s in debug, so the old 120s window
    # sat below the real convergence time rather than above it.
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2'], 'dc2': ['r1', 'r2']}, timeout=300)


# ---------------------------------------------------------------------------
# Gating tests.
#
# These tests exercise the rule implemented in
# service::topology_coordinator::get_racks_for_auto_rf_change(): a rack (or
# DC) is only considered eligible for auto-RF expansion if some *non-auto-RF*
# tablets-enabled keyspace already places replicas there (either via an
# explicit rack list or via a numeric RF, which makes all racks in the DC
# eligible).
#
# These tests use an explicit user-created tablets keyspace as the
# "eligibility driver" and verify that audit/system_traces follow it.
# ---------------------------------------------------------------------------


async def _list_rf_change_tasks(manager: ScyllaClusterManager, server: ServerInfo, ks: str):
    """Return all keyspace_rf_change tasks known to the task manager for `ks`.

    This includes pending/running tasks and recently-completed tasks still
    within the user-task TTL window.
    """
    task_mgr = TaskManagerClient(manager.api)
    tasks = await task_mgr.list_tasks(server.ip_addr, "global_topology_requests", keyspace=ks)
    return [t for t in tasks if t.type == "keyspace_rf_change"]


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
        tasks = await _list_rf_change_tasks(manager, server, ks)
        new_tasks = {t.task_id for t in tasks} - before_tasks
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
    # And make sure nothing else is still pending (defense in depth).
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server, count_tasks=True)


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
    cfg_audit = {"audit": "table"}

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
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a node in dc1/r2 -- auto-RF must NOT schedule any task for audit")
    before_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)
    # Wait for the coordinator to settle after the node-add; this also waits
    # for any (erroneously) scheduled rf_change task to finish.
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    # Give the coordinator a few additional iterations to ensure it truly
    # decided not to schedule a change.
    await asyncio.sleep(5)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    after_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
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
    cfg_audit = {"audit": "table"}

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a user tablets keyspace restricted to dc1:['r1']")
    user_ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r1']}")
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Add a node in a brand-new dc2/r1 -- auto-RF must NOT schedule any task for audit")
    before_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc2", "rack": "r1"}, cfg_audit)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    await asyncio.sleep(5)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    after_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
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
async def test_auto_rf_numeric_user_keyspace_makes_all_racks_eligible(manager: ScyllaClusterManager):
    """
    If a non-auto-RF tablets keyspace uses a numeric RF (e.g. dc1: 1), then
    all racks in that DC are considered eligible for auto-RF expansion,
    because numeric-RF replicas may live on any rack.

    Verify that adding a new rack in such a setup *does* cause auto-RF to
    expand audit to the new rack (contrasting with the rack-list test above).
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    # rf_rack_valid_keyspaces has to be disabled, otherwise CREATE KEYSPACE
    # expands the numeric RF into a single-rack rack list and the keyspace
    # would only make that one rack eligible.
    cfg_audit = {"audit": "table"} | NUMERIC_RF_CFG

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a user tablets keyspace with numeric RF (dc1: 1)")
    user_ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}")
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=120)

    logger.info("Add a node in dc1/r2 -- all racks are eligible, so audit must expand to r2")
    before_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r2"}, cfg_audit)
    await wait_for_rf_change_task(manager, server0, ks, before_tasks)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1', 'r2']}, timeout=60)

    await cql.run_async(f"DROP KEYSPACE {user_ks}")


@pytest.mark.asyncio
async def test_auto_rf_no_expansion_without_user_tablet_keyspace(manager: ScyllaClusterManager):
    """
    Regression test for the chicken-and-egg case: when there is no
    non-auto-RF tablets keyspace in the cluster at all, auto-RF has nothing
    to anchor expansion on, so audit/system_traces must stay at their
    initial numeric RF=1 even as additional racks and DCs are added.

    This test codifies the current (intentional) behavior; if we ever
    decide to fall back to "all racks eligible" when no such keyspace
    exists, this test should be updated accordingly.
    """
    ks = AUDIT_KS
    cfg_audit = {"audit": "table"}

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

    # The replication options the keyspace was created with. Note that the
    # exact value depends on which node created the keyspace and on whether
    # the numeric RF was expanded into a rack list by CREATE KEYSPACE, so it
    # must not be hardcoded here -- the point of the test is that auto-RF
    # leaves it alone.
    initial_replication = await _replication(cql, ks)
    logger.info(f"Initial replication options of {ks}: {initial_replication}")

    logger.info("Wait for the coordinator to settle, then assert no RF change task ran for audit")
    before_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
    # Wait until nothing is pending/running; this returns immediately if the
    # coordinator correctly decided not to schedule anything.
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    # Give the coordinator additional iterations and recheck.
    await asyncio.sleep(10)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    after_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
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
      3. Create a user keyspace with RF 3. The test framework enables
         rack-list expansion, so it becomes dc1:['r1','r2','r3'], dead rack
         included, and every live rack becomes eligible for auto-RF. Without
         the deferral auto-RF would immediately schedule audit r1 -> r1+r2
         and wedge on the barrier. Assert no keyspace_rf_change task is
         scheduled for audit.
      4. Restart node 3. Auto-RF must now expand audit to all three racks.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = {"audit": "table"}

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
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    await verify_schema(cql, manager, servers, host_to_dc_rack, ks, tables, {'dc1': ['r1']}, timeout=0)

    logger.info("Stop node 3 and wait until the other nodes see it down")
    await manager.server_stop(server2.server_id, convict=True)
    await manager.server_not_sees_other_server(server0.ip_addr, server2.ip_addr)
    await manager.server_not_sees_other_server(server1.ip_addr, server2.ip_addr)

    logger.info("Create a user keyspace covering all three racks: all live racks become eligible")
    before_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
    before_requests = await count_rf_change_requests(cql, ks)
    user_ks = await create_new_test_keyspace(
        cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async(f"CREATE TABLE {user_ks}.t (pk int PRIMARY KEY, c int)")

    logger.info("Give the coordinator several iterations; it must defer, not schedule")
    await asyncio.sleep(10)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
    after_tasks = {t.task_id for t in await _list_rf_change_tasks(manager, server0, ks)}
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

    deferred = False
    for s in (server0, server1):
        log = await manager.server_open_log(s.server_id)
        if await log.grep("auto-rf: deferring RF changes .* until dead node\\(s\\) .* are alive again"):
            deferred = True
    assert deferred, "no node logged the auto-RF deferral"

    logger.info("Restart node 3; auto-RF must now expand audit to all three racks")
    await manager.server_start(server2.server_id)
    await manager.server_sees_other_server(server0.ip_addr, server2.ip_addr)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await wait_for_rf_change_task(manager, server0, ks, after_tasks)
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
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
    Regression test for the auto-RF scheduling loop.

    A keyspace_rf_change which the request handler rejects is dropped without
    leaving a trace in group0: the request is removed from the queue and marked
    done with the error. service::ongoing_rf_change() therefore finds nothing
    pending on the coordinator's next iteration,
    get_keyspaces_that_require_auto_rf_change() re-detects the same shortfall,
    and the very same request is scheduled again. For a rejection which is
    persistent this used to be an unbounded loop: 3480 requests at ~50 ms
    intervals were observed in one run, starving every other topology operation.

    The rejection is driven here by the keyspace_rf_change_fail injection. The
    tablet state which makes the real rejection persistent (a tablet holding
    fewer replicas than the keyspace's RF while every rack in the RF is still
    placeable) is transient and could not be constructed synthetically, and the
    defect is the unbounded re-issue rather than any particular rejection.

    Two things are asserted: that only a handful of requests are scheduled over
    a fixed window, and that auto-RF still converges once the rejection is gone.
    """
    ks = AUDIT_KS
    tables = AUDIT_TABLES
    cfg_audit = {"audit": "table"}

    # Long enough to leave the unfixed coordinator no excuse - at ~50 ms per
    # iteration it would schedule hundreds of requests in this window - while
    # the backoff (1s, doubling) allows at most six.
    window = 30
    max_scheduled = 10

    logger.info("Start cluster with 1 node in dc1/r1, audit enabled")
    servers = []
    host_to_dc_rack = {}
    await add_server_and_update_map(manager, servers, host_to_dc_rack, {"dc": "dc1", "rack": "r1"}, cfg_audit)
    cql = manager.get_cql()
    server0 = servers[0]

    logger.info("Create a rack-list anchor keyspace pinned to dc1:['r1'] and let auto-RF settle")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1']})
    await wait_for_auto_rf_settled(manager, time.time() + 120, ks=ks, server=server0, count_tasks=True)
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
    """Size of the dc1 rack list of every auto-RF keyspace, 0 for a numeric RF."""
    sizes = {}
    for ks, _, _ in AUTO_RF_KEYSPACES:
        rf = (await _replication(cql, ks)).get('dc1')
        sizes[ks] = len(rf) if isinstance(rf, list) else 0
    return sizes


@pytest.mark.asyncio
async def test_quiesce_topology_waits_for_auto_rf(manager: ScyllaClusterManager):
    """
    A single quiesce_topology() issued right after the racks became eligible must not
    return before auto-RF has reached its goal, and must not be starved by a client that
    keeps resubmitting it: the reconciler only runs on an empty request queue, so the
    quiesce handler has to schedule the pending change itself.
    """
    cfg_audit = {"audit": "table"}
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3,
                                     [{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}],
                                     cfg_audit)
    cql = manager.get_cql()
    await manager.api.quiesce_topology(servers[0].ip_addr)
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 1, SYSTEM_TRACES_KS: 1}

    logger.info("Make all three racks eligible and quiesce immediately")
    await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    start = time.time()
    await manager.api.quiesce_topology(servers[0].ip_addr)
    logger.info(f"quiesce_topology returned after {time.time() - start:.1f}s")

    # Nothing may be left for auto-RF to do once quiesce has returned.
    await wait_for_auto_rf_settled(manager, time.time() + 1)
    assert await _rack_list_sizes(cql) == {AUDIT_KS: 3, SYSTEM_TRACES_KS: 2}, "quiesce returned before auto-RF reached its goal"
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=0)


@pytest.mark.asyncio
async def test_auto_rf_expansion_is_bounded(manager: ScyllaClusterManager):
    """
    Guard against the reconciler doing more than it should: expanding two keyspaces
    from one rack to their goals takes exactly one keyspace_rf_change per added rack
    (audit 1->3: two, system_traces 1->2: one), and a bounded amount of group0 traffic.
    """
    cfg_audit = {"audit": "table"}
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
    # Measured 36 in dev on a three-node cluster; the bound only has to catch a
    # reconciler that re-issues changes or a step that is retried in a loop.
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
    cfg_audit = {"audit": "table"}
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()

    logger.info("Anchor on all three racks and let auto-RF expand into them")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    traces_racks = set((await _replication(cql, SYSTEM_TRACES_KS))['dc1'])
    assert len(traces_racks) == 2, f"system_traces expected on two racks, got {traces_racks}"

    logger.info("Drop r2 from the anchor: audit has to give it up, system_traces has to leave it if it was there")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r3']}, timeout=120)
    await verify_schema(cql, manager, servers, host_to_dc_rack, SYSTEM_TRACES_KS, SYSTEM_TRACES_TABLES, {'dc1': ['r1', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await assert_no_pending_rf_change(cql, AUDIT_KS)
    await assert_no_pending_rf_change(cql, SYSTEM_TRACES_KS)

    logger.info("Drop the anchor altogether: with nothing eligible the auto-RF keyspaces are left alone")
    await cql.run_async(f"DROP KEYSPACE {anchor_ks}")
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    await asyncio.sleep(5)
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
    cfg_audit = {"audit": "table"}
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()
    live = servers[0]
    lost = servers[2]

    logger.info("Anchor on all three racks and let auto-RF expand into them")
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=live)

    logger.info("Lose r3 for good: stop its node and exclude it")
    lost_host_id = await manager.get_host_id(lost.server_id)
    await manager.server_stop(lost.server_id, convict=True)
    await manager.api.exclude_node(live.ip_addr, [lost_host_id])

    logger.info("Drop r3 from the anchor: the auto-RF keyspaces have to follow although r3's node is dead")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120, server=live)
    traces_racks = set((await _replication(cql, SYSTEM_TRACES_KS))['dc1'])
    assert 'r3' not in traces_racks, f"system_traces still replicates to the lost rack: {traces_racks}"

    logger.info("Remove the lost node: nothing may reference r3 any more, so the drain has nothing to do")
    await manager.remove_node(live.server_id, lost.server_id)
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=0)


@pytest.mark.asyncio
async def test_auto_rf_decommission_right_after_the_anchor_drops_the_rack(manager: ScyllaClusterManager):
    """
    An operator who drops a rack from their keyspace and decommissions its last
    node right away races the reconciler: the auto-RF keyspaces still name the
    rack for a moment, and the decommission used to be rejected up front with
    "its removal would make some existing keyspace RF-rack-invalid". The removal
    has to wait for auto-RF to give the rack up instead. This is dtest's
    test_decommission_after_decreasing_rf in small.
    """
    cfg_audit = {"audit": "table"}
    servers = []
    host_to_dc_rack = {}
    await add_servers_and_update_map(manager, servers, host_to_dc_rack, 3, [
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}], cfg_audit)
    cql = manager.get_cql()
    anchor_ks = await create_rack_anchor_keyspace(cql, {'dc1': ['r1', 'r2', 'r3']})
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2', 'r3']}, timeout=120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)

    logger.info("Drop r3 from the anchor and decommission its node without waiting for auto-RF")
    await set_anchor_racks(cql, anchor_ks, {'dc1': ['r1', 'r2']})
    await manager.decommission_node(servers[2].server_id)
    servers.pop()
    await verify_schema(cql, manager, servers, host_to_dc_rack, AUDIT_KS, AUDIT_TABLES, {'dc1': ['r1', 'r2']}, timeout=60)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    traces_racks = set((await _replication(cql, SYSTEM_TRACES_KS))['dc1'])
    assert 'r3' not in traces_racks, f"system_traces still replicates to the decommissioned rack: {traces_racks}"


@pytest.mark.asyncio
async def test_auto_rf_shrinks_a_dc_no_eligible_keyspace_uses_to_one_rack(manager: ScyllaClusterManager):
    """
    When the last non-auto-RF tablets keyspace stops replicating to a DC, the
    auto-RF keyspaces give that DC's racks up down to one: DCs are never dropped
    automatically, but everything beyond the last rack is, exactly as for a rack
    that stops being eligible in a DC which is still in use.
    """
    cfg_audit = {"audit": "table"}
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
    from test.pylib.util import wait_for
    await wait_for(one_rack_left_in_dc2, time.time() + 120)
    await wait_for_auto_rf_settled(manager, time.time() + 120)
    audit = await _replication(cql, AUDIT_KS)
    assert sorted(audit['dc1']) == ['r1', 'r2'], f"dc1 must be untouched: {audit}"
    assert len(audit['dc2']) == 1, f"dc2 must keep exactly one rack: {audit}"
    await assert_no_pending_rf_change(cql, AUDIT_KS)
    await assert_no_pending_rf_change(cql, SYSTEM_TRACES_KS)


@pytest.mark.asyncio
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [True, False])
async def test_auto_rf_keyspace_creation_skips_a_dc_without_token_owners(manager: ScyllaClusterManager, rf_rack_valid_keyspaces: bool):
    """
    A DC made of zero-token nodes only (an arbiter DC) cannot host replicas. The
    auto-RF keyspaces are created with one replica per DC, and naming such a DC
    makes the creation fail: in the rack-list expansion with
    rf_rack_valid_keyspaces, in the tablet allocation without it. The audit
    keyspace is created while the node that enables auditing starts, so that
    failure would stop the node. The creation has to leave such a DC out.
    """
    cfg = {"rf_rack_valid_keyspaces": rf_rack_valid_keyspaces}
    no_audit = cfg | {"audit": "none"}
    await manager.server_add(config=no_audit, property_file={"dc": "dc1", "rack": "r1"})
    await manager.server_add(config=no_audit | {"join_ring": False}, property_file={"dc": "dc2", "rack": "r1"})
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{AUDIT_KS}'")
    assert not rows, "the audit keyspace must not exist before auditing is enabled"

    logger.info("Enable auditing on a new node: it creates the audit keyspace while starting")
    await manager.server_add(config=cfg | {"audit": "table"}, property_file={"dc": "dc1", "rack": "r2"})

    replication = await _replication(cql, AUDIT_KS)
    logger.info(f"audit replication: {replication}")
    assert set(replication) == {"dc1"}, f"dc2 has no token owners and must not be named: {replication}"
    assert get_replica_count(replication["dc1"]) == 1, replication
