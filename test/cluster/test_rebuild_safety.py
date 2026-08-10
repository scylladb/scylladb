#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tests for the safety check in nodetool rebuild when selecting a source DC.

PR #16827 added logic in repair/repair.cc that detects when it is unsafe
to rebuild from a given source datacenter (due to insufficient replicas)
and either rejects the request, falls back to an alternative DC, or allows
the user to override with --force.

The rebuilt node is always counted as "lost" in its own DC.  A source_dc
is considered unsafe when:
    lost > 1  ||  (lost == 1 && rf <= 1)

The safety check applies to ALL keyspaces (including system keyspaces
like "audit" which may have different RF than user keyspaces).  In a
2-DC cluster, some system keyspaces may have RF=0 in one DC, which
makes rebuilding from that DC always unsafe for those keyspaces.

Note: Tablets-enabled keyspaces are skipped by rebuild (rebuild is not
supported for them), so the safety check only applies to non-tablet
keyspaces.

Covers SCYLLADB-564.
"""
import logging
import time

import pytest

from cassandra.query import SimpleStatement, ConsistencyLevel  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.util import new_test_keyspace, new_test_table, wait_for_expected_rows, \
    wait_for_no_pending_topology_transition, wait_for_replacement_propagation, \
    wait_for_token_ring_and_group0_consistency


logger = logging.getLogger(__name__)

# The error substring emitted by repair/repair.cc:2133 when the safety
# check fires.  We match on a prefix so we don't depend on the exact DC
# name or RF value in the message.
UNSAFE_ERROR = "it is unsafe to use source_dc="

# Rebuild skips tablet-enabled keyspaces, so the safety check only applies to
# non-tablet keyspaces.
_KS_OPTS = "WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {rf_dc1}, 'dc2': {rf_dc2}}}" \
           " AND tablets = {{'enabled': false}}"
_TABLE_SCHEMA = "pk int PRIMARY KEY, v int"
N_ROWS = 100

# Per-convergence-check deadline. Each individual wait (topology transition,
# token-ring/group0 consistency, per-node row count) gets its own fresh deadline,
# since the operations run sequentially. Note the composite helpers chain several
# of these, so the effective worst case is a multiple of this value:
# _wait_topology_quiesced = 2x, _wait_for_rebuilt_node_ready = 3x (and then scaled
# by the build mode factor via scale_timeout()).
_STEP_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deadline(scale_timeout) -> float:
    """A fresh, mode-scaled deadline for a single topology/data convergence step."""
    return time.time() + scale_timeout(_STEP_TIMEOUT_S)


async def _insert_data(manager: ManagerClient, table: str, n_rows: int = N_ROWS):
    """Insert test data into the table.

    Uses CL=ALL so every replica in both DCs holds the rows before any
    stop/replace/rebuild. This removes the write-vs-topology race: a CL=ONE
    write could leave the only acked copy on the node we are about to stop,
    so a later rebuild would stream from a source that is missing rows.
    """
    cql = manager.get_cql()
    stmt = SimpleStatement(
        f"INSERT INTO {table} (pk, v) VALUES (%s, %s)",
        consistency_level=ConsistencyLevel.ALL,
    )
    for i in range(n_rows):
        await cql.run_async(stmt, parameters=[i, i * 10])


async def _wait_topology_quiesced(manager: ManagerClient, scale_timeout) -> None:
    """Wait until the cluster-global topology state has converged.

    This is cluster-wide (not per-node), so it only needs to run once after a
    topology operation, not once per server.
    """
    await wait_for_no_pending_topology_transition(manager, _deadline(scale_timeout))
    await wait_for_token_ring_and_group0_consistency(manager, _deadline(scale_timeout))


async def _wait_for_rebuilt_node_ready(manager: ManagerClient, target, table: str,
                                       scale_timeout, n_rows: int = N_ROWS):
    """After a rebuild, wait for topology to quiesce and the rebuilt node to serve all rows."""
    await _wait_topology_quiesced(manager, scale_timeout)
    await wait_for_expected_rows(manager, table, target.ip_addr, n_rows, deadline=_deadline(scale_timeout))


async def _setup_two_dc_cluster(manager: ManagerClient, nodes_per_dc: int = 3,
                                 extra_config: dict | None = None):
    """Create a 2-DC cluster with `nodes_per_dc` nodes in each DC.

    Returns (dc1_servers, dc2_servers).
    """
    cfg = {"endpoint_snitch": "GossipingPropertyFileSnitch"}
    if extra_config:
        cfg.update(extra_config)
    dc1_servers = await manager.servers_add(nodes_per_dc, config=cfg,
                                            auto_rack_dc="dc1")
    dc2_servers = await manager.servers_add(nodes_per_dc, config=cfg,
                                            auto_rack_dc="dc2")
    return dc1_servers, dc2_servers


# ---------------------------------------------------------------------------
# Test: RF=1 safety — unsafe dc, safe dc, auto-fallback, force override
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rebuild_safety_rf1(manager: ManagerClient, scale_timeout):
    """With RF=1, rebuilding from the node's own DC is unsafe (lost=1, rf=1).

    Exercises four rebuild scenarios within one cluster:
      1. rebuild from dc1 (safe — dc1 has 0 lost nodes) -> succeeds
      2. rebuild from dc2 (unsafe — dc2 has lost=1, rf<=1) -> fails
      3. rebuild with no source_dc (auto-fallback)          -> succeeds
      4. rebuild from dc2 with --force                      -> succeeds
    """
    _dc1_servers, dc2_servers = await _setup_two_dc_cluster(manager)
    target = dc2_servers[0]

    async with new_test_keyspace(manager, _KS_OPTS.format(rf_dc1=1, rf_dc2=1)) as ks:
        async with new_test_table(manager, ks, _TABLE_SCHEMA) as table:
            await _insert_data(manager, table)
            await _wait_topology_quiesced(manager, scale_timeout)

            logger.info("Step 1: rebuild from dc1 (safe source) should succeed")
            await manager.rebuild_node(target.server_id, source_dc="dc1")
            await _wait_for_rebuilt_node_ready(manager, target, table, scale_timeout)

            logger.info("Step 2: rebuild from dc2 (unsafe source, RF=1) should fail")
            await manager.rebuild_node(target.server_id, source_dc="dc2",
                                       expected_error=UNSAFE_ERROR)

            logger.info("Step 3: rebuild with no source_dc (auto-fallback) should succeed")
            await manager.rebuild_node(target.server_id)
            await _wait_for_rebuilt_node_ready(manager, target, table, scale_timeout)

            logger.info("Step 4: rebuild from dc2 with force should succeed")
            await manager.rebuild_node(target.server_id, source_dc="dc2", force=True)
            await _wait_for_rebuilt_node_ready(manager, target, table, scale_timeout)


# ---------------------------------------------------------------------------
# Test: RF=2 and RF=3 safety — rebuild from a different DC
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [2, 3])
async def test_rebuild_safety_higher_rf(manager: ManagerClient, rf: int, scale_timeout):
    """With RF > 1 for user keyspaces, rebuilding from dc1 (the other DC)
    is safe because dc1 has no lost nodes.

    Rebuilding from dc2 (the node's own DC) may still fail because system
    keyspaces (e.g. "audit") can have RF=0 in dc2, triggering the safety
    check.  We verify that:
      1. rebuild from dc1 succeeds (no lost nodes in dc1)
      2. rebuild from dc2 fails (system keyspaces with rf=0 make it unsafe)
      3. rebuild with no source_dc succeeds (auto-fallback)

    NOTE (RF=2 case): Per repair.cc:2129-2131, even if the user keyspace
    has RF=2 (and thus lost=1, rf>1 is "safe"), system keyspaces with
    RF=0 still trip the check.
    """
    _dc1_servers, dc2_servers = await _setup_two_dc_cluster(manager)
    target = dc2_servers[0]

    async with new_test_keyspace(manager, _KS_OPTS.format(rf_dc1=rf, rf_dc2=rf)) as ks:
        async with new_test_table(manager, ks, _TABLE_SCHEMA) as table:
            await _insert_data(manager, table)
            await _wait_topology_quiesced(manager, scale_timeout)

            logger.info("Step 1: rebuild from dc1 with user RF=%d should succeed "
                        "(dc1 has 0 lost nodes)", rf)
            await manager.rebuild_node(target.server_id, source_dc="dc1")
            await _wait_for_rebuilt_node_ready(manager, target, table, scale_timeout)

            logger.info("Step 2: rebuild from dc2 fails due to system keyspaces "
                        "with RF=0 in dc2")
            await manager.rebuild_node(target.server_id, source_dc="dc2",
                                       expected_error=UNSAFE_ERROR)

            logger.info("Step 3: rebuild with no source_dc (auto-fallback) should succeed")
            await manager.rebuild_node(target.server_id)
            await _wait_for_rebuilt_node_ready(manager, target, table, scale_timeout)


# ---------------------------------------------------------------------------
# Test: Data-wipe + replace scenario (full ticket repro)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rebuild_safety_after_replace(manager: ManagerClient, scale_timeout):
    """Full scenario from the SCYLLADB-564 ticket:
    insert data -> stop dc2 node -> replace via host_id -> test rebuild safety.

    With RF=1 in both DCs, the replaced node has no data.  Rebuilding from
    dc1 (which has all the data) should succeed, while rebuilding from dc2
    should be rejected by the safety check.
    """
    _dc1_servers, dc2_servers = await _setup_two_dc_cluster(manager)
    target = dc2_servers[0]

    async with new_test_keyspace(manager, _KS_OPTS.format(rf_dc1=1, rf_dc2=1)) as ks:
        async with new_test_table(manager, ks, _TABLE_SCHEMA) as table:
            await _insert_data(manager, table)
            await _wait_topology_quiesced(manager, scale_timeout)
            old_host_id = await manager.get_host_id(target.server_id)
            old_ip = target.ip_addr

            logger.info("Stopping target node %s for replacement", target)
            await manager.server_stop_gracefully(target.server_id)

            logger.info("Replacing target node via host_id")
            replace_cfg = ReplaceConfig(
                replaced_id=target.server_id,
                reuse_ip_addr=False,
                use_host_id=True,
            )
            replaced = await manager.server_add(
                replace_cfg,
                property_file={"dc": "dc2", "rack": "rack1"},
            )
            await wait_for_replacement_propagation(manager, replaced, old_host_id, old_ip, _deadline(scale_timeout))
            await _wait_for_rebuilt_node_ready(manager, replaced, table, scale_timeout)

            logger.info("Step 1: rebuild replaced node from dc1 (safe) should succeed")
            await manager.rebuild_node(replaced.server_id, source_dc="dc1")
            await _wait_for_rebuilt_node_ready(manager, replaced, table, scale_timeout)

            logger.info("Step 2: rebuild replaced node from dc2 (unsafe, RF=1) should fail")
            await manager.rebuild_node(replaced.server_id, source_dc="dc2",
                                       expected_error=UNSAFE_ERROR)

            logger.info("Step 3: rebuild replaced node with no source_dc (auto-fallback) "
                        "should succeed")
            await manager.rebuild_node(replaced.server_id)
            await _wait_for_rebuilt_node_ready(manager, replaced, table, scale_timeout)
