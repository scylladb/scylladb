#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Materialized view writes while nodes fail or the base table is truncated,
# ported from scylla-dtest materialized_views_test.py, one test per dtest.
# On RF=3 vnodes clusters of three or four nodes, cassandra-stress first
# fills keyspace1.standard1, then runs, for a minute (two with a double
# failure), the user profile cs_mv_profile.yaml (mview.users with views by
# first and by last name: inserts and three kinds of reads) together with a
# "mixed" workload on standard1, while, 30 seconds in, one node stops, is
# removed or is decommissioned (and a new node joins), or two nodes stop (the
# second 10 seconds later). On a cluster of two DCs of three nodes, the user
# profile cs_mv_multidc_profile.yaml runs while all nodes of dc2 stop. After
# the stopped nodes start again and the views are built, the first node's
# log has no unexpected errors and the views have as many rows as the base
# table. The base table can also be truncated during the user profile
# workload, with and without auto_snapshot; the views are then still built.
# The cassandra-stress command lines are the dtests'.

import asyncio
import logging
import os
import re
import shlex
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.mv.dtest_mv_util import (add_node, check_errors, prepare, session_for, start_nodes, stop_nodes,
                                           wait_for_view)
from test.cluster.util import cassandra_stress, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

MV_PROFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cs_mv_profile.yaml")
MV_MULTIDC_PROFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cs_mv_multidc_profile.yaml")
STRESS_ERRORS_OPTIONS = ["-errors", "retries=10", "delay-policy=exponential", "min-delay-ms=20", "max-delay-ms=20000"]
# The log patterns the dtests' prepare() ignores.
PREPARE_IGNORED_ERRORS = [
    r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception",
    r".*view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*seastar::gate_closed_exception",
]
# And those _run_node_failure_during_mv_stress_insert() adds.
NODE_FAILURE_IGNORED_ERRORS = [
    r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: seastar::broken_promise",
    r"sstable - failed reading index .* std::bad_alloc",
    r"storage_proxy - exception during mutation write",
]


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def count_request_timeout(build_mode):
    """The dtests' count_request_timeout: cql_timeout(400)."""
    return 400 * (1 if build_mode == "release" else 3 if build_mode in DEBUG_MODES else 2)


def view_timeout(build_mode):
    """The dtests' wait_for_view() timeout, doubled in debug mode."""
    return 1200 if build_mode in DEBUG_MODES else 600


def stress_args(*options):
    """The dtests' stress options, each of which may hold several words."""
    return [arg for option in options for arg in shlex.split(option)]


async def stress_with_tailed_output(manager, server, *options, **kwargs):
    """Like the dtests' run_stress_with_tailed_output(): the workload's
    result is logged, not asserted; the test validates the data after."""
    try:
        output = await cassandra_stress(manager, server, *stress_args(*options), **kwargs)
        logger.info(output[-2000:])
    except AssertionError as e:
        logger.info(f"cassandra-stress failed: {e}")


def consistency_level(node_action, double_failure, total_rf, nodes):
    """The dtests' set_consistency_level()."""
    if double_failure:
        return ConsistencyLevel.ALL
    if node_action in ["stop", "restart", "decommission"] or total_rf > nodes:
        return ConsistencyLevel.QUORUM
    return ConsistencyLevel.ALL


async def validate_cs_results(manager, node, exclude_errors, cl, timeout):
    """The dtests' _validate_cs_results(): no unexpected errors in the
    node's log, and both views have as many rows as mview.users, read
    through an exclusive session to the node."""
    await check_errors(manager, node, exclude_errors)
    cql = session_for(manager, node, exclusive=True)
    try:
        cql.execute("USE mview")
        expected = list(cql.execute(SimpleStatement("select count(*) from mview.users", consistency_level=cl), timeout=timeout))[0][0]
        for view in ["users_by_first_name", "users_by_last_name"]:
            count = list(cql.execute(SimpleStatement(f"SELECT count(*) FROM {view}", consistency_level=cl), timeout=timeout))[0][0]
            assert count == expected, f"Expected a row count of {expected} in table '{view}', but got {count}"
    finally:
        cql.shutdown()


async def eventually(fun, trials=64):
    """The dtests' eventually(), for a coroutine function."""
    for i in range(trials - 1):
        try:
            return await fun()
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[{i + 1}/{trials}]: {e}: will retry in 1 second")
            await asyncio.sleep(1)
    return await fun()


async def wait_for_views(manager, cql, build_mode):
    alive = len(await manager.running_servers())
    for view in ["users_by_first_name", "users_by_last_name"]:
        await asyncio.to_thread(wait_for_view, cql, alive, "mview", view, view_timeout(build_mode))


async def run_node_failure_during_mv_stress_insert(manager, build_mode, rf, nodes, node_action, delay=30, duration="1m",
                                                   double_failure=False, exclude_errors=()):
    """The dtests' _run_node_failure_during_mv_stress_insert()."""
    timeout = count_request_timeout(build_mode)
    servers, cql = await prepare(manager, nodes=nodes, rf=rf, config={"range_request_timeout_in_ms": timeout * 1000},
                                 request_timeout=timeout)
    node1 = servers[0]
    n = 10000
    # The dtests pass "-mode cql3 native" too; cassandra_stress() sets the mode.
    output = await cassandra_stress(manager, node1, *stress_args("write", "cl=QUORUM", f"n={n}", "-schema replication(factor=3)",
                                                                 "-rate threads=10", f"-pop seq=1..{n}", *STRESS_ERRORS_OPTIONS), rf=3)
    logger.debug(output[-2000:])

    nodes_to_start = [servers[1]]
    if double_failure and len(servers) > 2:
        nodes_to_start.append(servers[2])
    removed = 0

    async def node_action_with_delay(node, delay):
        """The dtests' _node_action_with_delay()."""
        nonlocal removed
        await asyncio.sleep(delay)
        if node_action == "stop":
            await stop_nodes(manager, [node])
        elif node_action == "remove":
            # tools.misc.remove_node(): kill the node, wait until the others
            # see it down, and remove it through node1.
            await manager.server_stop(node.server_id, convict=False)
            await manager.others_not_see_server(node.ip_addr)
            await manager.remove_node(node1.server_id, node.server_id)
            removed += 1
        elif node_action == "decommission":
            await manager.decommission_node(node.server_id)
            await add_node(manager)

    await asyncio.gather(
        stress_with_tailed_output(manager, node1, "user", f"profile={MV_PROFILE}", "no-warmup", "cl=ONE", f"duration={duration}",
                                  "ops(insert=1,read1=1,read2=1,read3=1)", "-rate threads=10", *STRESS_ERRORS_OPTIONS,
                                  create_keyspace=False),
        stress_with_tailed_output(manager, node1, "mixed", "cl=ONE", f"duration={duration}", "-schema replication(factor=3)",
                                  "-rate threads=10", f"-pop dist=UNIFORM(1..{n})", "-log interval=5", *STRESS_ERRORS_OPTIONS,
                                  create_keyspace=False),
        *[node_action_with_delay(s, delay + 10 * i) for i, s in enumerate(nodes_to_start)])

    # The view build does not finish while updates cannot be sent.
    if node_action == "stop":
        await start_nodes(manager, nodes_to_start)
    await wait_for_views(manager, cql, build_mode)

    cl = consistency_level(node_action, double_failure, rf, len(servers) - removed)
    exclude = PREPARE_IGNORED_ERRORS + NODE_FAILURE_IGNORED_ERRORS + [re.escape(e) for e in exclude_errors]
    await eventually(lambda: validate_cs_results(manager, node1, exclude, cl, timeout))


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_stop_node_during_mv_insert_4_nodes.
# A node of four stops during view writes, and starts again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_stop_node_during_mv_insert_4_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=4, node_action="stop",
                                                   exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_stop_node_during_mv_insert_3_nodes.
# A node of three stops during view writes, and starts again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_stop_node_during_mv_insert_3_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=3, node_action="stop",
                                                   exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_remove_node_during_mv_insert_4_nodes.
# A node of four is killed and removed during view writes.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_remove_node_during_mv_insert_4_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=4, node_action="remove",
                                                   exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_decommission_node_during_mv_insert_4_nodes.
# A node of four is decommissioned during view writes, and a new node joins.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_decommission_node_during_mv_insert_4_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=4, node_action="decommission",
                                                   exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_remove_node_during_mv_insert_3_nodes.
# A node of three is killed and removed during view writes.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_remove_node_during_mv_insert_3_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=3, node_action="remove",
                                                   exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_double_node_failure_during_mv_insert_4_nodes.
# Two nodes of four stop, 10 seconds apart, during two minutes of view
# writes, and start again. (The dtest is not in next_gating: stress failed
# with hints overload, scylla-dtest#3372.)
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_double_node_failure_during_mv_insert_4_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=4, node_action="stop", duration="2m",
                                                   double_failure=True, exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_double_node_failure_during_mv_insert_3_nodes.
# Two nodes of three stop, 10 seconds apart, during two minutes of view
# writes, and start again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_double_node_failure_during_mv_insert_3_nodes(manager: ScyllaClusterManager, build_mode):
    await run_node_failure_during_mv_stress_insert(manager, build_mode, rf=3, nodes=3, node_action="stop", duration="2m",
                                                   double_failure=True, exclude_errors=["mutation_write_timeout_exception"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_multidc_dc_failure_during_mv_insert.
# Two DCs of three nodes (dc1 RF=2, dc2 RF=1): all dc2 nodes stop, one after
# the other, 30 seconds into two minutes of view writes at QUORUM, and start
# again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_multidc_dc_failure_during_mv_insert(manager: ScyllaClusterManager, build_mode):
    timeout = count_request_timeout(build_mode)
    servers, cql = await prepare(manager, nodes=[3, 3], rf={"dc1": 2, "dc2": 1},
                                 config={"range_request_timeout_in_ms": timeout * 1000}, request_timeout=timeout)
    node1_dc1 = servers[0]
    dc2 = servers[3:]

    async def stop_few_nodes(delay):
        """The dtests' _stop_few_nodes(by_dc_name="dc2"): graceful stops,
        not waiting for the other nodes to notice."""
        await asyncio.sleep(delay)
        for s in dc2:
            await manager.server_stop_gracefully(s.server_id)

    await asyncio.gather(
        stress_with_tailed_output(manager, node1_dc1, "user", f"profile={MV_MULTIDC_PROFILE}", "no-warmup", "cl=QUORUM", "duration=2m",
                                  "ops(insert=3,read1=1,read2=1,read3=1)", "-rate threads=10", *STRESS_ERRORS_OPTIONS,
                                  create_keyspace=False),
        stop_few_nodes(30))

    # The view build does not finish while updates cannot be sent.
    for s in dc2:
        await manager.server_start(s.server_id)
    await wait_for_views(manager, cql, build_mode)

    cl = consistency_level("", True, 3, len(servers))
    exclude = PREPARE_IGNORED_ERRORS + [re.escape("mutation_write_timeout_exception")]
    await eventually(lambda: validate_cs_results(manager, node1_dc1, exclude, cl, timeout))


async def truncate_base_during_mv_insert(manager, build_mode, auto_snapshot):
    """
    The dtests' _truncate_base_during_mv_insert(): the base table is
    truncated during view writes. The views are still built; the data is
    not validated, as what was truncated is not known.
    """
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"auto_snapshot": auto_snapshot})
    node1 = servers[0]

    async def truncate_table(ks="mview", table="users", delay=30, table_creation_timeout=300):
        """The dtests' _truncate_table(): once cassandra-stress has created
        the table, wait for delay seconds and truncate it, with a server and
        client timeout of cql_timeout(300)."""
        session = session_for(manager, node1)
        try:
            deadline = time.time() + table_creation_timeout
            while not list(session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s", (ks, table))):
                assert time.time() < deadline, f"Waiting for table '{ks}.{table}' to be created"
                await asyncio.sleep(1)
            await asyncio.sleep(delay)
            # A truncate flushes the table first, and with auto_snapshot also snapshots it, which under the concurrent
            # workload can take well over the default 30s request timeouts. So raise both the server's timeout and,
            # a bit beyond it so that the server's error is the one reported, the client's.
            timeout = 300 * (1 if build_mode == "release" else 3 if build_mode in DEBUG_MODES else 2)  # the dtests' cql_timeout(300)
            await asyncio.to_thread(session.execute, f"TRUNCATE table {ks}.{table} USING TIMEOUT {timeout}s", timeout=timeout + 30)
        finally:
            session.shutdown()

    # Like the dtest's node1.stress(), a failed workload fails the test.
    await asyncio.gather(
        cassandra_stress(manager, node1, *stress_args("user", f"profile={MV_PROFILE}", "no-warmup", "cl=QUORUM", "duration=1m",
                                                      "ops(insert=3,read1=1,read2=1,read3=1)", "-rate threads=10", *STRESS_ERRORS_OPTIONS),
                         create_keyspace=False),
        truncate_table())

    await wait_for_views(manager, cql, build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_truncate_base_during_mv_insert_test_with_auto_snapshot.
# The truncation takes a snapshot first.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_truncate_base_during_mv_insert_test_with_auto_snapshot(manager: ScyllaClusterManager, build_mode):
    await truncate_base_during_mv_insert(manager, build_mode, auto_snapshot=True)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_truncate_base_during_mv_insert_test_without_auto_snapshot.
# The truncation takes no snapshot.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_truncate_base_during_mv_insert_test_without_auto_snapshot(manager: ScyllaClusterManager, build_mode):
    await truncate_base_during_mv_insert(manager, build_mode, auto_snapshot=False)
