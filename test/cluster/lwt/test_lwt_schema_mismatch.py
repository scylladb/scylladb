#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Repair of an unfinished Paxos round after a schema change, ported one test
per dtest from scylla-dtest paxos_tests.py (TestPaxos schema mismatch
tests). An LWT saves its proposal (a mutation tagged with the table's schema
version) and fails before learning it; the table's schema then changes. The
next LWT on the same key repairs the unfinished round, i.e. applies the old
mutation, which needs the old schema: from the schema registry if it is
still cached there (*_cache), or, after a restart cleared the registry, from
the schema history table (*_no_cache).

As in the dtests: one node (dc1/rack1) with every logger at trace level and
schema_registry_grace_period 1000, keyspace ks with RF=1 (vnodes), and the
dtests' driver sessions (see test_lwt_basic.py).
"""

import logging
import time

import pytest
from cassandra import WriteFailure

from test import DEBUG_MODES
from test.cluster.lwt.test_lwt_basic import VNODES, create_ks, patient_cql_connection
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


async def base_schema_mismatch_test_tpl(manager: ScyllaClusterManager, clear_schema_cache: bool, setup_test_env_action,
                                        insert_action, ddl_action, second_insert_action, verify_results_action):
    """The dtests' _base_schema_mismatch_test_tpl()."""
    # The tests need the schema registry cache not to be evicted meanwhile.
    config = {**VNODES, "schema_registry_grace_period": 1000}
    # The dtests' prepare(nodes=1, rf=1): one node in dc1/rack1; and
    # cluster.set_log_level("TRACE").
    # test.py's default command line sets raft_topology and query_processor
    # to debug, which overrides the default level: set them to trace too.
    node1 = await manager.server_add(config=config, property_file={"dc": "dc1", "rack": "rack1"},
                                     cmdline=["--default-log-level", "trace",
                                              "--logger-log-level", "raft_topology=trace",
                                              "--logger-log-level", "query_processor=trace"])
    time.sleep(0.2)
    session = patient_cql_connection(manager, node1)
    create_ks(session, "ks", 1)
    setup_test_env_action(session)

    # Fail at the end of the accept stage: the proposal is saved, the round
    # not completed.
    await manager.api.enable_injection(node1.ip_addr, "paxos_error_after_save_proposal", one_shot=True)
    key = 0
    with pytest.raises(WriteFailure):
        insert_action(session, key)

    ddl_action(session)

    # The dtests' disable_errors(): disable all remaining injections.
    await manager.api.client.delete("/v2/error_injection/injection", host=node1.ip_addr)

    if clear_schema_cache:
        # Restart the node to clear the schema_registry cache.
        await manager.server_stop_gracefully(node1.server_id)
        await manager.server_start(node1.server_id)
        session = patient_cql_connection(manager, node1)
        session.execute("USE ks")

    # The next round on the key repairs the previous one.
    second_insert_action(session, key)
    log = await manager.server_open_log(node1.server_id)
    await verify_results_action(session, log)


def insert_k_v(session, key):
    stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
    session.execute(stmt, [key, 0])


def insert_k(session, key):
    stmt = session.prepare("INSERT INTO test (k) VALUES (?) IF NOT EXISTS")
    session.execute(stmt, [key])


def add_dummy_column(session):
    session.execute("ALTER TABLE test ADD dummy int")


def drop_column(session):
    session.execute("ALTER TABLE test DROP v")


async def check_schema_mismatch_exc(session, log):
    exc_msg = await log.grep("<schema_mismatch_error>")
    if exc_msg:
        raise Exception(f'Unexpected "schema_mismatch_error" exception: {exc_msg}')


def check_exc_and_table_data(expected):
    async def check(session, log):
        exc_msg = await log.grep(r"exception during mutation write to ([0-9.]+): std::out_of_range \(regular column id 0 >= 0\)")
        if exc_msg:
            raise Exception(f"Unexpected exception during mutation write: {exc_msg}")
        res = [list(r) for r in session.execute("SELECT * from test")]
        assert res == [expected], f"Expected {[expected]} from SELECT * from test, but got {res}"
    return check


def create_test_table(session):
    session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")


def create_test_table_and_mv(session):
    session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
    session.execute("CREATE MATERIALIZED VIEW test_view AS SELECT * from test where k > 0 PRIMARY KEY(k)")


def create_test_table_v2(session):
    session.execute("CREATE TABLE test (k int PRIMARY KEY, v int, v2 int)")


def insert_k_v2(session, key):
    stmt = session.prepare("INSERT INTO test (k, v2) VALUES (?, ?) IF NOT EXISTS")
    session.execute(stmt, [key, 0])


async def schema_mismatch_tpl(manager, clear_schema_cache):
    """A column added after the failed LWT (#6502)."""
    await base_schema_mismatch_test_tpl(manager, clear_schema_cache, create_test_table, insert_k_v, add_dummy_column,
                                        insert_k_v, check_schema_mismatch_exc)


async def schema_mismatch_mv_tpl(manager, clear_schema_cache):
    """The same, on a table with a materialized view (#6074)."""
    await base_schema_mismatch_test_tpl(manager, clear_schema_cache, create_test_table_and_mv, insert_k_v, add_dummy_column,
                                        insert_k_v, check_schema_mismatch_exc)


async def schema_mismatch_drop_regular_column_tpl(manager, clear_schema_cache):
    """The column the failed LWT wrote is dropped (#6467)."""
    await base_schema_mismatch_test_tpl(manager, clear_schema_cache, create_test_table, insert_k_v, drop_column,
                                        insert_k, check_exc_and_table_data([0]))


async def schema_mismatch_drop_regular_column_in_the_middle_tpl(manager, clear_schema_cache):
    """A column before the one the failed LWT wrote is dropped, shifting the
    column ids (#6467)."""
    await base_schema_mismatch_test_tpl(manager, clear_schema_cache, create_test_table_v2, insert_k_v2, drop_column,
                                        insert_k, check_exc_and_table_data([0, 0]))


# The dtests run in non-release modes only (scylla_mode("!release")), and
# are next_gating (release only).
NOT_RELEASE = pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
NEXT_GATING = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_cache(manager: ScyllaClusterManager):
    await schema_mismatch_tpl(manager, clear_schema_cache=False)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_no_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_no_cache(manager: ScyllaClusterManager):
    await schema_mismatch_tpl(manager, clear_schema_cache=True)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_mv_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_mv_cache(manager: ScyllaClusterManager):
    await schema_mismatch_mv_tpl(manager, clear_schema_cache=False)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_mv_no_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_mv_no_cache(manager: ScyllaClusterManager):
    await schema_mismatch_mv_tpl(manager, clear_schema_cache=True)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_drop_regular_column_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_drop_regular_column_cache(manager: ScyllaClusterManager):
    await schema_mismatch_drop_regular_column_tpl(manager, clear_schema_cache=False)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_drop_regular_column_no_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_drop_regular_column_no_cache(manager: ScyllaClusterManager):
    await schema_mismatch_drop_regular_column_tpl(manager, clear_schema_cache=True)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_drop_regular_column_in_the_middle_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_drop_regular_column_in_the_middle_cache(manager: ScyllaClusterManager):
    await schema_mismatch_drop_regular_column_in_the_middle_tpl(manager, clear_schema_cache=False)


# Ported from scylla-dtest paxos_tests.py::TestPaxos::test_schema_mismatch_drop_regular_column_in_the_middle_no_cache.
@NEXT_GATING
@NOT_RELEASE
async def test_schema_mismatch_drop_regular_column_in_the_middle_no_cache(manager: ScyllaClusterManager):
    await schema_mismatch_drop_regular_column_in_the_middle_tpl(manager, clear_schema_cache=True)
