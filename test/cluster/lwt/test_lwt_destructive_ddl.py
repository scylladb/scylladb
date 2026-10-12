#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Destructive DDL in presence of LWT, ported from scylla-dtest
# lwt_destructive_ddl_test.py: destructive DDL statements (which make the
# LWT statements running concurrently invalid) run in a loop in four
# sessions, while LWT statements run against the test table in four other
# sessions, for 30 s (90 s in debug mode). Scylla must not crash, and both
# the LWT statements and the DDL must succeed at times, so that they really
# race. The cluster is the dtests': three nodes in datacenter1, one per
# rack, and keyspace ks with RF 3.

import asyncio
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import AlreadyExists, ConsistencyLevel, DriverException, OperationTimedOut, Unavailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.protocol import ConfigurationException

from test.cluster.dtest_helpers import topology
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The dtests' clusters: vnodes and, unless a test sets up auth, ccm's
# default AllowAll authenticator and authorizer.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled",
          "authenticator": "AllowAllAuthenticator", "authorizer": "AllowAllAuthorizer",
          # Scylla's default, as on the dtest nodes; test.py turns it off.
          "auto_snapshot": True}
AUTH_CONFIG = {
    "tablets_mode_for_new_keyspaces": "disabled",
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    "auto_snapshot": True,
    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
    "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
    # An "almost disabled" permissions cache, so permission changes take
    # effect soon.
    "permissions_validity_in_ms": 1,
}
# generate_cluster_topology(rack_num=3)
TOPOLOGY = topology({"rack1": 1, "rack2": 1, "rack3": 1})
CREATE_TEST_TABLE = """
            CREATE TABLE IF NOT EXISTS ks.test (
                pk int PRIMARY KEY,
                v int
            )"""
RAW_DML_STATEMENTS = ["INSERT INTO ks.test (pk, v) VALUES (:pk, :v) IF NOT EXISTS",
                      "UPDATE ks.test SET v = 1001 WHERE pk = :pk IF v > -100 AND v < 100",
                      "DELETE FROM ks.test WHERE pk = :pk IF EXISTS"]
# Replica-side LWT/paxos write timeouts under this load are expected.
MUTATION_WRITE_TIMEOUT = r"exception during mutation write to .*: exceptions::mutation_write_timeout_exception"


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


def connect(manager: ScyllaClusterManager, server, user=None, password=None):
    """The dtests' patient_cql_connection(): a session through the server,
    with statements defaulting to CL=ONE."""
    auth = PlainTextAuthProvider(username=user, password=password) if user else None
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl, auth)
    session = cluster.connect()
    session.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return session


def describe_rf(rf) -> str:
    """The dtests' describe_rf(): a replication factor (a number, or a list
    of racks) as a REPLICATION option value."""
    if type(rf) is list:
        return "[" + ", ".join(f"'{rack}'" for rack in rf) + "]"
    return f"'{rf}'"


def decrease_rf(rf):
    """The dtests' decrease_rf(): one replica fewer (a rack list loses its
    last rack)."""
    if type(rf) is list:
        return rf[:-1]
    num_rf = int(rf)
    if num_rf == 0:
        raise Exception("Cannot decrease RF which is already 0")
    return str(num_rf - 1)


def get_replication_options(session, ks: str) -> dict:
    """The dtests' get_replication_options() of a vnodes keyspace: its
    replication options as stored, e.g. {"datacenter1": "3"}."""
    row = session.execute(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'").one()
    return dict(row.replication)


def create_ks(session, name: str, rf: int | dict) -> None:
    """The dtests' create_ks(): RF as a number or per DC, retried on
    timeouts, an existing keyspace is fine; then waits for schema agreement
    and uses the keyspace."""
    if isinstance(rf, int):
        options = f"'class':'NetworkTopologyStrategy', 'replication_factor':{rf}"
    else:
        options = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':{describe_rf(dc_rf)}" for dc, dc_rf in rf.items())
    query = f"CREATE KEYSPACE {name} WITH replication={{{options}}}"
    deadline = time.time() + 120
    while True:
        try:
            session.execute(query)
            break
        except OperationTimedOut:
            if time.time() > deadline:
                raise
        except AlreadyExists:
            logger.warning("AlreadyExists executing create ks query %s", query)
            break
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute(f"USE {name}")


async def prepare(manager: ScyllaClusterManager, setup_auth: bool = False, user=None, password=None, ks_dc_mapping=None):
    """The dtest's prepare(): the cluster, and a session through the first
    node using keyspace ks (RF 3, or the RF per DC of ks_dc_mapping)."""
    servers = await manager.servers_add(3, config=AUTH_CONFIG if setup_auth else CONFIG, property_file=TOPOLOGY)
    session = connect(manager, servers[0], user, password)
    create_ks(session, "ks", 3 if ks_dc_mapping is None else ks_dc_mapping)
    return servers, session


def prepare_lwt_statements(session):
    """The dtest's _prepare_lwt_statements(): the LWT statements, plain and
    in single-statement conditional batches, at serial consistency SERIAL."""
    dml_statements = [session.prepare(raw_stmt) for raw_stmt in RAW_DML_STATEMENTS]
    dml_statements.extend([session.prepare(f"""
            BEGIN BATCH
            {raw_stmt}
            APPLY BATCH
            """) for raw_stmt in RAW_DML_STATEMENTS])
    for stmt in dml_statements:
        stmt.serial_consistency_level = ConsistencyLevel.SERIAL
    return dml_statements


def lwt_load_run(manager, server, need_to_stop: threading.Event, tolerate_unavailable=False, user=None, password=None):
    """Random LWT statements until stopped; returns how many succeeded.
    Driver errors are expected."""
    thread_name = threading.current_thread().name
    # No "USE ks": the keyspace may be dropped by the DDL threads at any time.
    session = connect(manager, server, user, password)
    try:
        # Statements are prepared lazily and retried: prepare fails while the
        # DDL threads have the table or column dropped/renamed.
        dml_statements = None
        successes = 0
        logger.info('Producing LWT load on the cluster (thread "%s")', thread_name)
        while not need_to_stop.is_set():
            try:
                if dml_statements is None:
                    dml_statements = prepare_lwt_statements(session)
                session.execute(random.choice(dml_statements), {"pk": random.randint(0, 10000), "v": random.randint(-1000, 1000)})
                successes += 1
            except Unavailable as exc:
                logger.info('Failed to execute LWT statement (thread "%s"). Unavailable error: %s', thread_name, exc)
                if not tolerate_unavailable:
                    raise
            except (DriverException, NoHostAvailable) as exc:
                logger.info('Failed to execute LWT statement (thread "%s"). Driver error: %s', thread_name, exc)
                # Don't spin while prepare keeps failing (table/column dropped).
                time.sleep(0.05)
        logger.info('Finished LWT stress workload (thread "%s"): %d successful statements', thread_name, successes)
        return successes
    finally:
        session.cluster.shutdown()


def ddl_run(manager, server, need_to_stop: threading.Event, fn, user=None, password=None):
    """The DDL function in a loop until stopped; returns how many iterations
    succeeded. Driver errors are expected."""
    thread_name = threading.current_thread().name
    # No "USE ks": the keyspace may be dropped by another DDL thread at any time.
    session = connect(manager, server, user, password)
    try:
        successes = 0
        logger.info('Producing destructive DDL load on the cluster (thread "%s")', thread_name)
        while not need_to_stop.is_set():
            try:
                fn(session)
                successes += 1
            except Unavailable:
                raise
            except (DriverException, NoHostAvailable, ConfigurationException) as exc:
                logger.info('Failure during disruption thread operation (thread "%s"). Driver error: %s', thread_name, exc)
            # Without a pause the DDL threads never leave the table intact long enough for LWT to run.
            time.sleep(0.1)
        logger.info('Finished DDL stress workload (thread "%s"): %d successful iterations', thread_name, successes)
        return successes
    finally:
        session.cluster.shutdown()


async def case_template(manager: ScyllaClusterManager, servers, session, ddl_fn, build_mode: str, test_duration_sec=None,
                        tolerate_unavailable=False, ddl_user=None, ddl_pass=None, lwt_user=None, lwt_pass=None):
    """The dtest's _case_template(): four LWT and four DDL threads, one per
    node in turn, for test_duration_sec (30 s, 90 s in debug mode); then a
    worker thread's error fails the test, every node must be alive, and
    both the LWT statements and the DDL must have succeeded at times."""
    session.execute(session.prepare(CREATE_TEST_TABLE))
    if lwt_user:
        session.execute(f"GRANT ALL ON TABLE ks.test TO {lwt_user}")

    if test_duration_sec is None:
        # Debug is several times slower; keep enough window for a few DDL successes.
        test_duration_sec = 90 if build_mode == "debug" else 30

    need_to_stop = threading.Event()
    lwt_load_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lwt_wrk_thr")
    ddl_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ddl_thr")
    lwt_futures = []
    ddl_futures = []
    for i in range(4):
        server = servers[i % len(servers)]
        lwt_futures.append(lwt_load_executor.submit(lwt_load_run, manager, server, need_to_stop,
                                                    tolerate_unavailable, lwt_user, lwt_pass))
        ddl_futures.append(ddl_executor.submit(ddl_run, manager, server, need_to_stop, ddl_fn, ddl_user, ddl_pass))

    await asyncio.sleep(float(test_duration_sec))
    need_to_stop.set()
    await asyncio.to_thread(lwt_load_executor.shutdown)
    await asyncio.to_thread(ddl_executor.shutdown)

    # result() re-raises whatever killed a worker thread, so none dies silently.
    lwt_successes = [f.result() for f in lwt_futures]
    ddl_successes = [f.result() for f in ddl_futures]
    logger.info("LWT successes per thread: %s, DDL successes per thread: %s", lwt_successes, ddl_successes)

    for s in servers:
        assert await manager.server_is_alive(s.server_id)

    assert sum(lwt_successes) > 0, "LWT load never succeeded, so it did not really race the DDL"
    assert sum(ddl_successes) > 0, "DDL never succeeded, so it did not really race the LWT load"


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_drop_table.
# Scylla doesn't crash under LWT load while the table is dropped and created
# again.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_drop_table(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += ["Can't find a column family", "exception during mutation write",
                                    "mutation_write_failure_exception", "prune: failed", MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager)
    create_test_table_stmt = session.prepare(CREATE_TEST_TABLE)
    drop_test_table_stmt = session.prepare("DROP TABLE IF EXISTS ks.test")

    def drop_create_table_fn(session):
        """DROP + CREATE TABLE"""
        session.execute(drop_test_table_stmt)
        session.execute(create_test_table_stmt)

    try:
        await case_template(manager, servers, session, drop_create_table_fn, build_mode)
    finally:
        session.cluster.shutdown()


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_drop_keyspace.
# Scylla doesn't crash under LWT load while the keyspace is dropped and
# created again with the table.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_drop_keyspace(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += ["Can't find a column family", "exception during mutation write", "Can't find a keyspace",
                                    "Column family test exists", "mutation_write_failure_exception", "prune: failed",
                                    MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager)
    create_test_table_stmt = session.prepare(CREATE_TEST_TABLE)
    drop_test_ks_stmt = session.prepare("DROP KEYSPACE IF EXISTS ks")

    def drop_create_ks_fn(session):
        """DROP + CREATE KEYSPACE/TABLE"""
        session.execute(drop_test_ks_stmt)
        # No create_ks(): it ends with USE ks, and another DDL thread may drop ks first.
        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
        session.execute(create_test_table_stmt)

    try:
        await case_template(manager, servers, session, drop_create_ks_fn, build_mode)
    finally:
        session.cluster.shutdown()


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_rename_column.
# Scylla doesn't crash under LWT load while the partition key column the LWT
# statements use is renamed and renamed back (only primary key columns can
# be renamed).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_rename_column(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += [MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager)
    alter_column_stmt = session.prepare("ALTER TABLE ks.test RENAME pk TO pk1")
    revert_alter_column_stmt = session.prepare("ALTER TABLE ks.test RENAME pk1 TO pk")

    def rename_column_fn(session):
        """RENAME COLUMN TO"""
        session.execute(alter_column_stmt)
        session.execute(revert_alter_column_stmt)

    try:
        await case_template(manager, servers, session, rename_column_fn, build_mode)
    finally:
        session.cluster.shutdown()


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_drop_column.
# Scylla doesn't crash under LWT load while the column the LWT statements
# use is dropped and added again.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_drop_column(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += [MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager)
    alter_column_stmt = session.prepare("ALTER TABLE ks.test DROP v")
    revert_alter_column_stmt = session.prepare("ALTER TABLE ks.test ADD v int")

    def drop_create_column_fn(session):
        """DROP + CREATE COLUMN"""
        session.execute(alter_column_stmt)
        session.execute(revert_alter_column_stmt)

    try:
        await case_template(manager, servers, session, drop_create_column_fn, build_mode)
    finally:
        session.cluster.shutdown()


# Ported from scylla-dtest lwt_destructive_ddl_test.py::TestLwtDestructiveDDL::test_ks_netw_topology_change_rf.
# Scylla doesn't crash under LWT load while ALTER KEYSPACE lowers the RF of
# datacenter1 from 3 to 2 and restores it, in the RF form the keyspace is
# stored in. Unavailable errors of the LWT statements are tolerated.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.check_nodes_for_errors
async def test_ks_netw_topology_change_rf(manager: ScyllaClusterManager, build_mode: str):
    manager.ignore_log_patterns += [MUTATION_WRITE_TIMEOUT]
    servers, session = await prepare(manager, ks_dc_mapping={"datacenter1": 3})
    # Follow the stored form: a rack-list keyspace rejects numeric RF changes, and vice versa.
    rf = get_replication_options(session, "ks")["datacenter1"]
    lower_rf_stmt = session.prepare(f"ALTER KEYSPACE ks WITH replication={{'class': 'NetworkTopologyStrategy', 'datacenter1': {describe_rf(decrease_rf(rf))}}}")
    restore_rf_stmt = session.prepare(f"ALTER KEYSPACE ks WITH replication={{'class': 'NetworkTopologyStrategy', 'datacenter1': {describe_rf(rf)}}}")

    def change_rf_fn(session):
        """ALTER KEYSPACE changing the replica set, then restoring it"""
        session.execute(lower_rf_stmt)
        session.execute(restore_rf_stmt)

    try:
        await case_template(manager, servers, session, change_rf_fn, build_mode, tolerate_unavailable=True)
    finally:
        session.cluster.shutdown()
