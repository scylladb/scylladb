#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tracing while nodes shut down and start, and slow query tracing. Ported
# from scylla-dtest cql_tracing_test.py, one test per dtest. As there, the
# clusters are ccm's populate(generate_cluster_topology(...)) (one rack per
# node in datacenter1), keyspaces use vnodes, and the nodes are stopped
# gracefully at the end of each test.

import asyncio
import logging
import random
import re

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # type: ignore
from cassandra.concurrent import execute_concurrent  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import cassandra_stress, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {"tablets_mode_for_new_keyspaces": "disabled"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def populate_racks(manager: ScyllaClusterManager, racks: int, config=CONFIG):
    """ccm's populate(generate_cluster_topology(rack_num=racks)).start()."""
    return await manager.servers_add(racks, config=config,
                                     property_file=[{"dc": "datacenter1", "rack": f"rack{i + 1}"} for i in range(racks)])


def connect(manager: ScyllaClusterManager, server):
    """The dtests' patient_cql_connection(node): statements default to
    consistency level ONE."""
    cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


def create_ks(cql, name: str, rf: int) -> None:
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.execute(f"USE {name}")


def create_cf(cql, name: str) -> None:
    """The dtests' create_cf(session, name, read_repair=0.0, columns={"c1": "text", "c2": "text"})."""
    cql.execute(f"CREATE COLUMNFAMILY {name} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                "AND read_repair_chance=0.000000")
    cql.execute(f"SELECT * FROM {name} LIMIT 1")


def insert_c1c2_no_prepared(cql, keys, consistency) -> None:
    execute_concurrent(cql, [(SimpleStatement(f"INSERT INTO ks.cf (key, c1, c2) VALUES ('k{k}', 'value1', 'value2')",
                                              consistency_level=consistency), None) for k in keys])


def insert_swallowing_errors(cql, keys) -> None:
    """The dtests' insert thread: errors (from a node going down) end it."""
    try:
        insert_c1c2_no_prepared(cql, keys, ConsistencyLevel.ONE)
        logger.debug(f"insertion of {len(keys)} keys is done")
    except Exception:
        logger.debug("insertions were killed")


async def assert_no_assertion_failure(manager: ScyllaClusterManager, server) -> None:
    log = await manager.server_open_log(server.server_id)
    match = await log.grep("Assertion .* failed.")
    assert len(match) == 0, f"Found assertion failure: {match}"


# Ported from scylla-dtest cql_tracing_test.py::TestCqlTracing::test_tracing_shutdown.
# With tracing of all CQL requests on, a node shut down stops handling CQL
# before it stops tracing (an assertion used to fail otherwise), and pushes
# the trace sessions of all the requests it handled before; then the other
# node shuts down while the first one keeps sending it traced writes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_shutdown(manager: ScyllaClusterManager, build_mode):
    # As on the dtest's nodes (ccm keeps Scylla's default), a node waits 2 s
    # after announcing its shutdown; test.py's 0 lets node1 go down before
    # its last trace sessions (written every 2 s) reach node2.
    node1, node2 = await populate_racks(manager, 2, config={**CONFIG, "shutdown_announce_in_ms": 2000})
    for s in (node1, node2):
        await manager.api.set_trace_probability(s.ip_addr, 1.0)

    cql = connect(manager, node1)
    create_ks(cql, "ks", 2)
    create_cf(cql, "cf")

    num_keys = 500
    insert_c1c2_no_prepared(cql, range(num_keys), ConsistencyLevel.ONE)
    await run_nodetool(manager, node1, "flush")

    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_not_sees_other_server(node2.ip_addr, node1.ip_addr)
    await assert_no_assertion_failure(manager, node1)

    # All the tracing sessions were flushed.
    pattern = re.compile("INSERT INTO")
    rows = list(cql.execute(SimpleStatement("SELECT parameters FROM system_traces.sessions")))
    count = sum(1 for row in rows if pattern.search(row[0]["query"].strip()))
    assert count == num_keys, "Not all tracing session have been flushed"
    cql.shutdown()

    await manager.server_start(node1.server_id)
    await manager.api.set_trace_probability(node1.ip_addr, 1.0)
    cql = connect(manager, node1)
    try:
        additional_keys = 2 * num_keys if build_mode == "debug" else 30 * num_keys
        insert = asyncio.create_task(asyncio.to_thread(insert_swallowing_errors, cql,
                                                       range(num_keys, num_keys + additional_keys)))
        await asyncio.sleep(random.random())
        await manager.server_stop_gracefully(node2.server_id)
        await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)
        await insert
    finally:
        cql.shutdown()


# Ported from scylla-dtest cql_tracing_test.py::TestCqlTracing::test_tracing_startup.
# A node started while its peer keeps sending it traced writes does not
# handle them before its tracing service is up (an assertion used to fail).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_tracing_startup(manager: ScyllaClusterManager):
    node1, node2 = await populate_racks(manager, 2)
    await manager.api.set_trace_probability(node1.ip_addr, 1.0)

    cql = connect(manager, node1)
    try:
        create_ks(cql, "ks", 2)
        create_cf(cql, "cf")

        await manager.server_stop_gracefully(node2.server_id)
        await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)
        await assert_no_assertion_failure(manager, node2)

        insert = asyncio.create_task(asyncio.to_thread(insert_swallowing_errors, cql, range(15000)))
        await asyncio.sleep(random.random())
        await manager.server_start(node2.server_id)
        await insert
    finally:
        cql.shutdown()


def assert_cs_success(output: str) -> None:
    """The dtests' assert_cs_success(): cassandra-stress's output, without its
    log lines, ends with END or DONE."""
    filtered = "\n".join(line for line in output.splitlines() if not line.startswith(("INFO  [", "WARN  [", "ERROR  [")))
    assert filtered.strip().endswith(("END", "DONE")), f"Run c-s failed: {output}"


# Ported from scylla-dtest cql_tracing_test.py::TestSlowQueryTracing::test_fast_slow_query_tracing.
# Slow query tracing, enabled through the REST API with a 500 us threshold,
# records the slow queries of a cassandra-stress write load in
# system_traces.node_slow_log and sessions; in fast mode without their
# events, otherwise with them.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("fast", [True, False], ids=["enabled", "disabled"])
async def test_fast_slow_query_tracing(manager: ScyllaClusterManager, fast: bool):
    [node1] = await populate_racks(manager, 1)
    cql = connect(manager, node1)
    try:
        cql.execute("DROP KEYSPACE IF EXISTS ks")
        create_ks(cql, "ks", 1)

        threshold = 500
        await manager.api.client.post("/storage_service/slow_query", host=node1.ip_addr,
                                      params={"fast": str(fast).lower(), "enable": "true", "threshold": str(threshold)})
        status = await manager.api.client.get_json("/storage_service/slow_query", host=node1.ip_addr)
        if fast:
            assert "fast" in status, "Fast slow query tracing is not supported"
        if "fast" in status:
            assert status["fast"] == fast, f"Fast slow query tracing is {not fast}"
        assert status["enable"], "Slow query tracing is not enabled"
        assert status["threshold"] == threshold, "Slow query tracing is not enabled"

        out = await cassandra_stress(manager, node1, "write", "cl=ONE", "n=10000", "-schema", "replication(factor=1)",
                                     "-rate", "threads=10", create_keyspace=False)
        assert_cs_success(out)

        def count(table):
            return cql.execute(SimpleStatement(f"SELECT count(*) FROM {table}", consistency_level=ConsistencyLevel.ONE),
                               timeout=120).one()[0]
        if fast:
            assert list(cql.execute("select * from system_traces.events")) == []
        else:
            assert count("system_traces.events") > 0
        assert count("system_traces.node_slow_log") > 0
        assert count("system_traces.sessions") > 0
    finally:
        cql.shutdown()
