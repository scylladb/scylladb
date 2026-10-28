#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import logging
import os
import pytest
import random
import time
from typing import TypeAlias, Any

from cassandra.cluster import ConsistencyLevel, Session  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.pool import Host  # type: ignore

from test.pylib.util import wait_for_cql_and_get_hosts, execute_with_tracing
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace, run_nodetool


logger = logging.getLogger(__name__)


class DataClass:
    @classmethod
    def get_column_spec(self) -> str:
        raise NotImplementedError()

    @classmethod
    def get_unique_key(self) -> str:
        raise NotImplementedError()

    @classmethod
    def get_select_query(self, ks) -> str:
        raise NotImplementedError()

    @classmethod
    async def write_data(self, cql, ks: str, total_rows: int, node_live_rows: set[int], all_live_rows: set[int]) -> None:
        raise NotImplementedError()

    @classmethod
    def check_mutation_row(self, row, expected_live_rows: set[int]) -> tuple | None:
        raise NotImplementedError()

    @classmethod
    def check_page_count(self, page_count) -> None:
        raise NotImplementedError()

    @classmethod
    def check_result_row(self, i: int, row) -> None:
        raise NotImplementedError()

class row_tombstone_data(DataClass):
    pk = 0
    v = 1

    @classmethod
    def get_column_spec(self) -> str:
        return "pk int, ck int, v int, PRIMARY KEY (pk, ck)"

    @classmethod
    def get_unique_key(self) -> str:
        return 'ck'

    @classmethod
    def get_select_query(self, ks) -> str:
        return f"SELECT * FROM {ks}.tbl WHERE pk = {self.pk}"

    @classmethod
    async def write_data(cls, cql, ks, total_rows: int, live_rows: set[int], all_live_rows: set[int]) -> None:
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (?, ?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

        delete_stmt = cql.prepare(f"DELETE FROM {ks}.tbl WHERE pk = ? AND ck = ?")
        delete_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

        for ck in range(total_rows):
            if ck in live_rows:
                await cql.run_async(insert_stmt, (cls.pk, ck, cls.v))
            elif ck not in all_live_rows:
                await cql.run_async(delete_stmt, (cls.pk, ck))

    @classmethod
    def check_mutation_row(cls, row, expected_live_rows: set[int]) -> tuple | None:
        assert row.pk == cls.pk
        if row.partition_region != 2:
            return None
        metadata = json.loads(row.metadata)
        tombstone = metadata.get("tombstone")
        marker = metadata.get("marker")
        if tombstone is None or (marker is not None and tombstone["timestamp"] < marker["timestamp"]):
            is_live = True
            cols = json.loads(row.value)
            assert cols["v"] == str(cls.v)
        else:
            is_live = False
        return row.ck, is_live

    @classmethod
    def check_page_count(cls, page_count) -> None:
        assert page_count > 1

    @classmethod
    def check_result_row(cls, i: int, row) -> None:
        assert row.pk == cls.pk
        assert row.ck == i
        assert row.v == cls.v


class partition_tombstone_data(DataClass):
    v = 1

    @classmethod
    def get_column_spec(self) -> str:
        return "pk int PRIMARY KEY, v int"

    @classmethod
    def get_unique_key(self) -> str:
        return 'pk'

    partition_tombstone_timestamp = None
    partition_live = False

    @classmethod
    def get_select_query(self, ks):
        return f"SELECT * FROM {ks}.tbl"

    @classmethod
    async def write_data(cls, cql, ks, total_rows: int, live_rows: set[int], all_live_rows: set[int]) -> None:
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.tbl (pk, v) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

        delete_stmt = cql.prepare(f"DELETE FROM {ks}.tbl WHERE pk = ?")
        delete_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

        for pk in range(total_rows):
            if pk in live_rows:
                await cql.run_async(insert_stmt, (pk, cls.v))
            elif pk not in all_live_rows:
                await cql.run_async(delete_stmt, (pk,))

    @classmethod
    def check_mutation_row(cls, row, expected_live_rows: set[int]) -> tuple | None:
        if row.partition_region == 0:
            cls.partition_tombstone_timestamp = json.loads(row.metadata)["tombstone"].get("timestamp")
            return None
        elif row.partition_region == 3:
            is_live = cls.partition_live
            cls.partition_tombstone_timestamp = None
            cls.partition_live = False
            return row.pk, is_live
        elif row.partition_region != 2:
            return None

        metadata = json.loads(row.metadata)
        try:
            v = metadata["columns"]["v"]
            cls.partition_live = v["is_live"]
            assert cls.partition_tombstone_timestamp is None or v["timestamp"] > cls.partition_tombstone_timestamp
            cols = json.loads(row.value)
            assert cols["v"] == str(cls.v)
        except KeyError:
            cls.partition_live = False
        return None

    @classmethod
    def check_page_count(cls, page_count) -> None:
        # We cannot reliably generate partitions such that they trigger short pages
        # So we allow for a single page too.
        pass

    @classmethod
    def check_result_row(cls, i: int, row) -> None:
        assert row.pk == i
        assert row.v == cls.v


incremental_repair_test_data = [pytest.param(row_tombstone_data, id="row-tombstone"),
                                pytest.param(partition_tombstone_data, id="partition-tombstone")]


@pytest.mark.parametrize("data_class", incremental_repair_test_data)
async def test_incremental_read_repair(data_class: DataClass, manager: ScyllaClusterManager):
    """Stress the incremental read repair logic

    Write a long stream of row tombstones, with a live row before and after.
    """
    seed = int(time.time())
    logger.info(f"random-seed: {seed}")
    random.seed(seed)
    cmdline = ["--hinted-handoff-enabled", "0",
               "--cache-hit-rate-read-balancing", "0",
               "--query-tombstone-page-limit", "10",
               "--query-page-size-in-bytes", "1024"]
    nodes = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")
    node1, node2 = nodes

    cql = manager.get_cql()

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    # The test generates and uploads sstables, assuming their specific
    # contents. These assumptions are not held with tablets, which
    # distribute data among sstables differently than vnodes.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = { 'enabled': true }") as ks:
        table_schema = f"CREATE TABLE {ks}.tbl ({data_class.get_column_spec()}) WITH speculative_retry = 'NONE'"
        cql.execute(table_schema)

        total_rows = 100
        max_live_rows = 8

        row_set: TypeAlias = set[int]

        async def write_data(node: ServerInfo, node_live_rows: set[int], all_live_rows: set[int]) -> row_set:
            other_nodes = [n for n in nodes if n != node]

            for other_node in other_nodes:
                await manager.server_stop_gracefully(other_node.server_id)

            await manager.driver_connect(node)
            cql, _ = await manager.get_ready_cql([node])

            await data_class.write_data(cql, ks, total_rows, node_live_rows, all_live_rows)

            for other_node in other_nodes:
                await manager.server_start(other_node.server_id)

        node1_rows = {random.randint(0, total_rows - 1) for _ in range(random.randint(0, max_live_rows))} | {0}
        node2_rows = {random.randint(0, total_rows - 1) for _ in range(random.randint(0, max_live_rows))} | {total_rows - 1}
        all_rows = node1_rows | node2_rows
        assert len(all_rows) >= 2

        await write_data(node1, node1_rows, all_rows)
        await write_data(node2, node2_rows, all_rows)

        logger.info(f"node1_rows: {len(node1_rows)} rows, row ids: {node1_rows}")
        logger.info(f"node2_rows: {len(node2_rows)} rows, row ids: {node2_rows}")
        logger.info(f"all_rows: {len(all_rows)} rows, row ids: {all_rows}")

        def check_rows(cql: Session, host: Host, expected_live_rows: row_set) -> None:
            actual_live_rows = set()
            actual_dead_rows = set()
            for row in cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.tbl)", host=host):
                res = data_class.check_mutation_row(row, expected_live_rows)
                if res is None:
                    continue
                row_id, is_live = res
                if is_live:
                    actual_live_rows.add(row_id)
                else:
                    actual_dead_rows.add(row_id)

            # Account rows that have a tombstone but are live only once.
            actual_dead_rows -= actual_live_rows

            assert actual_live_rows == expected_live_rows
            assert len(actual_live_rows) + len(actual_dead_rows) == total_rows - (len(all_rows) - len(actual_live_rows))

        # Need reconnecting after start/stop of nodes
        await manager.driver_connect()
        cql, hosts = await manager.get_ready_cql(nodes)

        logger.info("Check rows before read-repair")
        check_rows(cql, host1, node1_rows)
        check_rows(cql, host2, node2_rows)

        async def read_repair_metrics() -> tuple[int, int]:
            digest_mismatches = 0
            diff_found = 0
            for node in nodes:
                metrics = await manager.metrics.query(node.ip_addr)
                digest_mismatches += metrics.get("scylla_storage_proxy_coordinator_foreground_read_repairs") or 0
                digest_mismatches += metrics.get("scylla_storage_proxy_coordinator_background_read_repairs") or 0
                diff_found += metrics.get("scylla_storage_proxy_coordinator_read_repairs_diff_found_total") or 0
            return int(digest_mismatches), int(diff_found)

        digest_mismatches_before, diff_found_before = await read_repair_metrics()

        logger.info("Run read-repair")
        res = cql.execute(SimpleStatement(data_class.get_select_query(ks), consistency_level=ConsistencyLevel.ALL), trace=True)
        res_rows = []
        pages = []
        while True:
            res_rows.extend(list(res.current_rows))
            pages.append(list(res.current_rows))
            if res.has_more_pages:
                res.fetch_next_page()
            else:
                break

        tracing = res.get_all_query_traces(max_wait_sec_per=900)
        page_events = []
        for trace in tracing:
            events = []
            for event in trace.events:
                events.append(f"{event.source} {event.source_elapsed} {event.description}")
            page_events.append('\n'.join(events))
        logger.info(f"Tracing:\n{'\n\n'.join(page_events)}")

        logger.debug(f"repair: {len(pages)} pages: {pages}")
        data_class.check_page_count(len(pages))
        assert len(res_rows) == len(all_rows)
        actual_row_ids = set()
        for res_row in res_rows:
            row_id = getattr(res_row, data_class.get_unique_key())
            actual_row_ids.add(row_id)
            assert row_id in all_rows
            data_class.check_result_row(row_id, res_row)
        assert actual_row_ids == all_rows

        # The replicas diverged, so the CL=ALL read must have hit a digest
        # mismatch, and the reconciliation must have found actual differences.
        digest_mismatches_after, diff_found_after = await read_repair_metrics()
        assert digest_mismatches_after > digest_mismatches_before
        assert diff_found_after > diff_found_before
        # Reconciliations are triggered by digest mismatches, so no more
        # diff-found events than digest mismatches.
        assert diff_found_after - diff_found_before <= digest_mismatches_after - digest_mismatches_before

        for node in (node1, node2):
            await manager.api.keyspace_flush(node.ip_addr, ks)
            await manager.api.keyspace_compaction(node.ip_addr, ks)

        logger.info("Check rows after read-repair")
        check_rows(cql, host1, all_rows)
        check_rows(cql, host2, all_rows)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_with_trace_logging(request, manager):
    logger.info("Creating a new cluster")
    cmdline = ["--hinted-handoff-enabled", "0", "--logger-log-level", "mutation_data=trace:debug_error_injection=trace"]
    config = {"read_request_timeout_in_ms": 60000}

    [node1, node2] = await manager.servers_add(2, cmdline=cmdline, config=config, auto_rack_dc="dc1")

    cql = manager.get_cql()
    srvs = await manager.running_servers()
    await wait_for_cql_and_get_hosts(cql, srvs, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk bigint, ck bigint, c int, PRIMARY KEY (pk, ck));")

        await cql.run_async(f"INSERT INTO {ks}.t (pk, ck, c) VALUES (0, 0, 0)")

        insert_stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, ck, c) VALUES (?, ?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ONE

        await manager.api.enable_injection(node1.ip_addr, "database_apply", one_shot=False, parameters={"ks_name": ks, "cf_name": "t", "what": "throw"})
        for ck in range(0, 100):
            await cql.run_async(insert_stmt, (0, ck, ck))
        await manager.api.disable_injection(node1.ip_addr, "database_apply")

        tracing = execute_with_tracing(cql, SimpleStatement(f"SELECT * FROM {ks}.t WHERE pk = 0", consistency_level = ConsistencyLevel.ALL), log = True)

        assert len(tracing) == 1 # 1 page

        found_read_repair = False
        for event in tracing[0]:
            found_read_repair |= "digest mismatch, starting read repair" == event.description

        assert found_read_repair


async def read_repair_prepare_cluster(manager: ScyllaClusterManager, dc: str, racks: list[int]) -> list[ServerInfo]:
    """TestReadRepair.prepare_cluster(): racks of the given sizes in dc, no
    hinted handoff, the GossipingPropertyFileSnitch with prefer_local, no
    dynamic snitch; vnodes."""
    config = {"hinted_handoff_enabled": False, "endpoint_snitch": "GossipingPropertyFileSnitch", "dynamic_snitch": False,
              "tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}
    property_files = [{"dc": dc, "rack": f"rack{i + 1}", "prefer_local": "true"} for i, count in enumerate(racks) for _ in range(count)]
    return await manager.servers_add(len(property_files), config=config, property_file=property_files)


# Ported from scylla-dtest read_repair_test.py::TestReadRepair::test_alter_rf_and_run_read_repair.
# A row written at RF=1 (dc1 of rack1 x2 and rack2, its replica flushed) is
# read at ONE through every node; after RF is raised to 2 without repair, a
# read at ALL of one column through a node that is not the original replica
# returns only that column and read-repairs the whole row: every node then
# reads all of it at ONE (CASSANDRA-10655).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_read_repair_of_column_subset_after_rf_increase(manager: ScyllaClusterManager):
    dc = "dc1"
    servers = await read_repair_prepare_cluster(manager, dc, [2, 1])
    node1 = servers[0]
    session = manager.get_cql()
    session.execute(f"CREATE KEYSPACE alter_rf_test WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': 1}};")
    session.execute("CREATE TABLE alter_rf_test.t1 (k int PRIMARY KEY, a int, b int);")
    session.execute("INSERT INTO alter_rf_test.t1 (k, a, b) VALUES (1, 1, 1);")
    cl_one_stmt = SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1", consistency_level=ConsistencyLevel.ONE)

    # identify_initial_placement(): the replica per "nodetool getendpoints".
    out, _, _ = await run_nodetool(manager, node1, "getendpoints", "alter_rf_test", "t1", "1")
    address = out.split("\n")[-2]
    initial_replica = next(s for s in servers if s.ip_addr == address)
    non_replicas = [s for s in servers if s.ip_addr != address]
    await run_nodetool(manager, initial_replica, "flush")

    async def check_all_nodes():
        for s in servers:
            node_session = await manager.get_cql_exclusive(s)
            try:
                res = [list(r) for r in node_session.execute(cl_one_stmt)]
            finally:
                node_session.cluster.shutdown()
            assert res == [[1, 1, 1]], res

    await check_all_nodes()
    rf = session.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'alter_rf_test'").one().replication[dc]
    rf = "['rack1', 'rack2']" if rf.startswith("[") else "2"
    session.execute(f"ALTER KEYSPACE alter_rf_test WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{dc}' : {rf} }};")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    read_repair_session = await manager.get_cql_exclusive(non_replicas[0])
    try:
        res = [list(r) for r in read_repair_session.execute(SimpleStatement("SELECT a FROM alter_rf_test.t1 WHERE k=1", consistency_level=ConsistencyLevel.ALL))]
    finally:
        read_repair_session.cluster.shutdown()
    assert res == [[1]], res
    await check_all_nodes()


# Ported from scylla-dtest read_repair_test.py::TestReadRepair::test_range_slice_query_with_tombstones.
# 2499 rows on dc1 (rack1 x2, rack2) with RF 2, flushed, the cluster
# restarted, 999 of them deleted and node1 flushed: a traced LOCAL_QUORUM
# range read of 100 rows through node1 writes nothing (no read repair;
# CASSANDRA-8989, CASSANDRA-9502).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_range_scan_over_consistent_tombstones_does_not_read_repair(manager: ScyllaClusterManager):
    servers = await read_repair_prepare_cluster(manager, "dc1", [2, 1])
    node1 = servers[0]
    session1 = await manager.get_cql_exclusive(node1)
    session1.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}")
    session1.execute("CREATE TABLE ks.cf (key int primary key, value double, txt text);")
    for n in range(1, 2500):
        session1.execute(f"INSERT INTO ks.cf (key, value, txt) VALUES ({n}, {n}, 'foo bar {n} iuhiu iuhiu ihi')")
    session1.cluster.shutdown()
    for s in servers:
        await run_nodetool(manager, s, "flush")
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        await manager.server_start(s.server_id)
    session1 = await manager.get_cql_exclusive(node1)
    try:
        for n in range(1, 1000):
            session1.execute(f"DELETE FROM ks.cf WHERE key = {n}")
        await asyncio.sleep(1)
        await run_nodetool(manager, node1, "flush")
        await asyncio.sleep(1)
        future = session1.execute_async(SimpleStatement("SELECT * FROM ks.cf LIMIT 100", consistency_level=ConsistencyLevel.LOCAL_QUORUM), trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
    finally:
        session1.cluster.shutdown()
    for event in trace.events:
        activity = event.description
        assert "Appending to commitlog" not in activity
        assert "Adding to cf memtable" not in activity
        assert "Acquiring switchLock read lock" not in activity


# The TestReversedQueries* dtests' cluster: generate_cluster_topology(dc_num=1,
# rack_num=2) (datacenter1, rack1 and rack2), vnodes, no hinted handoff,
# and keyspace ks with RF=2.
REVERSED_CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "hinted_handoff_enabled": False}


async def reversed_prepare(manager: ScyllaClusterManager, table_options: str):
    servers = await manager.servers_add(2, config=REVERSED_CONFIG, property_file=[
        {"dc": "datacenter1", "rack": "rack1"}, {"dc": "datacenter1", "rack": "rack2"}])
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    await cql.run_async(f"CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH {table_options}")
    return servers, cql


async def stop_node(manager: ScyllaClusterManager, server: ServerInfo) -> None:
    """ccm's node.stop(wait_other_notice=True)."""
    await manager.server_stop_gracefully(server.server_id)
    await manager.others_not_see_server(server.ip_addr)


async def start_node(manager: ScyllaClusterManager, server: ServerInfo, servers: list[ServerInfo]) -> None:
    """ccm's node.start(wait_for_binary_proto=True, wait_other_notice=True)."""
    await manager.server_start(server.server_id)
    await manager.servers_see_each_other(await manager.running_servers())
    await wait_for_cql_and_get_hosts(manager.get_cql(), await manager.running_servers(), time.time() + 60)


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesReadRepair::test_queries_with_read_repair.
# A reversed (ORDER BY ... DESC) read with CL=ALL reconciles the replicas
# into the right reversed result, and repairs the replica that missed the
# writes: after it, that replica alone returns all the rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reversed_read_repair(manager: ScyllaClusterManager):
    row_count = 100
    servers, cql = await reversed_prepare(manager, "read_repair_chance = 100.0")
    node1, node2 = servers
    await stop_node(manager, node2)
    stmt = cql.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE
    for i in range(row_count):
        await cql.run_async(stmt, (0, i, 2 * i))
    await start_node(manager, node2, servers)

    query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck DESC", consistency_level=ConsistencyLevel.ALL)
    expected_rows = [(i, 2 * i) for i in range(row_count)]
    assert list(await manager.get_cql().run_async(query)) == expected_rows[::-1]

    await stop_node(manager, node1)
    session = await manager.get_cql_exclusive(node2)
    query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 BYPASS CACHE", consistency_level=ConsistencyLevel.ONE)
    assert list(await session.run_async(query, trace=True)) == expected_rows


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesMerging::test_read_from_memtables_and_multiple_sstables.
# A reversed read with CL=ALL merges correctly the data of two replicas
# that each have it spread over three sstables and the memtable, with
# overlapping, differently-valued writes of the same rows: each row has the
# value of its latest write, with the cache and with BYPASS CACHE (on
# different partitions).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reversed_read_merging_memtable_and_sstables(manager: ScyllaClusterManager):
    rng = 1000
    servers, cql = await reversed_prepare(manager, "COMPACTION = {'class': 'NullCompactionStrategy'} AND read_repair_chance = 0.0")
    node1, node2 = servers
    expected_dict = {}
    await stop_node(manager, node2)

    async def load_cks(session, cks, v_offset):
        stmt = session.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
        stmt.consistency_level = ConsistencyLevel.ONE
        for pk in [0, 1]:
            for ck in cks:
                v = 100 * ck + v_offset
                await session.run_async(stmt, (pk, ck, v))
                expected_dict[ck] = v

    for cks, offset in [(range(0, rng, 2), 1), (range(0, rng, 3), 2), (range(0, rng, 5), 3)]:
        await load_cks(cql, list(cks), offset)
        await manager.api.flush_all_keyspaces(node1.ip_addr)

    await start_node(manager, node2, servers)
    await stop_node(manager, node1)
    session = await manager.get_cql_exclusive(node2)
    for cks, offset in [(range(1, rng, 2), 4), (range(1, rng, 3), 5), (range(1, rng, 5), 6)]:
        await load_cks(session, list(cks), offset)
        await manager.api.flush_all_keyspaces(node2.ip_addr)
    await start_node(manager, node1, servers)
    # No flush: kept in the memtables.
    await load_cks(session, list(range(0, rng, 7)), 7)

    expected_rows = [(ck, expected_dict[ck]) for ck in list(range(rng))[::-1] if ck in expected_dict]
    for pk, bypass_cache in [(0, ""), (1, " BYPASS CACHE")]:
        query = SimpleStatement(f"SELECT ck, v FROM ks.t WHERE pk = {pk} ORDER BY ck DESC{bypass_cache}", consistency_level=ConsistencyLevel.ALL)
        assert list(await session.run_async(query, trace=True)) == expected_rows


# Ported from scylla-dtest reversed_queries_test.py::TestReversedQueriesReadRepairBigMutationsSmallChanges::test_queries_with_read_repair.
# A partition of 10000 rows (one deleted in each thousand, flushed on node2
# every thousand rows): with the files of node2's first sstable removed, a
# reversed CL=ALL read returns the right rows in reverse order and repairs
# node2, which then alone returns all the rows.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reversed_read_repair_big_partition_small_difference(manager: ScyllaClusterManager):
    row_count = 10_000
    servers, cql = await reversed_prepare(manager, "compaction = {'class': 'NullCompactionStrategy'} AND read_repair_chance = 100.0")
    node1, node2 = servers
    delete_stmt = cql.prepare("DELETE FROM ks.t WHERE pk = ? AND ck = ?")
    delete_stmt.consistency_level = ConsistencyLevel.ALL
    insert_stmt = cql.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
    insert_stmt.consistency_level = ConsistencyLevel.ALL
    expected_rows = []
    for i in range(1, row_count + 1):
        await cql.run_async(insert_stmt, (0, i, 2 * i))
        expected_rows.append((i, 2 * i))
        if i % 1000 == 0:
            ck = random.randint(i - 999, i)
            await cql.run_async(delete_stmt, (0, ck))
            await manager.api.flush_all_keyspaces(node2.ip_addr)
            expected_rows.remove((ck, 2 * ck))

    await stop_node(manager, node2)
    # Remove the files of one sstable: the first nine files under data/ks, by
    # path (CompressionInfo, Data, Digest, Filter, Index, Scylla, Statistics,
    # Summary and TOC of the oldest sstable).
    root = os.path.join(await manager.server_get_workdir(node2.server_id), "data", "ks")
    data_files = sorted(os.path.join(r, name) for r, _, files in os.walk(root) for name in files)
    for file in data_files[0:9]:
        os.remove(file)
    await start_node(manager, node2, servers)

    query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 ORDER BY ck DESC", consistency_level=ConsistencyLevel.ALL)
    assert list(await manager.get_cql().run_async(query, trace=True)) == expected_rows[::-1]

    await stop_node(manager, node1)
    session = await manager.get_cql_exclusive(node2)
    query = SimpleStatement("SELECT ck, v FROM ks.t WHERE pk = 0 BYPASS CACHE", consistency_level=ConsistencyLevel.ONE)
    assert list(await session.run_async(query, trace=True)) == expected_rows
