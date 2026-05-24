#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import json
import logging
import pytest
import random
import time
from enum import Enum
from typing import TypeAlias, Any

from cassandra.cluster import ConsistencyLevel, Session  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.pool import Host  # type: ignore

from test.pylib.util import wait_for_cql_and_get_hosts, execute_with_tracing
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, new_test_table


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
async def test_incremental_read_repair(data_class: DataClass, manager: ManagerClient):
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

            await data_class.write_data(manager.get_cql(), ks, total_rows, node_live_rows, all_live_rows)

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


class ReadRepairPhase(Enum):
    READ = 1
    WRITE = 2

class QueryKind(Enum):
    FORWARD = "forward"
    REVERSED = "reversed"
    RANGE = "range"

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize(
    "failed_phase",
    [
        pytest.param(ReadRepairPhase.READ),
        pytest.param(ReadRepairPhase.WRITE),
    ]
)
@pytest.mark.parametrize(
    "query_kind",
    [
        pytest.param(QueryKind.FORWARD, id="forward"),
        pytest.param(QueryKind.REVERSED, id="reversed"),
        pytest.param(QueryKind.RANGE, id="range"),
    ]
)
async def test_read_repair_failure(manager: ManagerClient, failed_phase: ReadRepairPhase, query_kind: QueryKind):
    cmdline = ["--hinted-handoff-enabled", "0", "--logger-log-level", "storage_proxy=trace"]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk bigint, ck bigint, c int, PRIMARY KEY (pk, ck)", " WITH speculative_retry = 'NONE'") as cf:

            insert_stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, c) VALUES (?, ?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.ONE

            # Block writes on servers[0] so it becomes stale. servers[1] and servers[2]
            # receive all writes and have up-to-date data.
            cf_name = cf.split(".")[1]
            await manager.api.enable_injection(servers[0].ip_addr, "database_apply", one_shot=False, parameters={"ks_name": ks, "cf_name": cf_name, "what": "throw"})
            await asyncio.gather(*[cql.run_async(insert_stmt, (0, ck, ck)) for ck in range(0, 100)])
            await manager.api.disable_injection(servers[0].ip_addr, "database_apply")

            # Fail servers[1] (a fresh replica) during the read or write phase.
            # servers[0] (stale) remains healthy and always participates in reads
            # and receives repair writes.
            match failed_phase:
                case ReadRepairPhase.READ:
                    await manager.api.enable_injection(servers[1].ip_addr, "fail_mutation_query", one_shot=False, parameters={"ks_name": ks, "cf_name": cf.split(".")[1]})
                case ReadRepairPhase.WRITE:
                    await manager.api.enable_injection(servers[1].ip_addr, "database_apply", one_shot=False, parameters={"ks_name": ks, "cf_name": cf.split(".")[1], "what": "throw"})

            # Force servers[0] (stale) into every QUORUM read's target set.
            # With RF=3 and 3 nodes, every node is a replica, but CL=QUORUM
            # only selects 2. This injection ensures servers[0] is always
            # selected, guaranteeing digest mismatch and read-repair on every read.
            server0_host_id = await manager.get_host_id(servers[0].server_id)
            for srv in servers:
                await manager.api.enable_injection(srv.ip_addr, "force_read_target", one_shot=False, parameters={"host_id": server0_host_id})

            # Open log files on all servers.
            logs = []
            for srv in servers:
                log = await manager.server_open_log(srv.server_id)
                logs.append(log)

            # Read all 100 rows with CL=QUORUM. Each read includes servers[0] (stale),
            # triggering read-repair. servers[1] may fail during read or write phase,
            # but the read still succeeds and servers[0] gets repaired.
            # After each read, verify that reconciliation was triggered.
            #
            # Build query list: range scan issues a single full-table query;
            # forward/reversed issue per-row point queries.
            match query_kind:
                case QueryKind.RANGE:
                    queries = [(f"SELECT * FROM {cf}", None)]
                case QueryKind.REVERSED:
                    queries = [(f"SELECT * FROM {cf} WHERE pk = 0 AND ck = {ck} ORDER BY ck DESC", ck) for ck in range(100)]
                case QueryKind.FORWARD:
                    queries = [(f"SELECT * FROM {cf} WHERE pk = 0 AND ck = {ck}", ck) for ck in range(100)]

            for query, ck_filter in queries:
                marks = [await log.mark() for log in logs]

                resp = await cql.run_async(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM))
                if ck_filter is not None:
                    assert resp
                    assert resp[0] == (0, ck_filter, ck_filter)
                else:
                    assert len(resp) == 100, f"Expected 100 rows, got {len(resp)}"
                    for row in resp:
                        assert row == (0, row.ck, row.ck), f"Unexpected row: {row}"

                reconciled = False
                for log, mark in zip(logs, marks):
                    if await log.grep("read-repair reconcile:", from_mark=mark):
                        reconciled = True
                        break
                label = f"ck={ck_filter}" if ck_filter is not None else "range scan"
                assert reconciled, f"No reconciliation triggered for {label}"

            # Verify servers[0] has all rows via MUTATION_FRAGMENTS.
            await manager.api.keyspace_flush(servers[0].ip_addr, ks)
            rows = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.{cf_name})", host=hosts[0])
            live_cks = {row.ck for row in rows if row.partition_region == 2}
            expected_cks = set(range(100))
            assert live_cks == expected_cks, \
                f"Expected all 100 rows repaired on servers[0], got {len(live_cks)}: missing {expected_cks - live_cks}"


class TombstoneKind(Enum):
    ROW = "row"
    PARTITION = "partition"
    CELL = "cell"
    RANGE = "range"


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize(
    "tombstone_kind",
    [
        pytest.param(TombstoneKind.ROW, id="row"),
        pytest.param(TombstoneKind.PARTITION, id="partition"),
        pytest.param(TombstoneKind.CELL, id="cell"),
        pytest.param(TombstoneKind.RANGE, id="range"),
    ]
)
async def test_read_repair_tombstone(manager: ManagerClient, tombstone_kind: TombstoneKind):
    """Test that read-repair correctly propagates tombstones to stale replicas.

    Setup: 3 nodes, RF=3. Insert 100 rows (pk=0 ck=0..49, pk=1 ck=0..49) with
    CL=ALL so all nodes have the data. Then block writes on servers[0] and execute
    deletes on the remaining nodes. servers[0] misses the deletes.

    Read with CL=QUORUM triggers read-repair which should propagate the tombstones
    to servers[0]. Verify via MUTATION_FRAGMENTS that the tombstones landed.
    """
    NUM_CKS = 50
    DELETED_CKS = set(range(25))  # ck 0-24 deleted, ck 25-49 alive

    cmdline = ["--hinted-handoff-enabled", "0"]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};"
    async with new_test_keyspace(manager, ks_opts) as ks:
        table_opts = " WITH speculative_retry = 'NONE'"
        async with new_test_table(manager, ks, "pk bigint, ck bigint, c int, PRIMARY KEY (pk, ck)", table_opts) as cf:

            # Step 1: Insert all rows with CL=ALL so every node has the data.
            insert_stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, c) VALUES (?, ?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.ALL
            await asyncio.gather(*[
                cql.run_async(insert_stmt, (pk, ck, ck))
                for pk in (0, 1) for ck in range(NUM_CKS)
            ])

            # Step 2: Block writes on servers[0] and execute deletes.
            # servers[0] misses these deletes and retains the original rows.
            cf_name = cf.split(".")[1]
            await manager.api.enable_injection(servers[0].ip_addr, "database_apply", one_shot=False,
                                               parameters={"ks_name": ks, "cf_name": cf_name, "what": "throw"})

            match tombstone_kind:
                case TombstoneKind.ROW:
                    delete_stmt = cql.prepare(f"DELETE FROM {cf} WHERE pk = ? AND ck = ?")
                    delete_stmt.consistency_level = ConsistencyLevel.ONE
                    await asyncio.gather(*[
                        cql.run_async(delete_stmt, (pk, ck))
                        for pk in (0, 1) for ck in DELETED_CKS
                    ])
                case TombstoneKind.PARTITION:
                    # Delete only pk=0 entirely. pk=1 stays intact.
                    await cql.run_async(SimpleStatement(
                        f"DELETE FROM {cf} WHERE pk = 0",
                        consistency_level=ConsistencyLevel.ONE))
                case TombstoneKind.CELL:
                    delete_stmt = cql.prepare(f"DELETE c FROM {cf} WHERE pk = ? AND ck = ?")
                    delete_stmt.consistency_level = ConsistencyLevel.ONE
                    await asyncio.gather(*[
                        cql.run_async(delete_stmt, (pk, ck))
                        for pk in (0, 1) for ck in DELETED_CKS
                    ])
                case TombstoneKind.RANGE:
                    for pk in (0, 1):
                        await cql.run_async(SimpleStatement(
                            f"DELETE FROM {cf} WHERE pk = {pk} AND ck >= 0 AND ck < 25",
                            consistency_level=ConsistencyLevel.ONE))

            await manager.api.disable_injection(servers[0].ip_addr, "database_apply")

            # Step 3: Force servers[0] into every QUORUM read's target set.
            server0_host_id = await manager.get_host_id(servers[0].server_id)
            for srv in servers:
                await manager.api.enable_injection(srv.ip_addr, "force_read_target", one_shot=False,
                                                   parameters={"host_id": server0_host_id})

            # Step 4: Read each row and verify correct results.
            # Read-repair triggers because servers[0] has stale data (no tombstones).
            for pk in (0, 1):
                for ck in range(NUM_CKS):
                    resp = await cql.run_async(SimpleStatement(
                        f"SELECT * FROM {cf} WHERE pk = {pk} AND ck = {ck}",
                        consistency_level=ConsistencyLevel.QUORUM))

                    is_deleted = _is_deleted(tombstone_kind, pk, ck, DELETED_CKS)

                    if tombstone_kind == TombstoneKind.CELL and is_deleted:
                        # Cell tombstone: row exists but column c is null.
                        assert resp, f"pk={pk} ck={ck}: expected row with c=None"
                        assert resp[0] == (pk, ck, None), f"pk={pk} ck={ck}: expected (pk, ck, None), got {resp[0]}"
                    elif is_deleted:
                        # Row/partition/range tombstone: row is gone.
                        assert not resp, f"pk={pk} ck={ck}: expected no row, got {resp}"
                    else:
                        # Not deleted: row is live.
                        assert resp, f"pk={pk} ck={ck}: expected live row"
                        assert resp[0] == (pk, ck, ck), f"pk={pk} ck={ck}: expected (pk, ck, ck), got {resp[0]}"

            # Step 5: Verify tombstones were propagated to servers[0] via MUTATION_FRAGMENTS.
            await manager.api.keyspace_flush(servers[0].ip_addr, ks)
            mf_rows = await cql.run_async(
                f"SELECT * FROM MUTATION_FRAGMENTS({ks}.{cf_name})", host=hosts[0])

            _verify_tombstones(mf_rows, tombstone_kind, NUM_CKS, DELETED_CKS)


def _is_deleted(tombstone_kind: TombstoneKind, pk: int, ck: int, deleted_cks: set[int]) -> bool:
    """Return True if this (pk, ck) should be deleted for the given tombstone kind."""
    if tombstone_kind == TombstoneKind.PARTITION:
        # Only pk=0 is deleted.
        return pk == 0
    return ck in deleted_cks


def _verify_tombstones(mf_rows, tombstone_kind: TombstoneKind, num_cks: int, deleted_cks: set[int]):
    """Verify MUTATION_FRAGMENTS on servers[0] shows tombstones were propagated."""

    match tombstone_kind:
        case TombstoneKind.ROW:
            # For ck in deleted_cks, clustering rows should have a row tombstone.
            for pk in (0, 1):
                pk_rows = [r for r in mf_rows if r.pk == pk and r.partition_region == 2
                           and r.mutation_fragment_kind == "clustering row"]
                tombstoned_cks = set()
                for row in pk_rows:
                    metadata = json.loads(row.metadata)
                    tombstone = metadata.get("tombstone")
                    if tombstone:
                        tombstoned_cks.add(row.ck)
                assert tombstoned_cks >= deleted_cks, \
                    f"pk={pk}: expected row tombstones for {deleted_cks}, got {tombstoned_cks}"

        case TombstoneKind.PARTITION:
            # pk=0 partition_start should have a non-empty tombstone.
            pk0_starts = [r for r in mf_rows if r.pk == 0 and r.partition_region == 0]
            assert pk0_starts, "pk=0: no partition_start found"
            for ps in pk0_starts:
                metadata = json.loads(ps.metadata)
                assert metadata.get("tombstone"), \
                    f"pk=0: expected partition tombstone, got {metadata}"
            # pk=1 should NOT have a partition tombstone.
            pk1_starts = [r for r in mf_rows if r.pk == 1 and r.partition_region == 0]
            assert pk1_starts, "pk=1: no partition_start found"
            for ps in pk1_starts:
                metadata = json.loads(ps.metadata)
                assert not metadata.get("tombstone"), \
                    f"pk=1: unexpected partition tombstone: {metadata}"

        case TombstoneKind.CELL:
            # For ck in deleted_cks, column 'c' should have is_live: false.
            for pk in (0, 1):
                pk_rows = [r for r in mf_rows if r.pk == pk and r.partition_region == 2
                           and r.mutation_fragment_kind == "clustering row"]
                dead_cell_cks = set()
                for row in pk_rows:
                    metadata = json.loads(row.metadata)
                    columns = metadata.get("columns", {})
                    c_meta = columns.get("c", {})
                    if c_meta and not c_meta.get("is_live", True):
                        dead_cell_cks.add(row.ck)
                assert dead_cell_cks >= deleted_cks, \
                    f"pk={pk}: expected cell tombstones for {deleted_cks}, got {dead_cell_cks}"

        case TombstoneKind.RANGE:
            # For each pk, there should be range_tombstone_change fragments.
            for pk in (0, 1):
                rtc_rows = [r for r in mf_rows if r.pk == pk and r.partition_region == 2
                            and r.mutation_fragment_kind == "range tombstone change"]
                assert rtc_rows, \
                    f"pk={pk}: expected range tombstone change fragments, found none"
                # At minimum we expect one pair of range tombstone changes.
                assert len(rtc_rows) >= 2, \
                    f"pk={pk}: expected at least 2 range tombstone change fragments, got {len(rtc_rows)}"
