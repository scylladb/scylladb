#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import json
import logging
import pytest
import random
import time
from typing import TypeAlias, Any

from cassandra.cluster import ConsistencyLevel, Session  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.pool import Host  # type: ignore

from test.pylib.util import wait_for_cql_and_get_hosts, execute_with_tracing
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace


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
@pytest.mark.asyncio
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


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_read_repair_with_trace_logging(request, manager):
    logger.info("Creating a new cluster")
    cmdline = ["--hinted-handoff-enabled", "0", "--logger-log-level", "mutation_data=trace:debug_error_injection=trace"]
    config = {"read_request_timeout_in_ms": 60000}

    [node1, node2] = await manager.servers_add(2, cmdline=cmdline, config=config, auto_rack_dc="dc1")

    cql = manager.get_cql()
    srvs = await manager.running_servers()
    await wait_for_cql_and_get_hosts(cql, srvs, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk bigint PRIMARY KEY, c int);")

        await cql.run_async(f"INSERT INTO {ks}.t (pk, c) VALUES (0, 0)")

        await manager.api.enable_injection(node1.ip_addr, "database_apply", one_shot=True)
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.t (pk, c) VALUES (0, 1)", consistency_level = ConsistencyLevel.ONE))

        tracing = execute_with_tracing(cql, SimpleStatement(f"SELECT * FROM {ks}.t WHERE pk = 0", consistency_level = ConsistencyLevel.ALL), log = True)

        assert len(tracing) == 1 # 1 page

        found_read_repair = False
        for event in tracing[0]:
            found_read_repair |= "digest mismatch, starting read repair" == event.description

        assert found_read_repair
