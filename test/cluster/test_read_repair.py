#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import json
import logging
import pytest
import random
import time
from typing import TypeAlias, Any

from cassandra import WriteFailure  # type: ignore
from cassandra.cluster import ConsistencyLevel, Session  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.pool import Host  # type: ignore

from test.pylib.util import wait_for_cql_and_get_hosts, execute_with_tracing
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("reversed_query", [True, False], ids=["reversed", "forward"])
@pytest.mark.parametrize("paged", [False, True], ids=["unpaged", "paged"])
@pytest.mark.parametrize("per_partition_limit", [False, True], ids=["limit", "per_partition_limit"])
async def test_read_repair_replica_missing_first_row(manager: ScyllaClusterManager, reversed_query: bool, paged: bool, per_partition_limit: bool):
    """Read repair, when one replica misses the first row in query order

    One replica misses the newest row (reversed query) or the oldest row
    (forward query). A CL=ALL read hits a digest mismatch and reconciles.
    The reconciled result must contain the missing row, the reconciliation
    must succeed without retries, and the replica must be repaired. LIMIT
    and PER PARTITION LIMIT are checked separately by the reconciliation,
    so both are covered.

    Reproduces a bug where read repair of reversed queries applied the
    legacy reverse-order logic to mutations that are already in query order:
    unpaged reads retried the same read until they timed out, without ever
    repairing the replica, and paged reads returned stale rows.
    """
    cmdline = ["--hinted-handoff-enabled", "0", "--cache-hit-rate-read-balancing", "0"]
    config = {"read_request_timeout_in_ms": 10000}
    nodes = await manager.servers_add(2, cmdline=cmdline, config=config, auto_rack_dc="dc1")
    node1, node2 = nodes

    cql = manager.get_cql()
    host1, _ = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'")

        # Written at CL=ALL, so that both replicas have them before node1 starts dropping writes.
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (?, ?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL

        common_cks = list(range(1, 10))
        for ck in common_cks:
            await cql.run_async(insert_stmt, (0, ck, ck))

        # The row node1 misses is the first one in query order. At CL=ALL the write fails once
        # node1 has rejected it, so by then node2 has applied it and node1 has dropped it for sure.
        missing_ck = 10 if reversed_query else 0
        await manager.api.enable_injection(node1.ip_addr, "database_apply", one_shot=False,
                                           parameters={"ks_name": ks, "cf_name": "t", "what": "throw"})
        with pytest.raises(WriteFailure):
            await cql.run_async(insert_stmt, (0, missing_ck, missing_ck))
        await manager.api.disable_injection(node1.ip_addr, "database_apply")

        # A limit of 1 is never paged on the server side, a limit of 2 with a larger page size is.
        limit = 2 if paged else 1
        order = "DESC" if reversed_query else "ASC"
        limit_clause = f"PER PARTITION LIMIT {limit}" if per_partition_limit else f"LIMIT {limit}"
        select = f"SELECT ck FROM {ks}.t WHERE pk = 0 ORDER BY ck {order} {limit_clause}"

        async def sum_metric(name: str) -> int:
            total = 0
            for node in nodes:
                metrics = await manager.metrics.query(node.ip_addr)
                total += metrics.get(name) or 0
            return int(total)

        retries_before = await sum_metric("scylla_storage_proxy_coordinator_read_retries")
        read_repairs_before = await sum_metric("scylla_storage_proxy_coordinator_foreground_read_repairs")

        res = cql.execute(SimpleStatement(select, consistency_level=ConsistencyLevel.ALL, fetch_size=100 if paged else None), host=host1)
        actual_cks = [row.ck for row in res]

        retries_after = await sum_metric("scylla_storage_proxy_coordinator_read_retries")
        read_repairs_after = await sum_metric("scylla_storage_proxy_coordinator_foreground_read_repairs")

        expected_cks = sorted(common_cks + [missing_ck], reverse=reversed_query)[:limit]
        assert actual_cks == expected_cks
        assert read_repairs_after > read_repairs_before
        assert retries_after == retries_before

        # MUTATION_FRAGMENTS() reads the local replica of the coordinator.
        res = cql.execute(f"SELECT ck FROM MUTATION_FRAGMENTS({ks}.t) WHERE pk = 0", host=host1)
        assert missing_ck in [row.ck for row in res]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("reversed_query", [True, False], ids=["reversed", "forward"])
async def test_read_repair_replica_missing_first_tombstones(manager: ScyllaClusterManager, reversed_query: bool):
    """Paged read repair, when one replica misses the deletes of the first rows in query order

    node1 misses the deletion of the first two rows in query order, so its
    page starts with two rows node2 only has tombstones for. Once reconciled,
    the result's last row lies beyond the last row of node1's page, so the
    coordinator cannot know whether node1 has more rows before it: the page
    has to be trimmed to node1's last row, a short read, and the client
    fetches the rest with the next page. node1 does have such a row, the
    first live one in query order, which node2 misses: a page trimmed at the
    wrong end skips it and returns the rows after it instead. Checks that the
    paged result is exactly the live rows, that the reconciliation needs no
    retry, and that both replicas get what they missed.

    Reproduces the paged half of the bug: read repair of reversed queries
    trimmed the wrong end of the reconciled result, returning stale rows.
    """
    cmdline = ["--hinted-handoff-enabled", "0", "--cache-hit-rate-read-balancing", "0"]
    config = {"read_request_timeout_in_ms": 10000}
    nodes = await manager.servers_add(2, cmdline=cmdline, config=config, auto_rack_dc="dc1")
    node1, node2 = nodes

    cql = manager.get_cql()
    host1, host2 = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 60)

    async def write_dropped_by(node: ServerInfo, stmt, params):
        """Writes at CL=ALL while node drops the write: the write fails once node has rejected
        it, so by then the other replica has applied it and node has dropped it for sure."""
        await manager.api.enable_injection(node.ip_addr, "database_apply", one_shot=False,
                                           parameters={"ks_name": ks, "cf_name": "t", "what": "throw"})
        try:
            with pytest.raises(WriteFailure):
                await cql.run_async(stmt, params)
        finally:
            await manager.api.disable_injection(node.ip_addr, "database_apply")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'")

        insert_stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (?, ?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        delete_stmt = cql.prepare(f"DELETE FROM {ks}.t WHERE pk = ? AND ck = ?")
        delete_stmt.consistency_level = ConsistencyLevel.ALL

        # The first two rows in query order get deleted, which node1 misses; the third one, the
        # first live row in query order, is missed by node2.
        cks = list(range(1, 10))
        deleted_cks = [9, 8] if reversed_query else [1, 2]
        skipped_ck = 7 if reversed_query else 3
        for ck in cks:
            if ck == skipped_ck:
                await write_dropped_by(node2, insert_stmt, (0, ck, ck))
            else:
                await cql.run_async(insert_stmt, (0, ck, ck))
        for ck in deleted_cks:
            await write_dropped_by(node1, delete_stmt, (0, ck))

        # A limit of 2 with a larger page size is paged on the server side, so short reads are
        # allowed: node1's page ends at its second row, before the reconciled result's last row.
        order = "DESC" if reversed_query else "ASC"
        select = f"SELECT ck FROM {ks}.t WHERE pk = 0 ORDER BY ck {order} LIMIT 2"

        async def sum_metric(name: str) -> int:
            total = 0
            for node in nodes:
                metrics = await manager.metrics.query(node.ip_addr)
                total += metrics.get(name) or 0
            return int(total)

        retries_before = await sum_metric("scylla_storage_proxy_coordinator_read_retries")
        read_repairs_before = await sum_metric("scylla_storage_proxy_coordinator_foreground_read_repairs")

        res = cql.execute(SimpleStatement(select, consistency_level=ConsistencyLevel.ALL, fetch_size=100), host=host1)
        actual_cks = [row.ck for row in res]

        retries_after = await sum_metric("scylla_storage_proxy_coordinator_read_retries")
        read_repairs_after = await sum_metric("scylla_storage_proxy_coordinator_foreground_read_repairs")

        expected_cks = sorted(set(cks) - set(deleted_cks), reverse=reversed_query)[:2]
        assert expected_cks[0] == skipped_ck
        assert actual_cks == expected_cks
        assert read_repairs_after > read_repairs_before
        assert retries_after == retries_before

        # MUTATION_FRAGMENTS() reads the local replica of the node it is sent to: node1 must have
        # the tombstones now, and node2 the row it missed.
        mf_select = f"SELECT ck, metadata FROM MUTATION_FRAGMENTS({ks}.t) WHERE pk = 0 AND partition_region = 2 ALLOW FILTERING"
        res = cql.execute(mf_select, host=host1)
        tombstoned_cks = {row.ck for row in res if row.metadata and '"tombstone":{"timestamp"' in row.metadata}
        assert set(deleted_cks) <= tombstoned_cks, f"deleted={deleted_cks} tombstoned on node1={tombstoned_cks}"
        res = cql.execute(mf_select, host=host2)
        assert skipped_ck in {row.ck for row in res}
