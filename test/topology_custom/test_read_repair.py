#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import datetime
import glob
import json
import logging
import os
import pytest
import random
import struct
import subprocess
import tempfile
import time
from typing import TypeAlias, Any

from cassandra.cluster import ConsistencyLevel, Session  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from cassandra.pool import Host  # type: ignore
from cassandra.murmur3 import murmur3  # type: ignore

from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.internal_types import ServerInfo


logger = logging.getLogger(__name__)


def serialize_int(i: int) -> str:
    return struct.pack(">l", i).hex()


def serialize_key(i: int) -> str:
    return struct.pack(">hl", 4, i).hex()


class row_tombstone_data:
    pk = 0
    v = 1

    column_spec = "pk int, ck int, v int, PRIMARY KEY (pk, ck)"
    select_query = f"SELECT * FROM ks.tbl WHERE pk = {pk}"
    unique_key = 'ck'

    @classmethod
    def generate_sstable(cls, total_rows: int, live_rows: set[int], dead_timestamp: int, live_timestamp: int,
                         deletion_time: datetime.datetime):
        rows = []
        formatted_deletion_time = deletion_time.strftime("%Y-%m-%d %H:%M:%S")
        serialized_value = serialize_int(cls.v)
        for ck in range(total_rows):
            row = {
                "type": "clustering-row",
                "key": {"raw": serialize_key(ck)},
            }
            if ck in live_rows:
                row["marker"] = {"timestamp": live_timestamp}
                row["columns"] = {"v": {
                    "is_live": True,
                    "type": "regular",
                    "timestamp": live_timestamp,
                    "value": serialized_value,
                }}
            else:
                row["tombstone"] = {"timestamp": dead_timestamp, "deletion_time": formatted_deletion_time}
            rows.append(row)

        assert len(rows) == total_rows

        return [
            {
                "key": {"raw": serialize_key(cls.pk)},
                "clustering_elements": rows,
            },
        ]

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
    def check_page_count(cls, page_count):
        assert page_count > 1

    @classmethod
    def check_result_row(cls, i: int, row) -> None:
        assert row.pk == cls.pk
        assert row.ck == i
        assert row.v == cls.v


class partition_tombstone_data:
    v = 1

    column_spec = "pk int PRIMARY KEY, v int"
    select_query = "SELECT * FROM ks.tbl"
    unique_key = 'pk'

    partition_tombstone_timestamp = None
    partition_live = False

    class PartitionKey:
        def __init__(self, value):
            self.value = value
            self.raw = serialize_key(self.value)
            self.token = murmur3(struct.pack(">l", self.value))

        def __lt__(self, o):
            return self.token < o.token

    @classmethod
    def generate_sstable(cls, total_rows: int, live_rows: set[int], dead_timestamp: int, live_timestamp: int,
                         deletion_time: datetime.datetime):
        partitions = []
        formatted_deletion_time = deletion_time.strftime("%Y-%m-%d %H:%M:%S")
        serialized_value = serialize_int(cls.v)
        pks = sorted([cls.PartitionKey(pk) for pk in range(total_rows)])
        for pk in pks:
            partition: dict[str, Any] = {
                "key": {"raw": pk.raw, "token": pk.token},
            }
            if pk.value in live_rows:
                partition["clustering_elements"] = [
                    {
                        "type": "clustering-row",
                        "key": {"raw": ""},
                        "marker": {"timestamp": live_timestamp},
                        "columns": {"v": {
                            "is_live": True,
                            "type": "regular",
                            "timestamp": live_timestamp,
                            "value": serialized_value,
                        }},
                    },
                ]
            else:
                partition["tombstone"] = {"timestamp": dead_timestamp, "deletion_time": formatted_deletion_time}

            partitions.append(partition)

        assert len(partitions) == total_rows
        return partitions

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
    def check_page_count(cls, page_count):
        # We cannot reliably generate partitions such that they trigger short pages
        # So we allow for a single page too.
        pass

    @classmethod
    def check_result_row(cls, i: int, row) -> None:
        assert row.pk == i
        assert row.v == cls.v


incremental_repair_test_data = [pytest.param(row_tombstone_data, id="row-tombstone"),
                                pytest.param(partition_tombstone_data, id="partition-tombstone")]


@pytest.fixture(scope="function")
def workdir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.mark.parametrize("data_class", incremental_repair_test_data)
@pytest.mark.asyncio
async def test_incremental_read_repair(data_class, workdir, manager):
    """Stress the incremental read repair logic

    Write a long stream of row tombstones, with a live row before and after.
    """
    seed = int(time.time())
    logger.info(f"random-seed: {seed}")
    random.seed(seed)
    cmdline = ["--hinted-handoff-enabled", "0",
               "--query-tombstone-page-limit", "10",
               "--query-page-size-in-bytes", "1024"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    table_schema = f"CREATE TABLE ks.tbl ({data_class.column_spec}) WITH speculative_retry = 'NONE'"
    cql.execute(table_schema)

    schema_file_path = os.path.join(workdir, "schema.cql")
    with open(schema_file_path, "w") as schema_file:
        schema_file.write(table_schema)

    dead_timestamp = int(time.time() * 1000)
    live_timestamp = dead_timestamp + 1

    total_rows = 100
    max_live_rows = 8
    deletion_time = datetime.datetime.now()

    row_set: TypeAlias = set[int]

    async def generate_and_upload_sstable(node: ServerInfo, node_row: int) -> row_set:
        live_rows = {random.randint(0, total_rows - 1) for _ in range(random.randint(0, max_live_rows))}
        live_rows.add(node_row)

        sstable = data_class.generate_sstable(total_rows, live_rows, dead_timestamp, live_timestamp, deletion_time)
        scylla_exe = await manager.server_get_exe(node.server_id)
        node_workdir = await manager.server_get_workdir(node.server_id)
        table_upload_dir = glob.glob(os.path.join(node_workdir, "data", "ks", "tbl-*", "upload"))[0]

        input_file_path = os.path.join(workdir, f"node{node.server_id}.sstable.json")
        with open(input_file_path, "w") as f:
            json.dump(sstable, f, indent=4)

        subprocess.check_call([
            scylla_exe, "sstable", "write",
            "--schema-file", schema_file_path,
            "--input-file", input_file_path,
            "--output-dir", table_upload_dir,
            "--generation", "1"])

        await manager.api.load_new_sstables(node.ip_addr, "ks", "tbl")

        return live_rows

    node1_rows = await generate_and_upload_sstable(node1, 0)
    node2_rows = await generate_and_upload_sstable(node2, total_rows - 1)
    all_rows = node1_rows | node2_rows
    assert len(all_rows) >= 2

    logger.info(f"node1_rows: {len(node1_rows)} rows, row ids: {node1_rows}")
    logger.info(f"node2_rows: {len(node2_rows)} rows, row ids: {node2_rows}")
    logger.info(f"all_rows: {len(all_rows)} rows, row ids: {all_rows}")

    def check_rows(cql: Session, host: Host, expected_live_rows: row_set) -> None:
        actual_live_rows = set()
        actual_dead_rows = set()
        for row in cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl)", host=host):
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
        assert len(actual_live_rows) + len(actual_dead_rows) == total_rows

    logger.info("Check rows with CL=ONE before read-repair")
    check_rows(cql, host1, node1_rows)
    check_rows(cql, host2, node2_rows)

    logger.info("Run read-repair")
    res = cql.execute(SimpleStatement(data_class.select_query, consistency_level=ConsistencyLevel.ALL))
    res_rows = []
    pages = []
    while True:
        res_rows.extend(list(res.current_rows))
        pages.append(list(res.current_rows))
        if res.has_more_pages:
            res.fetch_next_page()
        else:
            break

    logger.debug(f"repair: {len(pages)} pages: {pages}")
    data_class.check_page_count(len(pages))
    assert len(res_rows) == len(all_rows)
    actual_row_ids = set()
    for res_row in res_rows:
        row_id = getattr(res_row, data_class.unique_key)
        actual_row_ids.add(row_id)
        assert row_id in all_rows
        data_class.check_result_row(row_id, res_row)
    assert actual_row_ids == all_rows

    for node in (node1, node2):
        await manager.api.keyspace_flush(node.ip_addr, "ks")
        await manager.api.keyspace_compaction(node.ip_addr, "ks")

    logger.info("Check rows with CL=ONE after read-repair")
    check_rows(cql, host1, all_rows)
    check_rows(cql, host2, all_rows)
