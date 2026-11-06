#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Clustering key filtering of sstables, ported from scylla-dtest
# clustering_key_filter_test.py. As in the dtest, the node runs without the
# row cache (so reads go to the sstables), with hinted handoff disabled and
# a batch commitlog, and every write is flushed to an sstable of its own,
# with "nodetool flush -- <keyspace>" like in the dtest.

import asyncio
import subprocess
import time

import pytest
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "NullCompactionStrategy"])
async def test_check_non_composite(manager: ScyllaClusterManager, strategy):
    """Slices of a partition with one clustering column, written to separate
    sstables, by every combination of inclusive and exclusive bounds,
    including an empty range."""
    server = await manager.server_add(cmdline=["--enable-cache", "0"],
                                      config={"hinted_handoff_enabled": False, "commitlog_sync": "batch",
                                              "tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    exe = await manager.server_get_exe(server.server_id)

    def flush(ks):
        subprocess.run([exe, "nodetool", "-h", server.ip_addr, "flush", "--", ks], check=True, capture_output=True)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        cql.execute(f"CREATE COLUMNFAMILY {ks}.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) "
                    f"WITH compaction = {{'class':'{strategy}', 'enabled' : 'false'}}")
        flush(ks)
        for c in ["a", "b"]:
            cql.execute(SimpleStatement(f"INSERT INTO {ks}.cf (p1, c1, r1) VALUES ('key1', '{c}', 1)"))
            flush(ks)
        assert cql.execute(f"SELECT COUNT(*) FROM {ks}.cf").one().count == 2
        for where, expected in [("c1 <= 'a' AND c1 >= 'b'", []), ("c1 >= 'a' AND c1 <= 'b'", ["a", "b"]),
                                ("c1 > 'a' AND c1 <= 'b'", ["b"]), ("c1 >= 'a' AND c1 < 'b'", ["a"]),
                                ("c1 > 'a' AND c1 < 'b'", []), ("c1 > 'a'", ["b"]), ("c1 < 'b'", ["a"])]:
            rows = list(cql.execute(SimpleStatement(f"SELECT * FROM {ks}.cf WHERE p1 = 'key1' AND {where}")))
            assert [(r.p1, r.c1) for r in rows] == [("key1", c) for c in expected], where


class FilterNode:
    """TestClusteringKeyFilter's helpers on its one node: no row cache,
    hinted handoff off and a batch commitlog; keyspace ks (RF 1); every
    write optionally followed by "nodetool flush -- ks" and 0.2 s."""

    def __init__(self, cql, exe, server):
        self.cql, self.exe, self.server = cql, exe, server

    def flush(self):
        subprocess.run([self.exe, "nodetool", "-h", self.server.ip_addr, "flush", "--", "ks"], check=True, capture_output=True)

    def create_ks_and_cf(self, query):
        self.cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        self.cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        self.cql.execute(query)
        time.sleep(0.2)
        self.flush()

    def insert(self, query, flush=True):
        self.cql.execute(SimpleStatement(query))
        if flush:
            self.flush()
            time.sleep(0.2)

    def remove_row(self, row):
        self.cql.execute(SimpleStatement(f"DELETE FROM ks.cf WHERE p1='{row}';"))
        self.flush()
        time.sleep(0.2)

    def check_number_of_rows(self, expected):
        assert self.cql.execute(SimpleStatement("SELECT COUNT(*) FROM ks.cf")).one().count == expected

    def select(self, query):
        return list(self.cql.execute(SimpleStatement(query)))


def check_result(result, pkey, ckeys):
    assert len(result) == len(ckeys), "check number of clustering rows"
    for row, ckey in zip(result, ckeys):
        assert row.p1 == pkey and row.c1 == ckey


def check_result_composite(result, pkey, composite_ckeys):
    assert len(result) == len(composite_ckeys), "check number of clustering rows"
    for row, (c1, c2) in zip(result, composite_ckeys):
        assert row.p1 == pkey and row.c1 == c1 and row.c2 == c2


async def start_node(manager: ScyllaClusterManager) -> FilterNode:
    """start_cluster_and_get_node1(): populate(1) (SimpleSnitch, vnodes),
    --enable-cache 0."""
    server = await manager.server_add(cmdline=["--enable-cache", "0"],
                                      config={"hinted_handoff_enabled": False, "commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 2,
                                              "tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "endpoint_snitch": "SimpleSnitch"},
                                      property_file={"dc": "datacenter1", "rack": "rack1"})
    return FilterNode(manager.get_cql(), await manager.server_get_exe(server.server_id), server)


def strategy_string(strategy):
    return "'class':'" + strategy + "', 'enabled' : 'false'"


# Ported from scylla-dtest clustering_key_filter_test.py::TestClusteringKeyFilter::test_check_consistence_after_row_tombstone.
# A partition tombstone in an sstable whose clustering range is outside the
# query's is still applied: key2, deleted, is not returned.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "NullCompactionStrategy"])
async def test_check_consistence_after_row_tombstone(manager: ScyllaClusterManager, strategy):
    n = await start_node(manager)
    await asyncio.to_thread(n.create_ks_and_cf, "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) WITH compaction= {" + strategy_string(strategy) + "};")
    await asyncio.to_thread(n.insert, "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'a', 1);")
    await asyncio.to_thread(n.insert, "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key2', 'b', 1);")
    n.check_number_of_rows(2)
    await asyncio.to_thread(n.insert, "INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'c', 1);", False)
    await asyncio.to_thread(n.remove_row, "key2")
    n.check_number_of_rows(2)
    check_result(n.select("SELECT * FROM ks.cf WHERE p1 IN ('key1', 'key2') AND c1 >= 'a' AND c1 <= 'b';"), "key1", ["a"])


# Ported from scylla-dtest clustering_key_filter_test.py::TestClusteringKeyFilter::test_check_composite.
# Slices of a partition with two clustering columns, written to separate
# sstables, by ranges of the first column and of the second within one value
# of the first.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "NullCompactionStrategy"])
async def test_check_composite(manager: ScyllaClusterManager, strategy):
    n = await start_node(manager)
    await asyncio.to_thread(n.create_ks_and_cf, "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, c2 text, r1 int, PRIMARY KEY (p1, c1, c2)) WITH compaction= {" + strategy_string(strategy) + "};")
    for c1, c2 in [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2")]:
        await asyncio.to_thread(n.insert, f"INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', '{c1}', '{c2}', 1);")
    n.check_number_of_rows(4)
    for where, expected in [
            ("c1 <= 'a' AND c1 >= 'b'", []),
            ("c1 >= 'a' AND c1 <= 'b'", [["a", "1"], ["a", "2"], ["b", "1"], ["b", "2"]]),
            ("c1 > 'a' AND c1 <= 'b'", [["b", "1"], ["b", "2"]]),
            ("c1 >= 'a' AND c1 < 'b'", [["a", "1"], ["a", "2"]]),
            ("c1 > 'a' AND c1 < 'b'", []),
            ("c1 > 'a'", [["b", "1"], ["b", "2"]]),
            ("c1 < 'b'", [["a", "1"], ["a", "2"]]),
            ("c1 = 'a' AND c2 > '1' AND c2 < '2'", []),
            ("c1 = 'a' AND c2 >= '1' AND c2 < '2'", [["a", "1"]]),
            ("c1 = 'a' AND c2 > '1' AND c2 <= '2'", [["a", "2"]]),
            ("c1 = 'a' AND c2 >= '1' AND c2 <= '2'", [["a", "1"], ["a", "2"]]),
            ("c1 = 'a' AND c2 > '1'", [["a", "2"]]),
            ("c1 = 'a' AND c2 >= '1'", [["a", "1"], ["a", "2"]])]:
        check_result_composite(n.select(f"SELECT * FROM ks.cf WHERE p1 = 'key1' AND {where};"), "key1", expected)


# Ported from scylla-dtest clustering_key_filter_test.py::TestClusteringKeyFilter::test_check_composite_2.
# An sstable with clustering bounds [a, a] and [c, c] is not filtered out by
# a query for c1 = 'a' and c2 in (b, d).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "NullCompactionStrategy"])
async def test_check_composite_2(manager: ScyllaClusterManager, strategy):
    n = await start_node(manager)
    await asyncio.to_thread(n.create_ks_and_cf, "CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, c2 text, r1 int, PRIMARY KEY (p1, c1, c2)) WITH compaction= {" + strategy_string(strategy) + "};")
    await asyncio.to_thread(n.insert, "INSERT INTO ks.cf (p1, c1, c2, r1) VALUES ('key1', 'a', 'c', 1);")
    check_result_composite(n.select("SELECT * FROM ks.cf WHERE p1 = 'key1' AND c1 = 'a' AND c2 > 'b' AND c2 < 'd'"), "key1", [["a", "c"]])
    n.check_number_of_rows(1)
