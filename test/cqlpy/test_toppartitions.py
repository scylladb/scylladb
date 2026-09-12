# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the system.toppartitions virtual table, comparing its results
# against the older nodetool/REST toppartitions endpoint (same underlying
# sampler, different front end) to confirm the CQL path is a faithful
# alternative rather than a second, divergent implementation.

import threading
import time

import pytest
import requests
from cassandra.protocol import InvalidRequest, ReadFailure, Unauthorized

from . import nodetool
from . import util
from test.pylib.skip_types import skip_env

# Validation errors thrown inside toppartitions_table::execute() cross the
# replica RPC boundary as a generic ReadFailure, not InvalidRequest: the
# read-path exception encoder (replica::exception_variant) doesn't carry
# invalid_request_exception. Tracked as a known gap, not fixed here.

# Every page opens a fresh sampling window, so all queries here must fit in one page.
PAGE_SIZE = 5000


def cql_toppartitions(cql, keyspace, table, capacity=None, list_size=None, extra_where="", timeout_ms=6000):
    where = f"keyspace_name = '{keyspace}' AND table_name = '{table}'"
    if capacity is not None:
        where += f" AND capacity = {capacity}"
    if list_size is not None:
        where += f" AND list_size = {list_size}"
    rows = list(cql.execute(
        f"SELECT * FROM system.toppartitions WHERE {where} {extra_where} USING TIMEOUT {timeout_ms}ms"))
    assert len(rows) < PAGE_SIZE
    reads = [r for r in rows if r.kind == 'read']
    writes = [r for r in rows if r.kind == 'write']
    reads.sort(key=lambda r: r.rank)
    writes.sort(key=lambda r: r.rank)
    return reads, writes


def rest_toppartitions(cql, keyspace, table, duration_ms, capacity, list_size):
    if not nodetool.has_rest_api(cql):
        skip_env("REST API not available")
    url = f"{nodetool.rest_api_url(cql)}/column_family/toppartitions/{keyspace}:{table}"
    params = {"duration": duration_ms, "capacity": capacity, "list_size": list_size}
    # The endpoint blocks for the whole sampling window, so the read timeout must outlast it.
    res = requests.get(url, params=params, timeout=(5, duration_ms / 1000 + 5))
    res.raise_for_status()
    return res.json()


# Background reads+writes with partition 0 far hotter than the rest, so it must
# top both samplers regardless of the sampling window. A failure in the thread
# is re-raised by the test instead of silently producing an empty sample.
class HotTraffic:
    def __init__(self, cql, table):
        self.insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        self.select = cql.prepare(f"SELECT v FROM {table} WHERE pk = ?")
        self.cql = cql
        self.stop = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self._run)

    def _run(self):
        try:
            i = 0
            while not self.stop.is_set():
                self.cql.execute(self.insert, [0, i])
                self.cql.execute(self.select, [0])
                if i % 5 == 0:
                    self.cql.execute(self.insert, [(i % 9) + 1, i])
                    self.cql.execute(self.select, [(i % 9) + 1])
                i += 1
        except Exception as e:
            self.error = e

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()
        if self.error and not exc[0]:
            raise self.error


def assert_ranked(rows):
    assert rows, "expected at least one sample"
    assert [r.rank for r in rows] == list(range(len(rows)))
    assert all(a.count >= b.count for a, b in zip(rows, rows[1:]))


# Sample live traffic via both the CQL system.toppartitions table and the old
# REST endpoint, and check they agree on which partition is hottest (the one
# exact thing both paths reproduce deterministically; counts differ since the
# two samples don't span identical windows).
def test_toppartitions_matches_rest(scylla_only, cql, test_keyspace):
    if not nodetool.has_rest_api(cql):
        skip_env("REST API not available")
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            cql_reads, cql_writes = cql_toppartitions(cql, keyspace, cf, capacity=64, list_size=5, timeout_ms=3000)
            rest_result = rest_toppartitions(cql, keyspace, cf, duration_ms=2000, capacity=64, list_size=5)
        assert_ranked(cql_reads)
        assert_ranked(cql_writes)
        assert len(cql_reads) <= 5 and len(cql_writes) <= 5
        assert rest_result['write'] and rest_result['read']
        assert cql_writes[0].partition_key == rest_result['write'][0]['partition']
        assert cql_reads[0].partition_key == rest_result['read'][0]['partition']


def test_toppartitions_default_capacity_and_list_size(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            rows = list(cql.execute(
                f"SELECT capacity, list_size FROM system.toppartitions "
                f"WHERE keyspace_name = '{keyspace}' AND table_name = '{cf}' LIMIT 1 USING TIMEOUT 3000ms"))
        assert rows and rows[0].capacity == 256 and rows[0].list_size == 10


# kind/rank follow capacity/list_size in the clustering key, so restricting them
# (nodetool's -a, or a top-N cut) is legal and must not trip the equality check.
@pytest.mark.parametrize("extra_where", [
    "AND kind = 'write' AND rank < 2",
    "AND kind IN ('write') AND rank IN (0, 1)",
])
def test_toppartitions_kind_and_rank_restrictions(scylla_only, cql, test_keyspace, extra_where):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            reads, writes = cql_toppartitions(cql, keyspace, cf, capacity=64, list_size=5,
                                              extra_where=extra_where, timeout_ms=3000)
        assert reads == []
        assert_ranked(writes)
        assert len(writes) <= 2


@pytest.mark.parametrize("extra_where", [
    "AND capacity = 20 AND list_size = 512",  # list_size must be < capacity
    "AND capacity = 20 AND list_size = 0",    # list_size must be positive
    "AND capacity = -1 AND list_size = -2",   # capacity must be positive
    "AND capacity = 1000000",                 # capacity is capped
    "AND capacity = 4097",                    # one past the max (4096)
    "AND capacity = 8",                       # default list_size (10) is not smaller
    "AND capacity IN (64, 128)",              # only equality is supported
    "AND capacity = 64 AND list_size > 5",    # only equality is supported
    "AND capacity = 64 AND list_size IN (5, 10)",  # IN is rejected on list_size too
])
def test_toppartitions_rejects_invalid_args(scylla_only, cql, test_keyspace, extra_where):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        with pytest.raises(ReadFailure):
            cql.execute(
                f"SELECT * FROM system.toppartitions WHERE keyspace_name = '{keyspace}' "
                f"AND table_name = '{cf}' {extra_where} USING TIMEOUT 6000ms")


# capacity/list_size at their smallest and largest legal values must both work.
@pytest.mark.parametrize("capacity,list_size", [(2, 1), (4096, 10)])
def test_toppartitions_capacity_boundaries(scylla_only, cql, test_keyspace, capacity, list_size):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            reads, writes = cql_toppartitions(cql, keyspace, cf, capacity=capacity, list_size=list_size,
                                              timeout_ms=3000)
        assert_ranked(reads)
        assert_ranked(writes)
        assert len(reads) <= list_size and len(writes) <= list_size


# Restricting kind to a value the sampler never produces is a legal filter,
# not an error: it must return an empty result, not raise.
def test_toppartitions_kind_filters_to_empty(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            rows = list(cql.execute(
                f"SELECT * FROM system.toppartitions WHERE keyspace_name = '{keyspace}' "
                f"AND table_name = '{cf}' AND capacity = 64 AND list_size = 5 AND kind = 'bogus' "
                f"USING TIMEOUT 3000ms"))
        assert rows == []


# Skipping the keyspace/table prefix needs ALLOW FILTERING -- without it,
# this is a coordinator-side parse error (InvalidRequest), not a replica one.
def test_toppartitions_unrestricted_partition_key_needs_allow_filtering(scylla_only, cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT * FROM system.toppartitions WHERE capacity = 64 USING TIMEOUT 2000ms")


# capacity/list_size are still real sampler inputs (not mere filters) even when
# the keyspace/table prefix is left unrestricted -- they're a valid clustering
# prefix by themselves, only a gap *before* them turns them into filters.
def test_toppartitions_capacity_is_an_input_across_all_tables(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            rows = list(cql.execute(
                f"SELECT keyspace_name, table_name, capacity, list_size FROM system.toppartitions "
                f"WHERE capacity = 64 AND list_size = 5 ALLOW FILTERING USING TIMEOUT 3000ms"))
        mine = [r for r in rows if r.keyspace_name == keyspace and r.table_name == cf]
        assert mine
        assert all(r.capacity == 64 and r.list_size == 5 for r in rows)


# Restricting list_size without capacity is a gap in the clustering prefix (capacity is
# the earlier column): cql3 can't turn it into a range, so it's an ordinary post-read
# filter, not a sampler input. Sampling still runs with defaults (list_size=10), so
# filtering for a different value matches nothing -- it doesn't override the sample.
def test_toppartitions_gapped_restriction_is_a_filter_not_an_input(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            rows = list(cql.execute(
                "SELECT list_size FROM system.toppartitions WHERE list_size = 999 "
                "ALLOW FILTERING USING TIMEOUT 3000ms"))
        assert rows == []

# A timeout that leaves no sampling window is an error, not an empty result.
def test_toppartitions_rejects_too_short_timeout(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        with pytest.raises(ReadFailure):
            cql.execute(
                f"SELECT * FROM system.toppartitions WHERE keyspace_name = '{keyspace}' "
                f"AND table_name = '{cf}' USING TIMEOUT 500ms")


# A nonexistent table must return nothing without opening a sampling window,
# so the query must finish well before the window (timeout minus margin) would.
def test_toppartitions_nonexistent_table(scylla_only, cql, test_keyspace):
    start = time.monotonic()
    rows = list(cql.execute(
        f"SELECT * FROM system.toppartitions WHERE keyspace_name = '{test_keyspace}' "
        f"AND table_name = 'no_such_table' USING TIMEOUT 20000ms"))
    assert rows == []
    assert time.monotonic() - start < 10


# A partition-key IN opens one window per table; they must run concurrently, or
# the later ones would starve of timeout budget and fail. Both tables must report.
def test_toppartitions_partition_key_in(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as t1, \
         util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as t2:
        cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
        with HotTraffic(cql, t1), HotTraffic(cql, t2):
            start = time.monotonic()
            rows = list(cql.execute(
                f"SELECT table_name, kind, rank FROM system.toppartitions "
                f"WHERE keyspace_name = '{test_keyspace}' AND table_name IN ('{cf1}', '{cf2}') "
                f"USING TIMEOUT 3000ms"))
            elapsed = time.monotonic() - start
        assert {r.table_name for r in rows} == {cf1, cf2}
        assert elapsed < 4


# Reversed reads must keep the sampler parameters and only flip row order.
def test_toppartitions_reverse_order(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with HotTraffic(cql, table):
            rows = list(cql.execute(
                f"SELECT capacity, list_size, kind, rank FROM system.toppartitions "
                f"WHERE keyspace_name = '{keyspace}' AND table_name = '{cf}' "
                f"AND capacity = 64 AND list_size = 5 "
                f"ORDER BY capacity DESC, list_size DESC, kind DESC, rank DESC USING TIMEOUT 3000ms"))
        assert rows and all(r.capacity == 64 and r.list_size == 5 for r in rows)
        assert [(r.kind, r.rank) for r in rows] == sorted(((r.kind, r.rank) for r in rows), reverse=True)


# Only a superuser may query system.toppartitions: it installs cross-shard
# sampling listeners on live tables, a real performance impact that a plain
# SELECT grant shouldn't be able to trigger.
def test_toppartitions_requires_superuser(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        with util.new_user(cql) as user:
            cql.execute(f"GRANT SELECT ON system.toppartitions TO {user}")
            with util.new_session(cql, user) as user_cql:
                with pytest.raises(Unauthorized):
                    user_cql.execute(
                        f"SELECT * FROM system.toppartitions WHERE keyspace_name = '{keyspace}' "
                        f"AND table_name = '{cf}' USING TIMEOUT 6000ms")
