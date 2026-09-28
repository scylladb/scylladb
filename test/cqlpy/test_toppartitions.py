# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for EXECUTE COMMAND toppartitions, comparing its results against the
# older REST API toppartitions endpoint (same underlying sampler).

import concurrent.futures
import threading
import time

import pytest
import requests
from cassandra.protocol import InvalidRequest, SyntaxException, Unauthorized

from . import nodetool
from . import util
from test.pylib.skip_types import skip_env


# The sampler only counts traffic inside the window, which the statement itself holds
# open, so send it async and keep hitting the tables until it returns.
def sample(cql, tables, query, params=None, traffic=None):
    f = cql.execute_async(query, params)
    done = threading.Event()
    f.add_callbacks(lambda _: done.set(), lambda _: done.set())
    (traffic or hot_traffic)(cql, tables, done.is_set)
    return list(f.result())


# pk 0 gets 5x the hits of all other keys combined, so it tops both samplers.
def hot_traffic(cql, tables, done):
    stmts = [(cql.prepare(f"INSERT INTO {t} (pk, v) VALUES (?, ?)"),
              cql.prepare(f"SELECT v FROM {t} WHERE pk = ?")) for t in tables]
    i = 0
    while not done():
        for insert, select in stmts:
            cql.execute(insert, [0, i])
            cql.execute(select, [0])
            if i % 5 == 0:
                cql.execute(insert, [(i % 9) + 1, i])
                cql.execute(select, [(i % 9) + 1])
        i += 1


def execute_args(**kwargs):
    return " AND ".join(f"{k} = {v!r}" if isinstance(v, str) else f"{k} = {v}" for k, v in kwargs.items())


def cql_toppartitions(cql, keyspace, table, duration=2000, traffic=None, **kwargs):
    rows = sample(cql, [f"{keyspace}.{table}"],
                  f"EXECUTE COMMAND toppartitions WITH {execute_args(keyspace_name=keyspace, table_name=table, duration=duration, **kwargs)}",
                  traffic=traffic)
    reads = sorted((r for r in rows if r.kind == 'read'), key=lambda r: r.rank)
    writes = sorted((r for r in rows if r.kind == 'write'), key=lambda r: r.rank)
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


def assert_ranked(rows):
    assert rows, "expected at least one sample"
    assert [r.rank for r in rows] == list(range(len(rows)))
    assert all(a.count >= b.count for a, b in zip(rows, rows[1:]))


# Both EXECUTE COMMAND toppartitions and the REST endpoint must agree on the hottest
# partition; counts differ as the sampling windows differ.
def test_toppartitions_matches_rest(scylla_only, cql, test_keyspace):
    if not nodetool.has_rest_api(cql):
        skip_env("REST API not available")
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        with concurrent.futures.ThreadPoolExecutor(1) as ex:
            # The REST call blocks for its window; only it needs a thread.
            rest = ex.submit(rest_toppartitions, cql, keyspace, cf, duration_ms=2000, capacity=64, list_size=5)
            cql_reads, cql_writes = cql_toppartitions(cql, keyspace, cf, capacity=64, list_size=5)
            hot_traffic(cql, [table], rest.done)
            rest_result = rest.result()
        assert_ranked(cql_reads)
        assert_ranked(cql_writes)
        assert len(cql_reads) <= 5 and len(cql_writes) <= 5
        assert rest_result['write'] and rest_result['read']
        assert cql_writes[0].partition_key == rest_result['write'][0]['partition']
        assert cql_reads[0].partition_key == rest_result['read'][0]['partition']


# With only the table given, nodetool's defaults (capacity 256, list_size 10) apply.
def test_toppartitions_default_capacity_and_list_size(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        # 20 distinct hot keys, so the default list_size (10) is what caps the result.
        def traffic(cql, tables, done):
            insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            while not done():
                for pk in range(20):
                    cql.execute(insert, [pk, 0])
        _, writes = cql_toppartitions(cql, keyspace, cf, traffic=traffic)
        assert_ranked(writes)
        assert len(writes) == 10


@pytest.mark.parametrize("args", [
    "capacity = 20 AND list_size = 512",  # list_size must be <= capacity
    "capacity = 20 AND list_size = 0",    # list_size must be positive
    "capacity = -1 AND list_size = -2",   # capacity must be positive
    "capacity = 1000000",                 # capacity is capped
    "capacity = 4097",                    # one past the max (4096)
    "kind = 'bogus'",                     # only 'read' or 'write'
    "duration = 0",                       # no sampling window
    "duration = 60001",                   # window is capped
    "capacity = 'x'",                     # type-checked at prepare
    "bogus = 1",                          # unknown argument
    "capacity = 64 AND capacity = 64",    # duplicate argument
])
def test_toppartitions_rejects_invalid_args(scylla_only, cql, test_keyspace, args):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        with pytest.raises(InvalidRequest):
            cql.execute(f"EXECUTE COMMAND toppartitions WITH keyspace_name = '{keyspace}' AND table_name = '{cf}' AND {args}")


def test_toppartitions_unknown_command(scylla_only, cql):
    with pytest.raises(InvalidRequest, match="Unknown command"):
        cql.execute("EXECUTE COMMAND no_such_command")


def test_toppartitions_table_requires_keyspace(scylla_only, cql):
    with pytest.raises(InvalidRequest, match="requires keyspace_name"):
        cql.execute("EXECUTE COMMAND toppartitions WITH table_name = 't'")


def test_toppartitions_host_not_supported(scylla_only, cql):
    with pytest.raises(InvalidRequest, match="'host_id' is not supported"):
        cql.execute("EXECUTE COMMAND toppartitions WITH host_id = 00000000-0000-0000-0000-000000000000")


# Every parameter is named; positional or call-style arguments are a syntax error.
@pytest.mark.parametrize("query", [
    "EXECUTE COMMAND toppartitions WITH 'ks'",
    "EXECUTE COMMAND toppartitions('ks', 't')",
    "EXECUTE toppartitions WITH keyspace_name = 'ks'",
])
def test_toppartitions_malformed_is_a_syntax_error(scylla_only, cql, query):
    with pytest.raises(SyntaxException):
        cql.execute(query)


# capacity/list_size at their smallest and largest legal values must both work.
@pytest.mark.parametrize("capacity,list_size", [(2, 1), (4096, 10)])
def test_toppartitions_capacity_boundaries(scylla_only, cql, test_keyspace, capacity, list_size):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        reads, writes = cql_toppartitions(cql, keyspace, cf, capacity=capacity, list_size=list_size)
        assert_ranked(reads)
        assert_ranked(writes)
        assert len(reads) <= list_size and len(writes) <= list_size


# capacity below the default list_size (10) is legal; list_size then defaults to capacity.
def test_toppartitions_small_capacity_alone(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        reads, writes = cql_toppartitions(cql, keyspace, cf, duration=500, capacity=8)
        assert_ranked(writes)
        assert len(reads) <= 8 and len(writes) <= 8


# capacity=0 would trip space_saving_top_k's assertion, so both REST endpoints reject it.
@pytest.mark.parametrize("path", ["column_family/toppartitions/{ks}:{cf}", "storage_service/toppartitions/"])
def test_toppartitions_rest_rejects_zero_capacity(scylla_only, cql, test_keyspace, path):
    if not nodetool.has_rest_api(cql):
        skip_env("REST API not available")
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        url = f"{nodetool.rest_api_url(cql)}/{path.format(ks=keyspace, cf=cf)}"
        res = requests.get(url, params={"duration": 100, "capacity": 0, "table_filters": f"{keyspace}:{cf}"}, timeout=5)
        assert res.status_code == 400 and "capacity must be positive" in res.text


# A requested capacity above the tracker's own default (256) must actually
# reach the per-shard space_saving_top_k trackers: otherwise distinct partitions
# beyond the 256th are evicted before gather() ever sees them.
def test_toppartitions_capacity_exceeds_tracker_default(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        shard_count = cql.execute("SELECT shard_count FROM system.topology").one().shard_count
        # A hard, un-threaded 256 default can never keep more than 256 partitions *per
        # shard*, however many distinct partitions are written.
        buggy_ceiling = shard_count * 256
        max_capacity = 4096
        if buggy_ceiling + 25 > max_capacity:
            skip_env(f"{shard_count} shards: buggy ceiling {buggy_ceiling} leaves no room "
                     f"under the capacity cap {max_capacity} to prove the fix")
        # ~3-sigma margin over the buggy ceiling for per-shard hash-distribution variance.
        num_partitions = min(buggy_ceiling + shard_count * 40, max_capacity - 25)
        capacity = num_partitions + 24
        # Async: serial writes are too slow to cycle every partition within the window.
        def write_all():
            for f in [cql.execute_async(insert, [pk, 0]) for pk in range(num_partitions)]:
                f.result()

        def cycle(cql, tables, done):
            while not done():
                write_all()

        write_all()
        _, writes = cql_toppartitions(cql, keyspace, cf, duration=5000, capacity=capacity,
                                      list_size=num_partitions, traffic=cycle)
        assert len(writes) > buggy_ceiling, \
            (f"expected more than {buggy_ceiling} (= {shard_count} shards x 256) of the "
             f"{num_partitions} single-hit partitions to survive a capacity-{capacity} tracker, "
             f"got {len(writes)}")


# kind selects a single sampler (nodetool's -a); the other one is not even installed.
def test_toppartitions_kind_selects_one_sampler(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        reads, writes = cql_toppartitions(cql, keyspace, cf, capacity=64, list_size=5, kind='write')
        assert reads == []
        assert_ranked(writes)

        reads, writes = cql_toppartitions(cql, keyspace, cf, capacity=64, list_size=5, kind='read')
        assert writes == []
        assert_ranked(reads)


# No filter samples every table in one window, and capacity/list_size still apply.
def test_toppartitions_all_tables(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        rows = sample(cql, [table], "EXECUTE COMMAND toppartitions WITH duration = 2000 AND capacity = 64 AND list_size = 5")
        assert any(r.keyspace_name == keyspace and r.table_name == cf for r in rows)
        assert len([r for r in rows if r.kind == 'read']) <= 5


# A nonexistent table is an error, raised before any sampling window opens.
def test_toppartitions_nonexistent_table(scylla_only, cql, test_keyspace):
    with pytest.raises(InvalidRequest, match="Unknown table"):
        cql.execute(f"EXECUTE COMMAND toppartitions WITH keyspace_name = '{test_keyspace}' AND "
                    f"table_name = 'no_such_table' AND duration = 20000")
    with pytest.raises(InvalidRequest, match="Unknown keyspace"):
        cql.execute("EXECUTE COMMAND toppartitions WITH keyspace_name = 'no_such_keyspace' AND duration = 20000")


# A keyspace-only filter samples all its tables in a single window.
def test_toppartitions_keyspace_filter(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as t1, \
         util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as t2:
        cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
        rows = sample(cql, [t1, t2],
                      f"EXECUTE COMMAND toppartitions WITH keyspace_name = '{test_keyspace}' AND duration = 2000")
        assert {r.table_name for r in rows} == {cf1, cf2}
        assert {r.keyspace_name for r in rows} == {test_keyspace}


# The statement blocks for exactly its sampling window, not the request timeout.
def test_toppartitions_duration_is_the_window(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        start = time.monotonic()
        cql_toppartitions(cql, keyspace, cf, duration=1500)
        assert time.monotonic() - start >= 1.5


# Parameters are ordinary terms, so they can be bind markers (positional or named),
# and a null binding falls back to the default.
@pytest.mark.parametrize("query,params", [
    ("EXECUTE COMMAND toppartitions WITH keyspace_name = ? AND table_name = ? AND duration = ? AND capacity = ? AND list_size = ?",
     lambda ks, cf: [ks, cf, 2000, 64, 5]),
    ("EXECUTE COMMAND toppartitions WITH keyspace_name = :ks AND table_name = :cf AND duration = :d AND capacity = :c AND list_size = :n",
     lambda ks, cf: {"ks": ks, "cf": cf, "d": 2000, "c": None, "n": 5}),
])
def test_toppartitions_prepared(scylla_only, cql, test_keyspace, query, params):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        keyspace, cf = table.split('.')
        stmt = cql.prepare(query)
        rows = sample(cql, [table], stmt, params(keyspace, cf))
        reads = sorted((r for r in rows if r.kind == 'read'), key=lambda r: r.rank)
        assert_ranked(reads)
        assert len(reads) <= 5


# A mistyped bind value is rejected as InvalidRequest, not a server error.
def test_toppartitions_prepared_rejects_invalid_value(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        stmt = cql.prepare("EXECUTE COMMAND toppartitions WITH keyspace_name = ? AND table_name = ? AND capacity = ?")
        with pytest.raises(InvalidRequest):
            cql.execute(stmt, [keyspace, cf, 0])


# Only a superuser may run toppartitions: it installs cross-shard sampling
# listeners on live tables, a real performance impact.
def test_toppartitions_requires_superuser(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY") as table:
        keyspace, cf = table.split('.')
        with util.new_user(cql) as user:
            with util.new_session(cql, user) as user_cql:
                with pytest.raises(Unauthorized):
                    user_cql.execute(f"EXECUTE COMMAND toppartitions WITH keyspace_name = '{keyspace}' AND table_name = '{cf}'")
