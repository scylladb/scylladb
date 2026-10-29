# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for the shape of single-partition reads from strongly consistent
# tables - clustering slices, limits, ordering, DISTINCT, static columns,
# aggregates, filtering and paging.
#
# A strongly consistent SELECT does not go through storage_proxy and the
# query pagers. It builds its read_command by hand in
# cql3/statements/strong_consistency/select_statement.cc and runs it as one
# local query. So everything the regular path gets from the pagers - paging
# itself, the internal paging of aggregates and of filtering queries - has to
# be checked separately here. The slice itself is built by code shared with
# the regular path, so the cases below aim at what the strongly consistent
# path sets on its own: the row and partition limits, the result size limit,
# and the absence of paging.
#
# The expected result of every query is what the same query returns from an
# eventually consistent table holding the same data. The read path of such a
# table is the reference implementation of CQL semantics, so the tests don't
# repeat them.
#############################################################################

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from ..util import new_test_table


SCHEMA = "pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)"

# pk=1: a partition with a static row and 20 clustering rows, with v repeating
#       so that filtering on it and aggregating over it pick several rows.
# pk=2: a partition with only a static row.
ROWS = 20


def fill(cql, table):
    cql.execute(f"INSERT INTO {table} (pk, s) VALUES (1, 7)")
    insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (1, ?, ?)")
    execute_concurrent_with_args(cql, insert, [(ck, ck % 5) for ck in range(ROWS)], raise_on_first_error=True)
    cql.execute(f"INSERT INTO {table} (pk, s) VALUES (2, 8)")


@pytest.fixture(scope="module")
def tables(cql, sc_keyspace, test_keyspace):
    with new_test_table(cql, sc_keyspace, SCHEMA) as sc_table:
        with new_test_table(cql, test_keyspace, SCHEMA) as ec_table:
            fill(cql, sc_table)
            fill(cql, ec_table)
            yield sc_table, ec_table


def select(cql, query, table, **kwargs):
    return list(cql.execute(SimpleStatement(query.format(table=table), **kwargs)))


def assert_same_result(cql, tables, query, **kwargs):
    sc_table, ec_table = tables
    expected = select(cql, query, ec_table, **kwargs)
    assert select(cql, query, sc_table, **kwargs) == expected


# Filtering is done by the coordinator after the replica has applied the row
# limit, which is why the regular path pages internally through a filtering
# query rather than passing its LIMIT down to the replica. The strongly
# consistent path passes it down, so the replica returns the first LIMIT rows
# of the partition and the filter drops the ones that don't match.
limit_before_filter = pytest.mark.xfail(strict=True, raises=AssertionError,
    reason="SCYLLADB-5429: strongly consistent read applies LIMIT on the replica, before ALLOW FILTERING drops rows")


@pytest.mark.parametrize("query", [
    # Clustering slices.
    "SELECT * FROM {table} WHERE pk = 1",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck IN (2, 5, 100)",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 100",
    # The row limit and the per-partition limit the strongly consistent path
    # puts into the read_command itself, with get_inner_loop_limit().
    "SELECT ck, v FROM {table} WHERE pk = 1 LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 PER PARTITION LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9 LIMIT 2",
    # A reversed query reads with the reversed schema and reverses the
    # clustering bounds, so a slice and a limit have to come out of the other
    # end of the partition.
    "SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9 ORDER BY ck DESC LIMIT 2",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck IN (2, 5, 100) ORDER BY ck DESC",
    # Static columns and DISTINCT.
    "SELECT * FROM {table} WHERE pk = 2",
    "SELECT s FROM {table} WHERE pk = 1 LIMIT 3",
    "SELECT DISTINCT pk, s FROM {table} WHERE pk = 1",
    # An aggregate reads with no row limit (get_inner_loop_limit() returns
    # max_rows for it), and LIMIT then applies to the single aggregated row.
    "SELECT count(v), min(v), max(v), sum(v), avg(v) FROM {table} WHERE pk = 1",
    "SELECT count(*) FROM {table} WHERE pk = 1 LIMIT 1",
    # Filtering.
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ORDER BY ck DESC ALLOW FILTERING",
    "SELECT count(*) FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 PER PARTITION LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ORDER BY ck DESC LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
])
def test_select(cql, tables, query):
    assert_same_result(cql, tables, query)


# The strongly consistent path reads the whole result at once, whatever the
# page size the client asked for. However it's served, the rows the client
# ends up with have to be the ones an eventually consistent table returns. A
# page size that doesn't divide the result leaves a partial last page.
@pytest.mark.parametrize("query", [
    "SELECT * FROM {table} WHERE pk = 1",
    "SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 LIMIT 7",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
])
def test_paging(cql, tables, query):
    assert_same_result(cql, tables, query, fetch_size=3)


# Paged strongly consistent reads are deferred (SCYLLADB-2589), so a result
# has to fit in a single read, bounded like an unpaged regular read by
# max_memory_for_unlimited_query_hard_limit (100 MB by default). The strongly
# consistent path hardcodes 1 MB instead and ignores the option. 150 rows of
# 10 KB each go over 1 MB while staying far under the configured limit, and
# each write stays well under the raft command size limit. count(*) reads no
# cells, so its result stays small and it passes, while count(v) and max(v)
# read every cell and fail like the plain read - on the regular path
# aggregates are paged internally whatever the client's page size.
LARGE_ROWS = 150
LARGE_VALUE = b'x' * 10 * 1024


@pytest.fixture(scope="module")
def large_partition_tables(cql, sc_keyspace, test_keyspace):
    schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, sc_keyspace, schema) as sc_table:
        with new_test_table(cql, test_keyspace, schema) as ec_table:
            for table in (sc_table, ec_table):
                insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (1, ?, ?)")
                execute_concurrent_with_args(cql, insert, [(ck, LARGE_VALUE) for ck in range(LARGE_ROWS)], raise_on_first_error=True)
            yield sc_table, ec_table


# The read fails with a SERVER_ERROR, which the driver reports as NoHostAvailable.
over_result_size_limit = pytest.mark.xfail(strict=True, raises=NoHostAvailable,
    reason="SCYLLADB-5430: strongly consistent read hardcodes a 1 MB result size limit, ignoring max_memory_for_unlimited_query_hard_limit")


@pytest.mark.parametrize("query", [
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1", marks=over_result_size_limit),
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC", marks=over_result_size_limit),
    "SELECT count(*) FROM {table} WHERE pk = 1",
    pytest.param("SELECT count(v) FROM {table} WHERE pk = 1", marks=over_result_size_limit),
    pytest.param("SELECT max(v) FROM {table} WHERE pk = 1", marks=over_result_size_limit),
])
def test_partition_larger_than_result_size_limit(cql, large_partition_tables, query):
    assert_same_result(cql, large_partition_tables, query, fetch_size=10)
