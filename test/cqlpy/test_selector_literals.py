# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests literals in the SELECT clause.
#
# Originally, the CQL grammar allowed literals (constants, bind markers, and
# collections/tuples/UDTs of literals) only in the WHERE clause. This test suite
# tests literals in the SELECT clause, which were added later [1].
#
# The simplest example, "SELECT 1" actually doesn't work since its type cannot
# be inferred (is it a tinyint, int, or bigint?), so we use UDFs and other functions
# that accept known types instead. We do test that "SELECT 1" and similar fail
# in the expected way due to type inference failure.
#
# [1]: https://scylladb.atlassian.net/browse/SCYLLADB-296

from contextlib import contextmanager
import pytest
from .util import unique_name, new_function
from .conftest import scylla_only
from cassandra.protocol import InvalidRequest

want_lua = scylla_only

def test_simple_literal_selectors(cql, test_keyspace, want_lua):
    @contextmanager
    def new_sum_function(name: str, type: str, op: str):
        body = f"(i {type}, j {type}) RETURNS NULL ON NULL INPUT RETURNS {type} LANGUAGE lua AS 'return i {op} j;'"
        with new_function(cql, test_keyspace, body, name=name, args=f"{type}, {type}") as f:
            yield f

    # Create two different functions with the same name fun, but a
    # different signature (different parameters):
    fun = unique_name()
    ksfun = f"{test_keyspace}.{fun}"
    with new_sum_function(name=fun, type="int", op="+"):
        rows = cql.execute(f"SELECT {ksfun}(1, 2) AS sum_int FROM system.local")
        assert rows.one().sum_int == 3
        stmt = cql.prepare(f"SELECT {ksfun}(?, ?) AS sum_int FROM system.local")
        rows = cql.execute(stmt, (10, 20))
        assert rows.one().sum_int == 30
        with pytest.raises(InvalidRequest, match="Type error"):
            cql.execute(f"SELECT {ksfun}(1, 'asf') AS sum_int FROM system.local")
    with new_sum_function(name=fun, type="text", op=".."):
        rows = cql.execute(f"SELECT {ksfun}('hello, ', 'world!') AS sum_text FROM system.local")
        assert rows.one().sum_text == "hello, world!"
        stmt = cql.prepare(f"SELECT {ksfun}(?, ?) AS sum_text FROM system.local")
        rows = cql.execute(stmt, ('foo', 'bar'))
        assert rows.one().sum_text == "foobar"
        with pytest.raises(InvalidRequest, match="Type error"):
            cql.execute(f"SELECT {ksfun}('asf', 1) AS sum_text FROM system.local")

# scylla-only due to set_intersection function
def test_set_literal_selector(cql, test_keyspace, scylla_only):
    cql.execute(f"CREATE TABLE IF NOT EXISTS {test_keyspace}.sets (id int PRIMARY KEY, vals set<int>, vals2 set<frozen<map<text, int>>>)")
    cql.execute(f"INSERT INTO {test_keyspace}.sets (id, vals) VALUES (1, {{1, 2, 3, 4, 5}})")
    rows = cql.execute(f"SELECT set_intersection(vals, {{3,4,5,6,7}}) AS intersection FROM {test_keyspace}.sets WHERE id=1")
    assert rows.one().intersection == {3,4,5}

    cql.execute(f"INSERT INTO {test_keyspace}.sets (id, vals2) VALUES (1, {{ {{ 'aa': 1, 'bb': 2 }}, {{ 'cc': 3, 'dd': 4 }} }})")
    rows = cql.execute(f"SELECT set_intersection(vals2, {{ {{ 'cc': 3, 'dd': 4 }}, {{ 'cc': 3, 'dd': 5 }} }}) AS intersection FROM {test_keyspace}.sets WHERE id=1")
    assert rows.one().intersection == {frozenset([('cc', 3), ('dd', 4)])}

# Test that simple literals without type hints fail as expected due to type inference failure.
def test_simple_literal_type_inference_failure(cql, test_keyspace):
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT 1 AS one FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT 'hello' AS greeting FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT [1, 2, 3] AS lst FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT { 'a': 1, 'b': 2 } AS mp FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT (1, 'a', 3.0) AS tpl FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT ? AS qm FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT :bindvar AS bv FROM system.local")

# Test that count(2) fails as expected. We're likely to relax this restriction later
# as it is quite artificial. scylla_only because Cassandra does allow it.
def test_count_literal_only_1(cql, test_keyspace, scylla_only):
    with pytest.raises(InvalidRequest, match="expects a column or the literal 1 as an argument"):
        cql.execute("SELECT count(2) AS cnt FROM system.local")
    # Error message here is not the best, but tightening error messages
    # here is quite a hassle and we plan to relax the restriction later anyway.
    with pytest.raises(InvalidRequest, match="only valid when argument types are known"):
        cql.execute("SELECT count(?) AS cnt FROM system.local")
