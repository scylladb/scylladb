# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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
from .util import unique_name, new_function, new_test_table
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

# Test that simple literals without type hints succeed with type inference,
# and bind variables fail because they cannot self-type.
def test_simple_literal_type_inference(cql, test_keyspace, scylla_only):
    assert cql.execute("SELECT 1 AS one FROM system.local").one().one == 1
    assert cql.execute("SELECT 'hello' AS greeting FROM system.local").one().greeting == 'hello'
    assert cql.execute("SELECT [1, 2, 3] AS lst FROM system.local").one().lst == [1, 2, 3]
    assert cql.execute("SELECT { 'a': 1, 'b': 2 } AS mp FROM system.local").one().mp == {'a': 1, 'b': 2}
    assert cql.execute("SELECT (1, 'a', 3.0) AS tpl FROM system.local").one().tpl == (1, 'a', 3.0)
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT ? AS qm FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT :bindvar AS bv FROM system.local")

# Test that count with a literal argument works with types that can be inferred,
# and fails when the argument is a bind variable (which cannot self-type).
def test_count_literal_args(cql, test_keyspace, scylla_only):
    assert cql.execute("SELECT count(*) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count(1) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count('abc') AS cnt FROM system.local").one().cnt == 1
    with pytest.raises(InvalidRequest, match="only valid when argument types are known"):
        cql.execute("SELECT count(?) AS cnt FROM system.local")
    with pytest.raises(InvalidRequest, match="only valid when argument types are known"):
        stmt = cql.prepare("SELECT count(:bindvar) AS cnt FROM system.local")
        cql.execute(stmt, {'bindvar': 1})

# sum() and avg() each have several numeric overloads, so a literal argument used
# to match an overload was rejected as "Ambiguous call". Inferring the literal's
# default type lets overload resolution pick a single signature. system.local has
# exactly one row, so the aggregate of a literal is just that literal.
def test_select_aggregates_with_literal_args(cql, test_keyspace, scylla_only):
    assert cql.execute("SELECT sum(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT avg(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT min(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT max(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT min('hello') AS v FROM system.local").one().v == 'hello'
    assert cql.execute("SELECT max('hello') AS v FROM system.local").one().v == 'hello'
    # Aggregates and scalar literals mixed in the same SELECT.
    row = cql.execute("SELECT count(1) AS cnt, 42 AS num, min(10) AS mn FROM system.local").one()
    assert row.cnt == 1
    assert row.num == 42
    assert row.mn == 10

# Map literal {'a': 1} (inferred as map<text, int>) passed to a function
# expecting map<text, bigint>.
def test_map_literal_widening_direct_function_arg(cql, test_keyspace, scylla_only):
    body = "(m map<text, bigint>) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
    with new_function(cql, test_keyspace, body, args="map<text, bigint>") as fn:
        ksfn = f"{test_keyspace}.{fn}"
        row = cql.execute(f"SELECT {ksfn}({{'a': 1, 'b': 2}}) AS result FROM system.local").one()
        assert row.result == 42

# Map literal nested inside a collection that is a function arg: the inner {'a': 1}
# (map<text, int>) must widen to the inner function's map<text, bigint> parameter.
def test_map_literal_widening_nested_in_collection_arg(cql, test_keyspace, scylla_only):
    inner_body = "(m map<text, bigint>) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
    with new_function(cql, test_keyspace, inner_body, args="map<text, bigint>") as inner_fn:
        outer_body = f"(lst list<bigint>) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return lst[1];'"
        with new_function(cql, test_keyspace, outer_body, args="list<bigint>") as outer_fn:
            ksouter = f"{test_keyspace}.{outer_fn}"
            ksinner = f"{test_keyspace}.{inner_fn}"
            row = cql.execute(f"SELECT {ksouter}([{ksinner}({{'a': 1}}), 2]) AS result FROM system.local").one()
            assert row.result == 42

# Test that count(fn(col)) correctly counts only non-null results.
# The UDF returns NULL for inputs divisible by 7, so out of 14 rows
# (values 1-14), only values 7 and 14 produce NULL, giving count = 12.
def test_count_function_with_nulls(cql, test_keyspace, scylla_only):
    body = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'if i % 7 == 0 then return nil else return i end;'"
    with new_function(cql, test_keyspace, body, args="int") as fn:
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
            for i in range(1, 15):
                cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i})")
            ksfn = f"{test_keyspace}.{fn}"
            row = cql.execute(f"SELECT count({ksfn}(v)) AS cnt FROM {table}").one()
            assert row.cnt == 12
