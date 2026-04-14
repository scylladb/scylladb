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
from .util import unique_name, new_function, new_cql
from .conftest import scylla_only
from cassandra.protocol import InvalidRequest, SyntaxException

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
def test_simple_literal_type_inference(cql, test_keyspace):
    assert cql.execute("SELECT 1 AS one FROM system.local").one().one == 1
    assert cql.execute("SELECT 'hello' AS greeting FROM system.local").one().greeting == 'hello'
    assert cql.execute("SELECT [1, 2, 3] AS lst FROM system.local").one().lst == [1, 2, 3]
    assert cql.execute("SELECT { 'a': 1, 'b': 2 } AS mp FROM system.local").one().mp == {'a': 1, 'b': 2}
    assert cql.execute("SELECT (1, 'a', 3.0) AS tpl FROM system.local").one().tpl == (1, 'a', 3.0)
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT ? AS qm FROM system.local")
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT :bindvar AS bv FROM system.local")

# Test SELECT without a FROM clause. Cassandra does not support this syntax.
def test_select_without_from(cql, scylla_only):
    # Constants
    assert cql.execute("SELECT 1 AS one").one().one == 1
    assert cql.execute("SELECT 'hello' AS greeting").one().greeting == 'hello'
    assert cql.execute("SELECT true AS b").one().b == True
    # Function calls
    rows = cql.execute("SELECT now() AS t")
    assert rows.one().t is not None
    rows = cql.execute("SELECT toTimestamp(now()) AS ts")
    assert rows.one().ts is not None
    # Multiple selectors
    rows = cql.execute("SELECT 1 AS one, 'hi' AS greeting")
    row = rows.one()
    assert row.one == 1
    assert row.greeting == 'hi'
    # Collections and tuples
    assert cql.execute("SELECT [1, 2, 3] AS lst").one().lst == [1, 2, 3]
    assert cql.execute("SELECT {1, 2, 3} AS st").one().st == {1, 2, 3}
    assert cql.execute("SELECT {'a': 1, 'b': 2} AS mp").one().mp == {'a': 1, 'b': 2}
    assert cql.execute("SELECT (1, 'hello', true) AS tpl").one().tpl == (1, 'hello', True)
    # CAST
    assert cql.execute("SELECT CAST(1 AS bigint) AS v").one().v == 1
    # AS aliases are optional — results can be accessed by position
    assert cql.execute("SELECT 1")[0][0] == 1
    assert cql.execute("SELECT 'hello', 42")[0][0] == 'hello'
    assert cql.execute("SELECT 'hello', 42")[0][1] == 42
    assert cql.execute("SELECT now()")[0][0] is not None
    # Failure cases
    with pytest.raises(InvalidRequest, match="FROM"):
        cql.execute("SELECT *")
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT col")
    # Clauses that only apply to SELECT with FROM are rejected as syntax errors
    with pytest.raises(SyntaxException):
        cql.execute("SELECT 1 ALLOW FILTERING")
    with pytest.raises(SyntaxException):
        cql.execute("SELECT 1 LIMIT 5")
    with pytest.raises(SyntaxException):
        cql.execute("SELECT 1 WHERE x = 1")
    with pytest.raises(SyntaxException):
        cql.execute("SELECT 1 ORDER BY x")

# Test aggregate functions with literal arguments in SELECT ... FROM.
# system.local has exactly one row, so count(x) -> 1, sum(x) -> x, etc.
def test_select_aggregates_with_from(cql, test_keyspace, scylla_only):
    # count
    assert cql.execute("SELECT count(*) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count(1) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count(0) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count(2) AS cnt FROM system.local").one().cnt == 1
    assert cql.execute("SELECT count('abc') AS cnt FROM system.local").one().cnt == 1
    # sum, avg (integer)
    assert cql.execute("SELECT sum(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT avg(5) AS v FROM system.local").one().v == 5
    # min, max (integer)
    assert cql.execute("SELECT min(5) AS v FROM system.local").one().v == 5
    assert cql.execute("SELECT max(5) AS v FROM system.local").one().v == 5
    # min, max (string)
    assert cql.execute("SELECT min('hello') AS v FROM system.local").one().v == 'hello'
    assert cql.execute("SELECT max('hello') AS v FROM system.local").one().v == 'hello'
    # Mixed aggregates and scalar expressions
    row = cql.execute("SELECT count(1) AS cnt, 42 AS num, min(10) AS mn FROM system.local").one()
    assert row.cnt == 1
    assert row.num == 42
    assert row.mn == 10
    # Bind variables fail (cannot infer type)
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT count(?) AS cnt FROM system.local")
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT count(:bindvar) AS cnt FROM system.local")

# Test aggregate functions with literal arguments in SELECT without FROM.
# Without a FROM clause, aggregates operate over a single virtual row,
# following the PostgreSQL/MySQL convention:
#   count(5) -> 1, min(5) -> 5, max(5) -> 5, etc.
def test_select_aggregates_without_from(cql, scylla_only):
    # count
    assert cql.execute("SELECT count(*) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count(1) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count(0) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count(2) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count('abc') AS cnt").one().cnt == 1
    # sum, avg (integer)
    assert cql.execute("SELECT sum(5) AS v").one().v == 5
    assert cql.execute("SELECT avg(5) AS v").one().v == 5
    # min, max (integer)
    assert cql.execute("SELECT min(5) AS v").one().v == 5
    assert cql.execute("SELECT max(5) AS v").one().v == 5
    # min, max (string)
    assert cql.execute("SELECT min('hello') AS v").one().v == 'hello'
    assert cql.execute("SELECT max('hello') AS v").one().v == 'hello'
    # AS aliases are optional - results can be accessed by position
    assert cql.execute("SELECT count(1)")[0][0] == 1
    # Mixed aggregates and scalar expressions
    row = cql.execute("SELECT count(1) AS cnt, 42 AS num, min(10) AS mn").one()
    assert row.cnt == 1
    assert row.num == 42
    assert row.mn == 10
    # Bind variables fail (cannot infer type)
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT count(?) AS cnt")
    with pytest.raises(InvalidRequest):
        cql.execute("SELECT count(:bindvar) AS cnt")

# Test that user-defined functions can be called in SELECT without FROM,
# both with a keyspace-qualified name and with an unqualified name
# (resolved against the session keyspace).
def test_udf_without_from(cql, test_keyspace, scylla_only):
    body = "(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return i + 100;'"
    with new_function(cql, test_keyspace, body) as fun:
        # Keyspace-qualified call
        assert cql.execute(f"SELECT {test_keyspace}.{fun}(1) AS v").one().v == 101
        # Unqualified call (resolved against session keyspace).
        # Use a separate connection so that USE doesn't affect other tests.
        with new_cql(cql) as ncql:
            ncql.execute(f"USE {test_keyspace}")
            assert ncql.execute(f"SELECT {fun}(1) AS v").one().v == 101
