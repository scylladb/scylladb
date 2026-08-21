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
# The simplest example, "SELECT 1", evaluates an untyped literal: 1 could be
# a tinyint, int, or bigint, and type inference defaults it to int. Many tests
# here use UDFs and other functions that accept known types, exercising each
# type explicitly regardless of inference. Both type inference and SELECT
# without a FROM clause are tested here as well.
#
# [1]: https://scylladb.atlassian.net/browse/SCYLLADB-296

from contextlib import contextmanager
import pytest
from .util import unique_name, new_cql, new_function, new_test_table
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
    assert cql.execute("SELECT 1").one()[0] == 1
    assert cql.execute("SELECT 'hello', 42").one()[0] == 'hello'
    assert cql.execute("SELECT 'hello', 42").one()[1] == 42
    assert cql.execute("SELECT now()").one()[0] is not None
    # Failure cases
    with pytest.raises(InvalidRequest, match="FROM"):
        cql.execute("SELECT *")
    with pytest.raises(InvalidRequest, match="FROM"):
        cql.execute("SELECT JSON *")
    with pytest.raises(InvalidRequest, match="col"):
        cql.execute("SELECT col")
    with pytest.raises(InvalidRequest, match="col"):
        cql.execute("SELECT col, 1")
    # A top-level bind marker cannot self-type, without FROM as with it
    with pytest.raises(InvalidRequest, match="infer type"):
        cql.execute("SELECT ? AS qm")
    # WHERE, GROUP BY, and ORDER BY behave as in a select over the
    # backing table: no user columns are in scope, so column
    # references fail as unknown names.
    with pytest.raises(InvalidRequest, match="x"):
        cql.execute("SELECT 1 WHERE x = 1")
    with pytest.raises(InvalidRequest, match="x"):
        cql.execute("SELECT 1 GROUP BY x")
    with pytest.raises(InvalidRequest, match="ORDER BY"):
        cql.execute("SELECT 1 ORDER BY x")
    with pytest.raises(InvalidRequest, match="x"):
        cql.execute("SELECT 1 WHERE x = 1 LIMIT 5")
    # The backing table's own column is referencable when spelled
    # quoted, exactly like in the equivalent statement with
    # FROM system.one_row. Its name contains a '$', so it cannot
    # clash with an unquoted user identifier.
    assert cql.execute("SELECT \"system$dummy\"").one()[0] == ''
    assert cql.execute("SELECT 1 WHERE \"system$dummy\" = ''").one()[0] == 1
    assert len(list(cql.execute("SELECT 1 WHERE \"system$dummy\" = 'no'"))) == 0
    assert cql.execute("SELECT 1 GROUP BY \"system$dummy\"").one()[0] == 1
    with pytest.raises(InvalidRequest, match="ORDER BY"):
        cql.execute("SELECT 1 ORDER BY \"system$dummy\"")
    # Clauses that don't reference columns work as in any select:
    assert cql.execute("SELECT 1 LIMIT 5").one()[0] == 1
    assert cql.execute("SELECT 1 PER PARTITION LIMIT 1").one()[0] == 1
    assert cql.execute("SELECT 1 ALLOW FILTERING").one()[0] == 1
    assert cql.execute("SELECT 1 BYPASS CACHE").one()[0] == 1
    assert cql.execute("SELECT 1 USING TIMEOUT 50ms").one()[0] == 1
    assert cql.execute("SELECT 1 PER PARTITION LIMIT 1 LIMIT 5 ALLOW FILTERING BYPASS CACHE USING TIMEOUT 50ms").one()[0] == 1
    # A bind marker in LIMIT is typed by the limit receiver, like with FROM
    stmt = cql.prepare("SELECT 1 LIMIT ?")
    assert cql.execute(stmt, (5,)).one()[0] == 1
    # LIMIT 0 is rejected at execution, like with FROM
    with pytest.raises(InvalidRequest, match="positive"):
        cql.execute("SELECT 1 LIMIT 0")
    # DISTINCT means partition-key deduplication, which needs a table
    with pytest.raises(SyntaxException):
        cql.execute("SELECT DISTINCT 1")
    with pytest.raises(SyntaxException):
        cql.execute("SELECT JSON DISTINCT 1")

# Test aggregate functions in SELECT without FROM.
# Without a FROM clause, aggregates run over the single row of the backing
# system.one_row table:
#   count(5) -> 1, sum(5) -> 5, min(5) -> 5, etc.
def test_select_aggregates_without_from(cql, scylla_only):
    assert cql.execute("SELECT count(1) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count(0) AS cnt").one().cnt == 1
    assert cql.execute("SELECT count('abc') AS cnt").one().cnt == 1
    assert cql.execute("SELECT count(*) AS cnt").one().cnt == 1
    assert cql.execute("SELECT min(5) AS v").one().v == 5
    assert cql.execute("SELECT max(5) AS v").one().v == 5
    assert cql.execute("SELECT sum(5) AS v").one().v == 5
    assert cql.execute("SELECT avg(5) AS v").one().v == 5
    assert cql.execute("SELECT min('hello') AS v").one().v == 'hello'
    assert cql.execute("SELECT max('hello') AS v").one().v == 'hello'
    # AS aliases are optional - results can be accessed by position
    assert cql.execute("SELECT count(1)").one()[0] == 1
    # Mixed aggregates and scalar expressions
    row = cql.execute("SELECT count(1) AS cnt, 42 AS num, min(10) AS mn").one()
    assert row.cnt == 1
    assert row.num == 42
    assert row.mn == 10

# Test that user-defined functions can be called in SELECT without FROM,
# both with a keyspace-qualified name and with an unqualified name
# (resolved against the session keyspace).
def test_udf_without_from(cql, test_keyspace, scylla_only):
    body = "(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return i + 100;'"
    with new_function(cql, test_keyspace, body) as fun:
        # Keyspace-qualified call
        assert cql.execute(f"SELECT {test_keyspace}.{fun}(1) AS v").one().v == 101
        # A bind marker typed by the UDF's parameter works via prepare
        stmt = cql.prepare(f"SELECT {test_keyspace}.{fun}(?) AS v")
        assert cql.execute(stmt, (1,)).one().v == 101
        # Unqualified call (resolved against session keyspace).
        # Use a separate connection so that USE doesn't affect other tests.
        with new_cql(cql) as ncql:
            ncql.execute(f"USE {test_keyspace}")
            assert ncql.execute(f"SELECT {fun}(1) AS v").one().v == 101
        # With no session keyspace at all, an unqualified UDF cannot resolve
        # and fails cleanly; only native functions work without a keyspace.
        with new_cql(cql) as ncql:
            with pytest.raises(InvalidRequest, match="Unknown function"):
                ncql.execute(f"SELECT {fun}(1)")

# Test SELECT JSON without FROM. The row is encoded into a single column
# named '[json]', like SELECT JSON with a FROM clause.
def test_select_json_without_from(cql, scylla_only):
    assert cql.execute("SELECT JSON 1 AS one").one()[0] == '{"one": 1}'
    # Without an alias, the expression text is the JSON key.
    assert cql.execute("SELECT JSON 1").one()[0] == '{"1": 1}'
    # JSON composes with the clauses allowed without FROM.
    assert cql.execute("SELECT JSON 1 LIMIT 5").one()[0] == '{"1": 1}'
    assert cql.execute("SELECT JSON 'hello' AS greeting, 42 AS num").one()[0] == '{"greeting": "hello", "num": 42}'
    # JSON composes with aggregates: the row is evaluated first, then encoded.
    assert cql.execute("SELECT JSON count(1) AS cnt").one()[0] == '{"cnt": 1}'

# Test that a column named "json" is not mistaken for the JSON keyword:
# SELECT json FROM ... must reach the regular SELECT statement, also when
# the column is aliased or part of a multi-selector clause.
def test_select_column_named_json(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, json int") as table:
        cql.execute(f"INSERT INTO {table} (p, json) VALUES (1, 7)")
        assert cql.execute(f"SELECT json FROM {table} WHERE p = 1").one().json == 7
        assert cql.execute(f"SELECT JSON json FROM {table} WHERE p = 1").one()[0] == '{"json": 7}'
        assert cql.execute(f"SELECT json AS x FROM {table} WHERE p = 1").one().x == 7
        row = cql.execute(f"SELECT json, p FROM {table} WHERE p = 1").one()
        assert row.json == 7
        assert row.p == 1
