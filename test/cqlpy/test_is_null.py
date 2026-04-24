# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for IS NULL and IS NOT NULL in WHERE clauses - issue #8517.
#
# CQL, as defined in: cassandra.apache.org/doc/4.0/cassandra/cql/dml.html,
# doesn't support IS [NOT] NULL (except MVs), it's our CQL extension,
# so these tests will fail with Cassandra, thus are marked as `scylla_only`.
#
# The rules these tests pin down, in one place so the individual tests need not
# restate them:
#  - On a partition key column, which is never null, IS NOT NULL matches every
#    row and is dropped entirely, so it never requires ALLOW FILTERING and does
#    not count as restricting the partition key; IS NULL matches no row but is
#    still a restriction, so it does require ALLOW FILTERING.
#  - On a clustering key column neither operator is dropped, because a
#    clustering key column can read as null: a partition holding a static row
#    and no clustering rows is returned as one row whose clustering key columns
#    are all null. Both are therefore ordinary restrictions on the column,
#    obeying the usual clustering key prefix and ALLOW FILTERING rules.
#  - On a regular or static column both always require ALLOW FILTERING.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest, SyntaxException
from .util import new_test_table, unique_name, unique_key_int


# All tests in this file check Scylla-only IS [NOT] NULL support in WHERE clauses,
# so let's mark them all scylla_only with an autouse fixture.
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Shared table for tests with the same schema
@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int, c int, v int, s text, PRIMARY KEY (p, c))")
    yield table
    cql.execute(f"DROP TABLE {table}")


# Populate partition p with rows covering every combination of null v and s
def insert_null_test_data(cql, table, p):
    cql.execute(f"INSERT INTO {table} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")
    cql.execute(f"INSERT INTO {table} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table} (p, c, v, s) VALUES ({p}, 4, NULL, 'c')")  # v is null
    cql.execute(f"INSERT INTO {table} (p, c, v, s) VALUES ({p}, 5, NULL, NULL)")  # both v and s are null


# Test IS NULL on regular columns, unprepared and prepared
def test_is_null_regular_column(cql, table1):
    p = unique_key_int()
    insert_null_test_data(cql, table1, p)

    # Test IS NULL on regular column with ALLOW FILTERING
    result = cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL ALLOW FILTERING")
    assert {r.c for r in result} == {4, 5}

    # Test IS NULL on text column
    result = cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND s IS NULL ALLOW FILTERING")
    assert {r.c for r in result} == {3, 5}

    # The same query as a prepared statement, binding the partition key
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NULL ALLOW FILTERING")
    assert {r.c for r in cql.execute(stmt, [p])} == {3, 5}


# Test IS NOT NULL on regular columns, unprepared and prepared
def test_is_not_null_regular_column(cql, table1):
    p = unique_key_int()
    insert_null_test_data(cql, table1, p)

    # Test IS NOT NULL on regular column with ALLOW FILTERING
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {1, 2, 3}

    # Test IS NOT NULL on text column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND s IS NOT NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {1, 2, 4}

    # The same query as a prepared statement, binding the partition key
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NOT NULL ALLOW FILTERING")
    assert {r.c for r in cql.execute(stmt, [p])} == {1, 2, 4}


# IS [NOT] NULL on a static column, unprepared and prepared.
#
# A static column belongs to the partition, not to the row, so it is filtered on
# a different path than a regular column - but the outcome must be the same: a
# partition that never had the static column set reads it as null, and its rows
# match IS NULL and not IS NOT NULL. Both still require ALLOW FILTERING, since
# a static column can be null.
def test_is_null_static_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, s int static, v int, PRIMARY KEY (p, c)") as table:
        p_set = unique_key_int()
        p_unset = unique_key_int()
        p_deleted = unique_key_int()
        for p in [p_set, p_unset, p_deleted]:
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, 1, 10)")
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, 2, 20)")
        cql.execute(f"UPDATE {table} SET s = 7 WHERE p = {p_set}")
        # A static column that was set and then deleted reads as null again
        cql.execute(f"UPDATE {table} SET s = 7 WHERE p = {p_deleted}")
        cql.execute(f"DELETE s FROM {table} WHERE p = {p_deleted}")

        def keys(where):
            return {(r.p, r.c) for r in cql.execute(f"SELECT * FROM {table} WHERE {where} ALLOW FILTERING")
                    if r.p in (p_set, p_unset, p_deleted)}

        assert keys("s IS NOT NULL") == {(p_set, 1), (p_set, 2)}
        assert keys("s IS NULL") == {(p_unset, 1), (p_unset, 2), (p_deleted, 1), (p_deleted, 2)}

        # The same, restricted to one partition, and combined with other
        # restrictions on the row and on the static column itself
        assert keys(f"p = {p_set} AND s IS NOT NULL") == {(p_set, 1), (p_set, 2)}
        assert keys(f"p = {p_set} AND s IS NULL") == set()
        assert keys(f"p = {p_unset} AND s IS NULL") == {(p_unset, 1), (p_unset, 2)}
        assert keys(f"p = {p_set} AND c = 2 AND s IS NOT NULL") == {(p_set, 2)}
        assert keys(f"p = {p_set} AND s IS NOT NULL AND s = 7") == {(p_set, 1), (p_set, 2)}
        assert keys(f"p = {p_set} AND s IS NULL AND s = 7") == set()

        # The same queries as prepared statements
        stmt = cql.prepare(f"SELECT * FROM {table} WHERE p = ? AND s IS NULL ALLOW FILTERING")
        assert {r.c for r in cql.execute(stmt, [p_unset])} == {1, 2}
        assert {r.c for r in cql.execute(stmt, [p_set])} == set()
        stmt = cql.prepare(f"SELECT * FROM {table} WHERE p = ? AND s IS NOT NULL ALLOW FILTERING")
        assert {r.c for r in cql.execute(stmt, [p_set])} == {1, 2}
        assert {r.c for r in cql.execute(stmt, [p_unset])} == set()


# A partition can hold a static row with no clustering rows at all. SELECT still
# returns one row for such a partition, with the clustering key columns null, so
# IS [NOT] NULL on the static column has to filter that row like any other.
def test_is_null_static_column_static_only_partition(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, s int static, v int, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"UPDATE {table} SET s = 7 WHERE p = {p}")
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {p} AND s IS NOT NULL ALLOW FILTERING"))
        assert [(r.c, r.s) for r in result] == [(None, 7)]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p} AND s IS NULL ALLOW FILTERING")) == []

        # And with the static column deleted the static-only row is gone
        # altogether, so neither predicate has anything left to match.
        cql.execute(f"DELETE s FROM {table} WHERE p = {p}")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p} AND s IS NOT NULL ALLOW FILTERING")) == []
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p} AND s IS NULL ALLOW FILTERING")) == []


# Test IS NULL combined with other WHERE conditions
def test_is_null_combined_with_other_restrictions_on_different_columns(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 4, NULL, 'd')")  # v is null
    
    # Combine IS NULL with value comparison
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c > 1 AND s IS NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {2, 3}
    
    # Multiple IS NULL conditions
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL AND s IS NOT NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {4}


# Test IS NULL combined with other WHERE conditions applied to the same column
def test_is_null_combined_with_other_restrictions_on_the_same_column(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 4, NULL, 'd')")  # v is null

    # Combine IS [NOT] NULL with value comparison
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND s IS NOT NULL AND s IS NULL ALLOW FILTERING"))
    assert result == []

    # Double IS [NOT] NULL with value comparison
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL AND v IS NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {4}
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT NULL AND v IS NOT NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {1, 2, 3}

    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL AND v = 10 ALLOW FILTERING"))
    assert result == []
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT NULL AND v = 10 ALLOW FILTERING"))
    assert {r.c for r in result} == {1}

    # IS [NOT] NULL together with a slice on the very same column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v > 10 AND v IS NOT NULL ALLOW FILTERING"))
    assert {r.c for r in result} == {2, 3}
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v > 10 AND v IS NULL ALLOW FILTERING"))
    assert result == []


# Test IS NULL and IS NOT NULL on partition key columns
#
# A partition key column can never be null, so:
# - IS NULL should always return no rows, and still needs ALLOW FILTERING
# - IS NOT NULL should always be true, and needs no ALLOW FILTERING
def test_is_null_on_partition_key(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")

    # IS NULL on partition key should return no rows (keys can never be null)
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p IS NULL ALLOW FILTERING"))
    assert result == []

    # IS NOT NULL on partition key should match all rows. Since partition keys
    # can never be null the restriction is dropped, so no ALLOW FILTERING is
    # needed - the query is just a full scan.
    result = cql.execute(f"SELECT * FROM {table1} WHERE p IS NOT NULL")
    # This returns all rows from all partitions, so we should at least see our 2 rows
    assert sum(1 for r in result if r.p == p) == 2


# Test IS NULL and IS NOT NULL on clustering key columns
#
# A clustering key column is never null in a clustering row, so within a
# partition that has any, IS NULL selects none of them and IS NOT NULL selects
# all of them. (A partition with no clustering rows at all is a different
# story - see test_is_null_on_clustering_key_of_static_row.)
def test_is_null_on_clustering_key(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")

    # IS NULL on the clustering key returns no rows: every row in this
    # partition is a clustering row, and table1 has no static column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NULL"))
    assert result == []

    # ... so IS NOT NULL, with the partition key specified, returns all of them
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NOT NULL"))
    assert {r.c for r in result} == {1, 2}


# Test IS NULL and IS NOT NULL on compound partition key columns.
#
# The components of a compound partition key can never be null so IS NULL
# and IS NOT NULL are trivially false and true, respectively.
def test_is_null_on_compound_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, c int, v int, PRIMARY KEY ((p1, p2), c)") as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 1, 10)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 2, 20)")
        
        # IS NULL on first partition key component should return no rows
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p1 IS NULL ALLOW FILTERING"))
        assert result == []
        
        # IS NULL on second partition key component should return no rows
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p2 IS NULL ALLOW FILTERING"))
        assert result == []
        
        # IS NOT NULL on partition key components should match all rows, and
        # needs no ALLOW FILTERING even on a single component of a compound key
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p1 IS NOT NULL"))
        assert {(r.p1, r.p2, r.c) for r in result} == {(1, 2, 1), (1, 2, 2)}

        result = list(cql.execute(f"SELECT * FROM {table} WHERE p2 IS NOT NULL"))
        assert {(r.p1, r.p2, r.c) for r in result} == {(1, 2, 1), (1, 2, 2)}


# Test IS NULL and IS NOT NULL on compound clustering key columns
#
# In a table with no static columns every row is a clustering row, so no
# component of the clustering key is ever null: IS NULL matches nothing and
# IS NOT NULL matches everything. Both are ordinary restrictions on the column
# though, so a whole-table query needs ALLOW FILTERING either way.
def test_is_null_on_compound_clustering_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)") as table:
        cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES (1, 2, 1, 10)")
        cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES (1, 2, 2, 20)")

        # IS NULL on either clustering key component returns no rows
        assert list(cql.execute(f"SELECT * FROM {table} WHERE c1 IS NULL ALLOW FILTERING")) == []
        assert list(cql.execute(f"SELECT * FROM {table} WHERE c2 IS NULL ALLOW FILTERING")) == []

        # IS NOT NULL on either component matches every row
        result = list(cql.execute(f"SELECT * FROM {table} WHERE c1 IS NOT NULL ALLOW FILTERING"))
        assert {(r.c1, r.c2) for r in result} == {(2, 1), (2, 2)}

        result = list(cql.execute(f"SELECT * FROM {table} WHERE c2 IS NOT NULL ALLOW FILTERING"))
        assert {(r.c1, r.c2) for r in result} == {(2, 1), (2, 2)}

        # Within a single partition neither operator needs ALLOW FILTERING
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND c1 IS NOT NULL"))
        assert {(r.c1, r.c2) for r in result} == {(2, 1), (2, 2)}
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND c1 IS NULL")) == []


# Test that IS NULL without ALLOW FILTERING on a regular column raises an error
def test_is_null_without_filtering_error(cql, table1):
    # IS NULL on a regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE v IS NULL")


# Test that IS NOT NULL without ALLOW FILTERING on a regular column raises an error
def test_is_not_null_without_filtering_error(cql, table1):
    # IS NOT NULL on a regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE v IS NOT NULL")


# Test IS [NOT] NULL on key columns without ALLOW FILTERING.
#
# A partition key column is never null, so IS NOT NULL on one matches every row
# and is dropped entirely - it never requires ALLOW FILTERING. Nothing else is
# dropped: IS NULL on a partition key, and either operator on a clustering key,
# are ordinary restrictions on the column, so on a partition key they need
# ALLOW FILTERING and on a clustering key they need the partition key restricted.
def test_is_null_on_key_column_without_filtering(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES ({p}, 1, 10)")

    # IS NOT NULL on a partition key column never needs ALLOW FILTERING.
    assert sum(1 for r in cql.execute(f"SELECT * FROM {table1} WHERE p IS NOT NULL") if r.p == p) == 1

    # On a clustering key it does, unless the partition key is restricted.
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE c IS NOT NULL")
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NOT NULL"))
    assert {(r.p, r.c, r.v) for r in result} == {(p, 1, 10)}

    # IS NULL on a partition key column does need ALLOW FILTERING.
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE p IS NULL")

    # IS NULL on a clustering key needs the partition key to be restricted,
    # exactly like any other clustering key restriction.
    assert list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NULL")) == []
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE c IS NULL")


# Test that IS NULL only accepts NULL as RHS
def test_is_null_with_invalid_syntax(cql, table1):
    p = unique_key_int()
    # IS NULL with non-null value should fail with syntax error
    # (the grammar only allows IS NULL, not IS <value>)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS 123 ALLOW FILTERING")

    # IS NOT with non-null value should fail with syntax error
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT 123 ALLOW FILTERING")

    # The same holds when preparing the statement rather than executing it.
    with pytest.raises(SyntaxException):
        cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS 123 ALLOW FILTERING")
    with pytest.raises(SyntaxException):
        cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS NOT 123 ALLOW FILTERING")

    # A bind marker cannot be used to sneak a non-NULL right side past the
    # grammar: NULL has to be spelled out, so IS ? is a syntax error too.
    with pytest.raises(SyntaxException):
        cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS ? ALLOW FILTERING")
    with pytest.raises(SyntaxException):
        cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS NOT ? ALLOW FILTERING")

    # Spelled out, both prepare fine.
    cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS NULL ALLOW FILTERING")
    cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND v IS NOT NULL ALLOW FILTERING")


# Test IS NULL on multiple columns
def test_is_null_multiple_columns(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v1 int, v2 int, v3 text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 1, 10, 20, 'a')")
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 2, 10, 20, NULL)")  # v3 is null
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 3, 10, NULL, NULL)")  # v2 and v3 are null
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 4, NULL, NULL, NULL)")  # all regular columns are null
        
        # Test multiple IS NULL conditions
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND v2 IS NULL AND v3 IS NULL ALLOW FILTERING"))
        assert {r.c for r in result} == {3, 4}
        
        # Mix IS NULL and IS NOT NULL
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND v1 IS NOT NULL AND v2 IS NULL ALLOW FILTERING"))
        assert {r.c for r in result} == {3}


# Test that empty string is not treated as NULL
def test_is_null_empty_vs_null(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, s text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 1, '')")  # empty string
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 2, NULL)")  # null
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 3, NULL)")  # also null
        
        # IS NULL should only return truly null values, not empty strings
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NULL ALLOW FILTERING"))
        assert {r.c for r in result} == {2, 3}
        
        # IS NOT NULL should include empty string
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NOT NULL ALLOW FILTERING"))
        assert result[0].c == 1
        assert result[0].s == ''


# The IS [NOT] NULL predicates don't compare the column against a value, they
# only check whether it has any value at all, so unlike other relations they
# are also allowed on non-frozen collections. A non-frozen collection with no
# live elements is indistinguishable from an unset one - both read as NULL -
# so both are expected to match IS NULL.
@pytest.mark.parametrize("coltype,full,empty", [
    ("set<int>", "{1, 2}", "{}"),
    ("list<int>", "[1, 2]", "[]"),
    ("map<int, int>", "{1: 2}", "{}"),
])
def test_is_null_nonfrozen_collection(cql, test_keyspace, coltype, full, empty):
    with new_test_table(cql, test_keyspace, f"p int, c int, v {coltype}, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, {full})")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 2, NULL)")
        cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 3)")
        # An empty non-frozen collection is stored as no cells at all, i.e. NULL.
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 4, {empty})")
        # So is a collection whose every element was deleted.
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 5, {full})")
        cql.execute(f"UPDATE {table} SET v = {empty} WHERE p = 1 AND c = 5")

        result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NULL ALLOW FILTERING")
        assert {r.c for r in result} == {2, 3, 4, 5}

        result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NOT NULL ALLOW FILTERING")
        assert {r.c for r in result} == {1}


@pytest.mark.parametrize("coltype,full", [
    ("frozen<set<int>>", "{1, 2}"),
    ("frozen<list<int>>", "[1, 2]"),
    ("frozen<map<int, int>>", "{1: 2}"),
])
def test_is_null_frozen_collection(cql, test_keyspace, coltype, full):
    with new_test_table(cql, test_keyspace, f"p int, c int, v {coltype}, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, {full})")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 2, NULL)")
        cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 3)")

        result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NULL ALLOW FILTERING")
        assert {r.c for r in result} == {2, 3}

        result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NOT NULL ALLOW FILTERING")
        assert {r.c for r in result} == {1}


# IS [NOT] NULL can be combined with other relations on the same non-frozen collection
def test_is_null_nonfrozen_collection_with_contains(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v set<int>, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, {{1, 2}})")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 2, {{3}})")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 3, NULL)")

        result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NOT NULL AND v CONTAINS 1 ALLOW FILTERING")
        assert {r.c for r in result} == {1}


# IS [NOT] NULL on a non-frozen UDT column
def test_is_null_nonfrozen_udt(cql, test_keyspace):
    type_name = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TYPE {type_name} (a int, b int)")
    try:
        with new_test_table(cql, test_keyspace, f"p int, c int, v {type_name}, PRIMARY KEY (p, c)") as table:
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, {{a: 1, b: 2}})")
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 2, NULL)")
            cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 3)")
            # A UDT with only some fields set is still not NULL.
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 4, {{a: 1}})")

            result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NULL ALLOW FILTERING")
            assert {r.c for r in result} == {2, 3}

            result = cql.execute(f"SELECT c FROM {table} WHERE p = 1 AND v IS NOT NULL ALLOW FILTERING")
            assert {r.c for r in result} == {1, 4}
    finally:
        cql.execute(f"DROP TYPE {type_name}")


# IS NOT NULL on a partition key column is a tautology, so it never forces
# ALLOW FILTERING.
#
# A partition key column can never be null, so the restriction carries no
# information and is dropped altogether. Because it is gone, it neither helps
# nor hinders the rest of the WHERE clause: it does not restrict the partition
# key for the purpose of the ALLOW FILTERING rules.
def test_is_not_null_on_partition_key_never_needs_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, c1 int, c2 int, s int static, v int, PRIMARY KEY ((p1, p2), c1, c2)") as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c1, c2, s, v) VALUES (1, 2, 3, 4, 7, 5)")

        def keys(where):
            return {(r.c1, r.c2) for r in cql.execute(f"SELECT * FROM {table} WHERE " + where)}

        # A whole-table scan, with or without the tautologies spelled out.
        assert keys("p1 IS NOT NULL AND p2 IS NOT NULL") == {(3, 4)}
        assert keys("p1 IS NOT NULL") == {(3, 4)}
        # Alongside a fully restricted primary key it changes nothing.
        assert keys("p1 = 1 AND p2 = 2 AND c1 = 3 AND c2 = 4 AND p1 IS NOT NULL") == {(3, 4)}

        # Being dropped, it does not restrict the partition key: with only p1
        # given by =, the partition key is not fully restricted and the
        # clustering key restrictions need ALLOW FILTERING.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 IS NOT NULL AND p2 = 2 AND c1 = 3 AND c2 = 4")

        # A static or regular column can be null, so it keeps filtering.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 = 1 AND p2 = 2 AND s IS NOT NULL")
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 = 1 AND p2 = 2 AND v IS NOT NULL")


# A static row's clustering key columns read as null.
#
# A partition can hold a static row and no clustering rows at all. SELECT
# returns one row for such a partition, with every clustering key column null,
# so a clustering key column is not always non-null after all: IS NULL selects
# exactly those static-only rows and IS NOT NULL selects exactly the clustering
# rows. This is why neither operator can be treated as a constant on a
# clustering key column.
def test_is_null_on_clustering_key_of_static_row(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, s int static, v int, PRIMARY KEY (p, c1, c2)") as table:
        p_rows = unique_key_int()
        p_static = unique_key_int()
        cql.execute(f"INSERT INTO {table} (p, c1, c2, s, v) VALUES ({p_rows}, 3, 4, 7, 5)")
        cql.execute(f"UPDATE {table} SET s = 9 WHERE p = {p_static}")

        # In the static-only partition the one row has null c1 and c2, so it is
        # IS NULL that matches it and IS NOT NULL that does not.
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_static} AND c1 IS NULL"))
        assert [(r.c1, r.c2, r.s) for r in result] == [(None, None, 9)]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_static} AND c1 IS NOT NULL")) == []
        # Every component of the clustering key is null there, not just the first
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_static} AND c1 IS NULL AND c2 IS NULL"))
        assert [(r.c1, r.c2) for r in result] == [(None, None)]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_static} AND c1 IS NULL AND c2 IS NOT NULL")) == []

        # In the partition that does have a clustering row it is the other way
        # around - the static row is not returned separately.
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_rows} AND c1 IS NOT NULL"))
        assert [(r.c1, r.c2) for r in result] == [(3, 4)]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE p = {p_rows} AND c1 IS NULL")) == []

        # And across the whole table the two operators partition the rows.
        def keys(where):
            return {(r.p, r.c1) for r in cql.execute(f"SELECT * FROM {table} WHERE {where} ALLOW FILTERING")
                    if r.p in (p_rows, p_static)}

        assert keys("c1 IS NULL") == {(p_static, None)}
        assert keys("c1 IS NOT NULL") == {(p_rows, 3)}


# IS [NOT] NULL restricts a clustering key column, so the usual key rules apply.
#
# Neither operator can be dropped on a clustering key column - a static row's
# clustering key columns read as null, so both carry information - and both are
# handled like any other restriction on the column: they need the partition key
# fully restricted by = or IN, and they need the preceding clustering key
# columns restricted as well.
@pytest.mark.parametrize("op", ["IS NULL", "IS NOT NULL"])
def test_is_null_on_clustering_key_needs_partition_key(cql, test_keyspace, op):
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, c1 int, c2 int, v int, PRIMARY KEY ((p1, p2), c1, c2)") as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c1, c2, v) VALUES (1, 2, 3, 4, 5)")
        # The single row has non-null c1 and c2, so IS NULL matches nothing and
        # IS NOT NULL matches it. Neither answer is the point here: what matters
        # is which queries are accepted without ALLOW FILTERING.
        expected = {(3, 4)} if op == "IS NOT NULL" else set()

        def keys(where):
            return {(r.c1, r.c2) for r in cql.execute(f"SELECT * FROM {table} WHERE " + where)}

        # Whole partition key restricted by = or IN: no ALLOW FILTERING needed.
        assert keys(f"p1 = 1 AND p2 = 2 AND c1 {op}") == expected
        assert keys(f"p1 IN (1, 9) AND p2 = 2 AND c1 {op}") == expected
        assert keys(f"p1 = 1 AND p2 = 2 AND c1 {op} AND c2 {op}") == expected

        # Without a fully restricted partition key it needs ALLOW FILTERING.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE c1 {op}")
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 = 1 AND c1 {op}")
        # A token() range doesn't restrict the partition key well enough.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE token(p1, p2) >= -9223372036854775808 AND c1 {op}")
        # Neither does IS NOT NULL on the partition key, which is dropped.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 IS NOT NULL AND p2 IS NOT NULL AND c1 {op}")

        # Skipping a clustering key column is rejected as it is for any other
        # clustering key restriction - not with an ALLOW FILTERING suggestion.
        with pytest.raises(InvalidRequest, match='c2'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 = 1 AND p2 = 2 AND c2 {op}")
        # So is restricting it after a slice on the preceding column.
        with pytest.raises(InvalidRequest, match='c2'):
            cql.execute(f"SELECT * FROM {table} WHERE p1 = 1 AND p2 = 2 AND c1 > 0 AND c2 {op}")

        # With ALLOW FILTERING all of these work.
        assert keys(f"c1 {op} ALLOW FILTERING") == expected
        assert keys(f"p1 = 1 AND c1 {op} ALLOW FILTERING") == expected


# IS [NOT] NULL is a filter, so UPDATE and DELETE must reject it.
#
# The WHERE clause of a mutation names the rows to be written, and IS [NOT] NULL
# cannot name one: it tests whether a column has a value instead of saying which
# value it has, so it never yields a concrete key. Scylla rejects the other
# relations a mutation cannot express - "c != 3", for instance - and these must
# be rejected the same way, rather than being silently ignored and turning the
# statement into an unintended whole-partition write.
def test_is_null_rejected_in_mutation_where_clause(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, s int static, PRIMARY KEY (p, c)") as table:
        for where in ["p = {p} AND c IS NULL",
                      "p = {p} AND c IS NOT NULL",
                      "p = {p} AND c = 1 AND v IS NULL",
                      "p = {p} AND c = 1 AND v IS NOT NULL",
                      "p = {p} AND c = 1 AND s IS NULL",
                      "p IS NULL AND c = 1",
                      "p IS NOT NULL AND c = 1",
                      ]:
            for stmt in [f"DELETE FROM {table} WHERE {where}",
                         f"DELETE v FROM {table} WHERE {where}",
                         f"DELETE FROM {table} WHERE {where} IF EXISTS",
                         f"UPDATE {table} SET v = 99 WHERE {where}",
                         f"UPDATE {table} SET v = 99 WHERE {where} IF EXISTS",
                         ]:
                p = unique_key_int()
                for c in [1, 2, 3]:
                    cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c * 10})")
                try:
                    cql.execute(stmt.format(p=p))
                    pytest.fail(f"statement was not rejected: {stmt.format(p=p)}")
                except InvalidRequest:
                    pass
                # The rejected statement must not have modified anything
                assert {(r.c, r.v) for r in cql.execute(f"SELECT c, v FROM {table} WHERE p = {p}")} == {(1, 10), (2, 20), (3, 30)}
