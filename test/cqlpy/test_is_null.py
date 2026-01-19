# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for IS NULL and IS NOT NULL in WHERE clauses
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest, SyntaxException
from .util import new_test_table, unique_name, unique_key_int


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    """Shared table for tests with the same schema"""
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int, c int, v int, s text, PRIMARY KEY (p, c))")
    yield table
    cql.execute(f"DROP TABLE {table}")


def test_is_null_regular_column(cql, table1):
    """Test IS NULL on regular columns"""
    p = unique_key_int()
    # Insert some test data
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 4, NULL, 'c')")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 5, NULL, NULL)")  # both v and s are null
    
    # Test IS NULL on regular column with ALLOW FILTERING
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {4, 5}
    
    # Test IS NULL on text column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND s IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {3, 5}


def test_is_not_null_regular_column(cql, table1):
    """Test IS NOT NULL on regular columns"""
    p = unique_key_int()
    # Insert some test data
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 4, NULL, 'c')")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 5, NULL, NULL)")  # both v and s are null
    
    # Test IS NOT NULL on regular column with ALLOW FILTERING
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 3
    assert set((r.c for r in result)) == {1, 2, 3}
    
    # Test IS NOT NULL on text column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND s IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 3
    assert set((r.c for r in result)) == {1, 2, 4}


def test_is_null_with_prepared_statement(cql, table1):
    """Test IS NULL with prepared statements"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, NULL)")  # s is null
    
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NULL ALLOW FILTERING")
    result = list(cql.execute(stmt, [p]))
    assert len(result) == 1
    assert result[0].c == 2


def test_is_not_null_with_prepared_statement(cql, table1):
    """Test IS NOT NULL with prepared statements"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, NULL)")  # s is null
    
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NOT NULL ALLOW FILTERING")
    result = list(cql.execute(stmt, [p]))
    assert len(result) == 1
    assert result[0].c == 1


def test_is_null_combined_with_other_restrictions(cql, table1):
    """Test IS NULL combined with other WHERE conditions"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 3, 30, NULL)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 4, NULL, 'd')")  # v is null
    
    # Combine IS NULL with value comparison
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c > 1 AND s IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {2, 3}
    
    # Multiple IS NULL conditions
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL AND s IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 1
    assert result[0].c == 4


def test_is_null_on_partition_key(cql, table1):
    """Test IS NULL and IS NOT NULL on partition key columns
    
    Key columns can never be null, so:
    - IS NULL should always return no rows
    - IS NOT NULL should always be true (may not require ALLOW FILTERING)
    """
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")
    
    # IS NULL on partition key should return no rows (keys can never be null)
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p IS NULL ALLOW FILTERING"))
    assert len(result) == 0
    
    # IS NOT NULL on partition key should match all rows
    # Since partition keys can never be null, this is always true
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p IS NOT NULL ALLOW FILTERING"))
    # This returns all rows from all partitions, so we should at least see our 2 rows
    assert len([r for r in result if r.p == p]) == 2


def test_is_null_on_clustering_key(cql, table1):
    """Test IS NULL and IS NOT NULL on clustering key columns
    
    Key columns can never be null, so:
    - IS NULL should always return no rows
    - IS NOT NULL should always be true (may not require ALLOW FILTERING)
    """
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 2, 20, 'b')")
    
    # IS NULL on clustering key should return no rows (keys can never be null)
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NULL ALLOW FILTERING"))
    assert len(result) == 0
    
    # IS NOT NULL on clustering key with partition key specified
    # Since clustering keys can never be null, this is always true
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND c IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 2


def test_is_null_on_compound_partition_key(cql, test_keyspace):
    """Test IS NULL and IS NOT NULL on compound partition key columns"""
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, c int, v int, PRIMARY KEY ((p1, p2), c)") as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 1, 10)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 2, 20)")
        
        # IS NULL on first partition key component should return no rows
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p1 IS NULL ALLOW FILTERING"))
        assert len(result) == 0
        
        # IS NULL on second partition key component should return no rows
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p2 IS NULL ALLOW FILTERING"))
        assert len(result) == 0
        
        # IS NOT NULL on partition key components should match all rows
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p1 IS NOT NULL ALLOW FILTERING"))
        assert len(result) == 2
        
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p2 IS NOT NULL ALLOW FILTERING"))
        assert len(result) == 2


def test_is_null_without_filtering_error(cql, table1):
    """Test that IS NULL without ALLOW FILTERING raises an error"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES ({p}, 1, 10)")
    
    # IS NULL on regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NULL")


def test_is_not_null_without_filtering_error(cql, table1):
    """Test that IS NOT NULL without ALLOW FILTERING raises an error"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES ({p}, 1, 10)")
    
    # IS NOT NULL on regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT NULL")


def test_is_null_with_invalid_syntax(cql, table1):
    """Test that IS NULL only accepts NULL as RHS"""
    p = unique_key_int()
    # IS NULL with non-null value should fail with syntax error
    # (the grammar only allows IS NULL, not IS <value>)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS 123 ALLOW FILTERING")
    
    # IS NOT with non-null value should fail with syntax error
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v IS NOT 123 ALLOW FILTERING")


def test_null_equality_returns_empty(cql, table1):
    """Test that WHERE x = null returns no results (as per SQL semantics)"""
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES ({p}, 1, NULL, NULL)")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES ({p}, 2, 10)")
    
    # x = null should return nothing (not the same as IS NULL)
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = {p} AND v = null ALLOW FILTERING"))
    assert len(result) == 0


def test_is_null_multiple_columns(cql, test_keyspace):
    """Test IS NULL on multiple columns"""
    with new_test_table(cql, test_keyspace, "p int, c int, v1 int, v2 int, v3 text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 1, 10, 20, 'a')")
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 2, 10, 20, NULL)")  # v3 is null
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 3, 10, NULL, NULL)")  # v2 and v3 are null
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 4, NULL, NULL, NULL)")  # all regular columns are null
        
        # Test multiple IS NULL conditions
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND v2 IS NULL AND v3 IS NULL ALLOW FILTERING"))
        assert len(result) == 2
        assert set((r.c for r in result)) == {3, 4}
        
        # Mix IS NULL and IS NOT NULL
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND v1 IS NOT NULL AND v2 IS NULL ALLOW FILTERING"))
        assert len(result) == 1
        assert result[0].c == 3


def test_is_null_empty_vs_null(cql, test_keyspace):
    """Test that empty string is not treated as NULL"""
    with new_test_table(cql, test_keyspace, "p int, c int, s text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 1, '')")  # empty string
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 2, NULL)")  # null
        cql.execute(f"INSERT INTO {table} (p, c, s) VALUES (1, 3, NULL)")  # also null
        
        # IS NULL should only return truly null values, not empty strings
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NULL ALLOW FILTERING"))
        assert len(result) == 2
        assert set((r.c for r in result)) == {2, 3}
        
        # IS NOT NULL should include empty string
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NOT NULL ALLOW FILTERING"))
        assert len(result) == 1
        assert result[0].c == 1
        assert result[0].s == ''
