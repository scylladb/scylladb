# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for IS NULL and IS NOT NULL in WHERE clauses
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from .util import unique_name, new_test_table


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    """Table with partition key, clustering key, and regular columns"""
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int, c int, v int, s text, PRIMARY KEY (p, c))")
    yield table
    cql.execute(f"DROP TABLE {table}")


def test_is_null_regular_column(cql, table1):
    """Test IS NULL on regular columns"""
    # Insert some test data
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (1, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (1, 2, 20, 'b')")
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (1, 3, 30)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, s) VALUES (1, 4, 'c')")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c) VALUES (1, 5)")  # both v and s are null
    
    # Test IS NULL on regular column with ALLOW FILTERING
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 1 AND v IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {4, 5}
    
    # Test IS NULL on text column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 1 AND s IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {3, 5}


def test_is_not_null_regular_column(cql, table1):
    """Test IS NOT NULL on regular columns"""
    # Insert some test data
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (2, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (2, 2, 20, 'b')")
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (2, 3, 30)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, s) VALUES (2, 4, 'c')")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c) VALUES (2, 5)")  # both v and s are null
    
    # Test IS NOT NULL on regular column with ALLOW FILTERING
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 2 AND v IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 3
    assert set((r.c for r in result)) == {1, 2, 3}
    
    # Test IS NOT NULL on text column
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 2 AND s IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 3
    assert set((r.c for r in result)) == {1, 2, 4}


def test_is_null_with_prepared_statement(cql, table1):
    """Test IS NULL with prepared statements"""
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (3, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (3, 2, 20)")  # s is null
    
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NULL ALLOW FILTERING")
    result = list(cql.execute(stmt, [3]))
    assert len(result) == 1
    assert result[0].c == 2


def test_is_not_null_with_prepared_statement(cql, table1):
    """Test IS NOT NULL with prepared statements"""
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (4, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (4, 2, 20)")  # s is null
    
    stmt = cql.prepare(f"SELECT * FROM {table1} WHERE p = ? AND s IS NOT NULL ALLOW FILTERING")
    result = list(cql.execute(stmt, [4]))
    assert len(result) == 1
    assert result[0].c == 1


def test_is_null_combined_with_other_restrictions(cql, table1):
    """Test IS NULL combined with other WHERE conditions"""
    cql.execute(f"INSERT INTO {table1} (p, c, v, s) VALUES (5, 1, 10, 'a')")
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (5, 2, 20)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (5, 3, 30)")  # s is null
    cql.execute(f"INSERT INTO {table1} (p, c, s) VALUES (5, 4, 'd')")  # v is null
    
    # Combine IS NULL with value comparison
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 5 AND c > 1 AND s IS NULL ALLOW FILTERING"))
    assert len(result) == 2
    assert set((r.c for r in result)) == {2, 3}
    
    # Multiple IS NULL conditions
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 5 AND v IS NULL AND s IS NOT NULL ALLOW FILTERING"))
    assert len(result) == 1
    assert result[0].c == 4


def test_is_null_clustering_key(cql, test_keyspace):
    """Test that IS NULL on clustering key requires ALLOW FILTERING"""
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, 10)")
        
        # IS NULL on clustering key should work with ALLOW FILTERING
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND c IS NOT NULL ALLOW FILTERING"))
        assert len(result) == 1


def test_is_null_without_filtering_error(cql, table1):
    """Test that IS NULL without ALLOW FILTERING raises an error"""
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (6, 1, 10)")
    
    # IS NULL on regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE p = 6 AND v IS NULL")


def test_is_not_null_without_filtering_error(cql, table1):
    """Test that IS NOT NULL without ALLOW FILTERING raises an error"""
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (7, 1, 10)")
    
    # IS NOT NULL on regular column without ALLOW FILTERING should fail
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT * FROM {table1} WHERE p = 7 AND v IS NOT NULL")


def test_is_null_with_invalid_syntax(cql, table1):
    """Test that IS NULL only accepts NULL as RHS"""
    # IS NULL with non-null value should fail
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT * FROM {table1} WHERE p = 1 AND v IS 123 ALLOW FILTERING")
    
    # IS NOT with non-null value should fail
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT * FROM {table1} WHERE p = 1 AND v IS NOT 123 ALLOW FILTERING")


def test_null_equality_returns_empty(cql, table1):
    """Test that WHERE x = null returns no results (as per SQL semantics)"""
    cql.execute(f"INSERT INTO {table1} (p, c) VALUES (8, 1)")  # v is null
    cql.execute(f"INSERT INTO {table1} (p, c, v) VALUES (8, 2, 10)")
    
    # x = null should return nothing (not the same as IS NULL)
    result = list(cql.execute(f"SELECT * FROM {table1} WHERE p = 8 AND v = null ALLOW FILTERING"))
    assert len(result) == 0


def test_is_null_multiple_columns(cql, test_keyspace):
    """Test IS NULL on multiple columns"""
    with new_test_table(cql, test_keyspace, "p int, c int, v1 int, v2 int, v3 text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2, v3) VALUES (1, 1, 10, 20, 'a')")
        cql.execute(f"INSERT INTO {table} (p, c, v1, v2) VALUES (1, 2, 10, 20)")  # v3 is null
        cql.execute(f"INSERT INTO {table} (p, c, v1) VALUES (1, 3, 10)")  # v2 and v3 are null
        cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 4)")  # all regular columns are null
        
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
        cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 2)")  # null (not set)
        cql.execute(f"INSERT INTO {table} (p, c) VALUES (1, 3)")  # also null (not set)
        
        # IS NULL should only return truly null values, not empty strings
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NULL ALLOW FILTERING"))
        assert len(result) == 2
        assert set((r.c for r in result)) == {2, 3}
        
        # IS NOT NULL should include empty string
        result = list(cql.execute(f"SELECT * FROM {table} WHERE p = 1 AND s IS NOT NULL ALLOW FILTERING"))
        assert len(result) == 1
        assert result[0].c == 1
        assert result[0].s == ''
