# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests to check if invalid ANN queries are handled correctly.
# These tests ensure that queries that do not meet the requirements for ANN
# indexing throw the appropriate exceptions.
###############################################################################

import pytest
import re
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest, SyntaxException

ANN_REQUIRES_INDEX_MESSAGE = "ANN ordering by vector requires the column to be indexed"
SCYLLA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector does not support filtering"
CASSANDRA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector requires all restricted column(s) to be indexed"


@pytest.fixture(scope="module")
def indexed_vector_table(cql, test_keyspace, scylla_only):
    """A table with a vector_index on the vector column `v`, for the function-style
    ANN() syntax tests below. The syntax is a ScyllaDB extension."""
    schema = 'p int primary key, c int, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        yield table


def test_ann_query_without_index(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=re.escape(ANN_REQUIRES_INDEX_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5")

def test_ann_query_with_null_vector(cql, test_keyspace):
    schema = 'p int primary key, c int, v vector<float, 3>'
    custom_index = 'vector_index' if is_scylla(cql) else 'sai'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING '{custom_index}'")

        with pytest.raises(InvalidRequest, match="Unsupported null value for column v"):
            cql.execute(f"SELECT * FROM {table} ORDER BY v ANN OF null LIMIT 5")

###############################################################################
# The function-style syntax ORDER BY ANN(column, query_vector) is a ScyllaDB
# extension, equivalent to ORDER BY column ANN OF query_vector. The tests below
# cover the ways it can be used incorrectly.
###############################################################################

def test_ann_function_without_index(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=re.escape(ANN_REQUIRES_INDEX_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_in_where_clause(cql, indexed_vector_table):
    # ANN is a reserved keyword, so a relation cannot even name it.
    with pytest.raises(SyntaxException, match="no viable alternative at input 'ANN'"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} WHERE ANN(v, [0.1, 0.2, 0.3]) > 0 LIMIT 5")

def test_quoted_ann_function_in_where_clause(cql, indexed_vector_table):
    # A quoted function name bypasses the keyword restriction and reaches the
    # semantic check, which is where the useful error message comes from.
    with pytest.raises(InvalidRequest, match=re.escape("ANN() is only supported in the ORDER BY clause")):
        cql.execute(f'SELECT * FROM {indexed_vector_table} WHERE "ann"(v, [0.1, 0.2, 0.3]) > 0 LIMIT 5')

def test_ann_function_in_select_clause(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("ANN() is not supported in the SELECT clause")):
        cql.execute(f"SELECT p, ANN(v, [0.1, 0.2, 0.3]) FROM {indexed_vector_table} "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_too_few_arguments(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("ANN() takes exactly two arguments")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v) LIMIT 5")

def test_ann_function_with_too_many_arguments(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("ANN() takes exactly two arguments")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, [0.1, 0.2, 0.3], 7) LIMIT 5")

def test_ann_function_on_undefined_column(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="Undefined column name bad_col"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(bad_col, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_wrong_query_vector_dimension(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="Invalid vector literal"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, [0.1, 0.2]) LIMIT 5")

def test_ann_function_with_non_column_first_argument(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("First argument to ANN() must be a column reference")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} "
                    f"ORDER BY ANN([0.4, 0.5, 0.6], [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_column_query_vector(cql, indexed_vector_table):
    # The query vector must be a value, not another row's vector.
    with pytest.raises(InvalidRequest, match=re.escape("Second argument to ANN() must not be a column reference")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, v) LIMIT 5")

def test_ann_function_on_non_vector_column(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("ANN ordering is only supported on float vector indexes")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(c, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_null_vector(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="Unsupported null value for column v"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, null) LIMIT 5")

def test_unknown_scoring_function_in_order_by(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape("Only ANN() and BM25() are supported as scoring functions in ORDER BY")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY now() LIMIT 5")
