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
from .util import new_test_table, new_function, is_scylla
from cassandra.protocol import InvalidRequest, SyntaxException

ANN_REQUIRES_INDEX_MESSAGE = "ANN ordering by vector requires the column to be indexed"
UNKNOWN_SCORING_FUNCTION_MESSAGE = "Only ANN() and BM25() are supported as scoring functions in ORDER BY"
# ANN()'s arguments are checked by ordinary function resolution, which runs before the vector
# search claims the ORDER BY clause, so argument-count and argument-type diagnostics come from
# the function layer and name the resolved function.
ANN_ARGUMENT_COUNT_MESSAGE = "Invalid number of arguments for function system.ann"
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
    # ANN is a reserved keyword, so a relation cannot even name it. The exact parser
    # message is not asserted on purpose - the useful diagnostic is covered by
    # test_quoted_ann_function_in_where_clause below.
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {indexed_vector_table} WHERE ANN(v, [0.1, 0.2, 0.3]) > 0 LIMIT 5")

def test_quoted_ann_function_in_where_clause(cql, indexed_vector_table):
    # A quoted function name bypasses the keyword restriction and reaches the semantic
    # checks. With an ANN ordering the query is a vector search, which is what claims the
    # restriction - and rejects it, because threshold filtering is not implemented.
    with pytest.raises(InvalidRequest, match=re.escape("ANN() is not supported in the WHERE clause")):
        cql.execute(f'SELECT * FROM {indexed_vector_table} WHERE "ann"(v, [0.1, 0.2, 0.3]) > 0 '
                    f'ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5')

def test_quoted_ann_function_in_where_clause_without_ordering(cql, indexed_vector_table):
    # Without an ORDER BY nothing claims the restriction. It must not be silently dropped:
    # scoring restrictions never reach the filtering machinery.
    with pytest.raises(InvalidRequest, match="requires a matching ORDER BY clause"):
        cql.execute(f'SELECT * FROM {indexed_vector_table} WHERE "ann"(v, [0.1, 0.2, 0.3]) > 0 LIMIT 5')

# ANN() selected reports the score the rows are ranked by, so it needs an ANN ordering to agree
# with, on the query vector and the column both.
def test_ann_function_in_select_clause_without_ordering(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest,
            match=re.escape("ANN() is not supported in the SELECT clause without a matching ANN ordering")):
        cql.execute(f"SELECT p, ANN(v, [0.1, 0.2, 0.3]) FROM {indexed_vector_table} WHERE p = 1")

def test_ann_function_in_select_with_different_query_vector(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest,
            match=re.escape("ANN() in SELECT must use the same query vector as the ANN ordering")):
        cql.execute(f"SELECT p, ANN(v, [0.4, 0.5, 0.6]) FROM {indexed_vector_table} "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_in_select_on_different_column(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>, w vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        with pytest.raises(InvalidRequest,
                match=re.escape("ANN() in SELECT must reference the same column as the ANN ordering")):
            cql.execute(f"SELECT p, ANN(w, [0.1, 0.2, 0.3]) FROM {table} "
                        f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

# Aggregation is rejected whether or not the score is selected. Worth pinning for the selected
# case, because it is what keeps the two users of the temporary slot space apart: the score arrives
# in a slot allocated during prepare, and an aggregate's running state would want one of its own.
def test_ann_query_with_aggregation_rejected(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="cannot be run with aggregation"):
        cql.execute(f"SELECT count(*) FROM {indexed_vector_table} "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")
    with pytest.raises(InvalidRequest, match="cannot be run with aggregation"):
        cql.execute(f"SELECT max(ANN(v, [0.1, 0.2, 0.3])) FROM {indexed_vector_table} "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

# GROUP BY is aggregation too, and is rejected whether or not the score is selected.
def test_ann_query_with_group_by_rejected(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="cannot be run with aggregation"):
        cql.execute(f"SELECT p FROM {indexed_vector_table} GROUP BY p "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")
    with pytest.raises(InvalidRequest, match="cannot be run with aggregation"):
        cql.execute(f"SELECT ANN(v, [0.1, 0.2, 0.3]) FROM {indexed_vector_table} GROUP BY p "
                    f"ORDER BY ANN(v, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_too_few_arguments(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape(ANN_ARGUMENT_COUNT_MESSAGE)):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v) LIMIT 5")

def test_ann_function_with_too_many_arguments(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape(ANN_ARGUMENT_COUNT_MESSAGE)):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, [0.1, 0.2, 0.3], 7) LIMIT 5")

def test_ann_function_on_undefined_column(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="Unrecognized name bad_col"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(bad_col, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_wrong_query_vector_dimension(cql, indexed_vector_table):
    # The query vector's dimension is inferred from the ordered column, so a mismatch is a
    # function argument-type error.
    with pytest.raises(InvalidRequest, match="All arguments must have the same vector dimensions"):
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
    with pytest.raises(InvalidRequest, match=re.escape("requires a float vector argument, but found c of type int")):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(c, [0.1, 0.2, 0.3]) LIMIT 5")

def test_ann_function_with_null_vector(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match="Unsupported null value for column v"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY ANN(v, null) LIMIT 5")

def test_ann_function_in_select_with_null_bind_marker(cql, indexed_vector_table):
    # A null query vector is rejected before the two occurrences are compared, so the SELECT one
    # does not report a disagreement with the ordering.
    stmt = cql.prepare(f"SELECT p, ANN(v, ?) FROM {indexed_vector_table} ORDER BY ANN(v, ?) LIMIT 5")
    with pytest.raises(InvalidRequest, match="Unsupported null value"):
        cql.execute(stmt, [None, None])

def test_unknown_scoring_function_in_order_by(cql, indexed_vector_table):
    with pytest.raises(InvalidRequest, match=re.escape(UNKNOWN_SCORING_FUNCTION_MESSAGE)):
        cql.execute(f"SELECT * FROM {indexed_vector_table} ORDER BY now() LIMIT 5")

def test_keyspace_qualified_ann_in_order_by_rejected(cql, test_keyspace, indexed_vector_table):
    # Only the native, unqualified ann() is the vector ordering function. A keyspace-qualified
    # name refers to a user-defined function, which no external search handler claims - here
    # there is no such function at all, so resolution fails first.
    with pytest.raises(InvalidRequest, match="Unknown function .* called"):
        cql.execute(f"SELECT * FROM {indexed_vector_table} "
                    f"ORDER BY {test_keyspace}.ann(v, [0.1, 0.2, 0.3]) LIMIT 5")

def test_user_defined_ann_is_not_shadowed_by_native_ann(cql, test_keyspace, indexed_vector_table):
    # Same as above, but with a UDF actually named ann() in the keyspace: it must not be silently
    # executed as native ANN. The UDF is resolved as any other function would be - it is not an
    # external function, so no external search handler claims it - and its int arguments do not
    # accept a vector.
    body = "(a int, b int) CALLED ON NULL INPUT RETURNS float LANGUAGE lua AS 'return 0.0'"
    with new_function(cql, test_keyspace, body, name="ann", args="int, int"):
        with pytest.raises(InvalidRequest, match="cannot be passed as argument 0 of function"):
            cql.execute(f"SELECT * FROM {indexed_vector_table} "
                        f"ORDER BY {test_keyspace}.ann(v, [0.1, 0.2, 0.3]) LIMIT 5")
