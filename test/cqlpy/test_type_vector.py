# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests involving the "vector" column type.

import pytest
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest, SyntaxException


def test_vector_of_set_using_arguments_binding(cql, test_keyspace):
    """
    Tests that inserting and reading a vector of sets using argument binding
    succeeds.

    This test reproduces a problem where deserializing a value of a
    vector of sets incorrectly throws std::bad_cast exception.

    See issue #26704.
    """
    with new_test_table(cql, test_keyspace, "p int primary key, v vector<set<int>,2>") as table:
        prepared = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
        value_to_insert = [{1, 2, 3}, {4, 5, 6}]

        cql.execute(prepared, [0, value_to_insert])
        row = cql.execute(f"SELECT v FROM {table}").one()

        assert row is not None
        assert row.v == value_to_insert


# This is an artificial limit set to the value matching the OpenSearch implementation of Vector Search.
# Cassandra itself does not have a hard limit on the dimension of vectors, except mentioning 2^13 as a recommended maximum in the documentation.
# Instead, we test with Java's Integer.MAX_VALUE (2^31 - 1).
@pytest.fixture(scope="module")
def MAX_VECTOR_DIMENSION(cql):
    return 16000 if is_scylla(cql) else 2**31 - 1


def test_vector_dimension_upper_bound_is_allowed(cql, test_keyspace, MAX_VECTOR_DIMENSION):
    with new_test_table(cql, test_keyspace, f"pk int primary key, v vector<float, {MAX_VECTOR_DIMENSION}>"):
        pass


def test_vector_dimension_above_upper_bound_is_rejected(cql, test_keyspace, MAX_VECTOR_DIMENSION):
    with pytest.raises(InvalidRequest, match=f"Vectors must have a dimension less than or equal to {MAX_VECTOR_DIMENSION}") if is_scylla(cql) else pytest.raises(SyntaxException, match="NumberFormatException"):
        with new_test_table(cql, test_keyspace, f"pk int primary key, v vector<float, {MAX_VECTOR_DIMENSION + 1}>"):
            pass


def test_vector_dimension_zero_is_rejected(cql, test_keyspace):
    with pytest.raises(InvalidRequest, match="Vectors must have a dimension greater than 0" if is_scylla(cql) else "vectors may only have positive dimensions"):
        with new_test_table(cql, test_keyspace, "pk int primary key, v vector<float, 0>"):
            pass


def test_vector_dimension_negative_is_rejected(cql, test_keyspace):
    with pytest.raises(InvalidRequest, match="Vectors must have a dimension greater than 0" if is_scylla(cql) else "vectors may only have positive dimensions"):
        with new_test_table(cql, test_keyspace, "pk int primary key, v vector<float, -18>"):
            pass


@pytest.mark.parametrize("invalid_dimension", ["dog", "123x", "1.5"])
def test_vector_dimension_non_integer_is_rejected(cql, test_keyspace, invalid_dimension):
    with pytest.raises(SyntaxException):
        with new_test_table(cql, test_keyspace, f"pk int primary key, v vector<float, {invalid_dimension}>"):
            pass
