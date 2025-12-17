# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest
import math


###############################################################################
# Tests for vector search related functions
###############################################################################


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_fails_on_non_float_vector_column(cql, test_keyspace, similarity_function):
    schema = 'pk int, ck int, v vector<float, 3>, c int, vs vector<text, 3>, PRIMARY KEY (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v, c, vs) VALUES (1, 2, [1.0, 2.0, 3.0], 5, ['a', 'b', 'c'])")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires float vector arguments, but found pk of type int"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(pk, [1.1, 1.2, 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires float vector arguments, but found ck of type int"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(ck, [1.1, 1.2, 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires float vector arguments, but found c of type int"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(c, [1.1, 1.2, 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires float vector arguments, but found vs of type vector<text, 3>"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(vs, [1.1, 1.2, 20.25]) FROM {table}")


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_fails_on_non_vector_literal(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, 5) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, 'dog') FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, '{{1.1, 1.2, 20.25}}') FROM {table}")


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_fails_on_non_float_vector(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>" if is_scylla(cql) else "Type error"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, '2003-05-187T16:20:00.000', 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>" if is_scylla(cql) else "Type error"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, 'dog', 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match=f"Function system.similarity_{similarity_function} requires arguments to be assignable to vector<float, 3>" if is_scylla(cql) else "Type error"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, {{1.0, 2.0, 3.0}}, 20.25]) FROM {table}")
        # This test is Scylla-only because Cassandra does not handle it properly and crashes on org.apache.cassandra.serializers.MarshalException.
        if is_scylla(cql):
            with pytest.raises(InvalidRequest, match=f"null is not supported inside vectors"):
                cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, null, 20.25]) FROM {table}")


# The test is Scylla-only because Cassandra does not require the arguments to be not null.
@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_fails_on_null_arguments(cql, test_keyspace, scylla_only, similarity_function):
    schema = 'pk int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'similarity_function': '{similarity_function}'}}")
        with pytest.raises(InvalidRequest, match="Vector similarity functions cannot be executed with null arguments"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(null, [1.1, 1.2, 20.25]) FROM {table}")
        with pytest.raises(InvalidRequest, match="Vector similarity functions cannot be executed with null arguments"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, null) FROM {table}")


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_allow_both_vector_columns_and_literals_as_arguments(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v1 vector<float, 3>, v2 vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (1, [1.0, 2.0, 3.0], [4.0, 5.0, 6.0])")
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.1, 1.2, 20.25], [1.8, 0.5, 20.03]) FROM {table}")
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, v2) FROM {table}")
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.8, 0.5, 20.03]) FROM {table}")
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.1, 1.2, 20.25], v2) FROM {table}")


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_fails_on_vector_of_different_size(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        with pytest.raises(InvalidRequest, match="Invalid vector literal" if is_scylla(cql) else "All arguments must have the same vector dimensions"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, 1.2]) FROM {table}")
        with pytest.raises(InvalidRequest, match="Invalid vector literal" if is_scylla(cql) else "All arguments must have the same vector dimensions"):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v, [1.1, 1.2, 20.25, 123.7]) FROM {table}")


def calculate_distance(similarity_function, v1, v2):
    if similarity_function == "cosine":
        dot = sum(a * b for a, b in zip(v1, v2))
        norm_v = math.sqrt(sum(x**2 for x in v1))
        norm_q = math.sqrt(sum(x**2 for x in v2))
        cosine = dot / (norm_v * norm_q) if norm_v * norm_q != 0 else 0
        return round(1 - cosine, 6)
    elif similarity_function == "euclidean":
        euclidean_sq = sum((a - b)**2 for a, b in zip(v1, v2))
        return round(euclidean_sq, 6)
    elif similarity_function == "dot_product":
        dot_product = sum(a * b for a, b in zip(v1, v2))
        return round(dot_product, 6)


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_with_column_and_literal(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v vector<float, 3>'
    data = [
        [0.267261, 0.534522, 0.801784],
        [0.455842, 0.569803, 0.683763],
        [0.502571, 0.574367, 0.646162],
    ]
    query_vector = [0.707107, 0.0, -0.707107]

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, {data[0]})")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, {data[1]})")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (3, {data[2]})")
        result = cql.execute(f"SELECT v, similarity_{similarity_function}(v, {query_vector}) FROM {table}")

        for row in result:
            assert round(row[1], 6) == calculate_distance(similarity_function, row.v, query_vector)


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_with_two_columns(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key, v1 vector<float, 3>, v2 vector<float, 3>'
    data = [
        [0.267261, 0.534522, 0.801784],
        [0.455842, 0.569803, 0.683763],
        [0.502571, 0.574367, 0.646162],
    ]
    query_vector = [0.707107, 0.0, -0.707107]

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (1, {data[0]}, {query_vector})")
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (2, {data[1]}, {query_vector})")
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (3, {data[2]}, {query_vector})")
        result = cql.execute(f"SELECT v1, v2, similarity_{similarity_function}(v1, v2) FROM {table}")

        for row in result:
            assert round(row[2], 6) == calculate_distance(similarity_function, row.v1, row.v2)


@pytest.mark.parametrize("similarity_function", ["cosine", "euclidean", "dot_product"])
def test_vector_similarity_with_two_literals(cql, test_keyspace, similarity_function):
    schema = 'pk int primary key'
    v1 = [0.267261, 0.534522, 0.801784]
    v2 = [0.707107, 0.0, -0.707107]

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (pk) VALUES (1)")
        result = cql.execute(f"SELECT pk, similarity_{similarity_function}({v1}, {v2}) FROM {table}")

        for row in result:
            assert round(row[1], 6) == calculate_distance(similarity_function, v1, v2)
