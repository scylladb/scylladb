# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest


###############################################################################
# Tests for vector search related functions
###############################################################################

similarity_functions = {"cosine", "euclidean", "dot_product"}


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    schema = """
        pk int,
        ck int,
        v1 vector<float, 3>,
        v2 vector<float, 3>,
        v3 vector<float, 2>,
        v4 vector<float, 4>,
        vs vector<text, 3>,
        c int,
        s set<float>,
        PRIMARY KEY (pk, ck)
    """
    data = [
        [0.267261, 0.534522, 0.801784],
        [0.455842, 0.569803, 0.683763],
        [0.502571, 0.574367, 0.646162],
    ]
    with new_test_table(cql, test_keyspace, schema) as table1:
        for i, v in enumerate(data):
            cql.execute(f"INSERT INTO {table1} (pk, ck, v1, v2) VALUES ({i}, {i}, {v}, {v})")
        yield table1


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_on_non_float_vector_column(cql, table1, similarity_function):
    expected_error=f"Function system.similarity_{similarity_function} requires a float vector argument"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(pk, [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(ck, [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(c, [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(vs, [1.1, 1.2, 20.25]) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_on_non_vector_literal(cql, table1, similarity_function):
    expected_error=f"Function system.similarity_{similarity_function} requires a float vector argument"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, 5) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, 'dog') FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, '{{1.1, 1.2, 20.25}}') FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_on_non_float_vector(cql, table1, similarity_function):
    expected_error=f"Function system.similarity_{similarity_function} requires a float vector argument" if is_scylla(cql) else "Type error"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, '2003-05-187T16:20:00.000', 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, 'dog', 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, {{1.0, 2.0, 3.0}}, 20.25]) FROM {table1}")
    # This test is Scylla-only because Cassandra does not handle it properly and crashes on org.apache.cassandra.serializers.MarshalException.
    if is_scylla(cql):
        with pytest.raises(InvalidRequest, match=expected_error):
            cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, null, 20.25]) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_on_non_float_vector_constants(cql, table1, similarity_function):
    expected_error=f"Function system.similarity_{similarity_function} requires a float vector argument"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(1, 2) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(1, [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.1, 1.2, 20.25], 2) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}('a', 'b') FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}('a', [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.1, 1.2, 20.25], 'b') FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error ):
        cql.execute(f"SELECT pk, similarity_{similarity_function}({{1.0, 2.0, 3.0}}, {{4.0, 5.0, 6.0}}) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}({{1.0, 2.0, 3.0}}, [1.1, 1.2, 20.25]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.1, 1.2, 20.25], {{4.0, 5.0, 6.0}}) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_returns_null_on_null_arguments(cql, table1, similarity_function):
    result = cql.execute(f"SELECT pk, similarity_{similarity_function}(null, [1.1, 1.2, 20.25]) FROM {table1}")
    for row in result:
        assert row[1] is None
    result = cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, null) FROM {table1}")
    for row in result:
        assert row[1] is None
    with pytest.raises(InvalidRequest, match="Cannot infer type of argument"):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(null, null) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_on_vector_of_different_size(cql, table1, similarity_function):
    expected_error="All arguments must have the same vector dimensions"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, 1.2]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, [1.1, 1.2, 20.25, 123.7]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, v3) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, v4) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.0, 2.0, 3.0], [1.1, 1.2]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([1.0, 2.0, 3.0], [1.1, 1.2, 20.25, 123.7]) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_with_invalid_bind_variables(cql, table1, similarity_function):
    invalid_vectors = [123, ['a', 'b', 'c'], [1.0, 'b', 3.0], 45.67, 'abc']
    stmt = cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(v1, ?) FROM {table1}")
    for invalid_vector in invalid_vectors:
        with pytest.raises(TypeError):
            cql.execute(stmt, (invalid_vector,))
    stmt = cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(?, v1) FROM {table1}")
    for invalid_vector in invalid_vectors:
        with pytest.raises(TypeError):
            cql.execute(stmt, (invalid_vector,))
    with pytest.raises(InvalidRequest, match="Cannot infer type of argument ?"):
        cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(?, ?) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_with_invalid_bind_variables_wrong_length(cql, table1, similarity_function):
    invalid_vectors = ['invalid_vector', {'a': 1.0, 'b': 2.0}]
    stmt = cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(v1, ?) FROM {table1}")
    for invalid_vector in invalid_vectors:
        with pytest.raises(ValueError):
            cql.execute(stmt, (invalid_vector,))
    stmt = cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(?, v1) FROM {table1}")
    for invalid_vector in invalid_vectors:
        with pytest.raises(ValueError):
            cql.execute(stmt, (invalid_vector,))
    with pytest.raises(InvalidRequest, match="Cannot infer type of argument ?"):
        cql.prepare(f"SELECT pk, v1, similarity_{similarity_function}(?, ?) FROM {table1}")


@pytest.mark.parametrize("similarity_function", similarity_functions)
def test_vector_similarity_fails_wrong_number_of_arguments(cql, table1, similarity_function):
    expected_error=f"Invalid number of arguments for function system.similarity_{similarity_function}"
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}(v1, v1, v1) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([0.1, 0.2, 0.3], [1.0, 2.0, 3.0], [1.1, 1.2, 1.3]) FROM {table1}")
    with pytest.raises(InvalidRequest, match=expected_error):
        cql.execute(f"SELECT pk, similarity_{similarity_function}([0.1, 0.2, 0.3]) FROM {table1}")
