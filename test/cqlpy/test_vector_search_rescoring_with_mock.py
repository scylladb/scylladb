# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for vector search rescoring and oversampling behavior.
#
# These tests use the vector store mock infrastructure from
# test_vector_search_with_vector_store_mock.py to verify that Scylla correctly
# applies oversampling when querying the vector store and rescores results
# returned by the vector store before returning them to the client.
###############################################################################

import json
from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from cassandra.query import UNSET_VALUE
from cassandra.protocol import InvalidRequest

from .test_vector_search_with_vector_store_mock import vector_store_mock, _vector_store_mock_session
from .util import new_test_table


# All tests in this file require tablet support (vector search with vector_index).
@pytest.fixture(scope="function", autouse=True)
def require_tablets(skip_without_tablets):
    pass

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

@dataclass
class EmbeddingRow:
    id: int
    embedding: list[float]
    expected_similarity: float


# Test data for each similarity function, ordered best-to-worst similarity to ANN_QUERY_VECTOR.
TEST_DATA = {
    "cosine": [
        EmbeddingRow(1, [0.1, 0.1], 1.0),
        EmbeddingRow(2, [0.1, 0.2], 0.97),
        EmbeddingRow(3, [-0.1, 0.8], 0.81),
        EmbeddingRow(4, [-0.1, 0.4], 0.76),
    ],
    "euclidean": [
        EmbeddingRow(1, [0.1, 0.2], 0.99),
        EmbeddingRow(2, [0.1, 0.4], 0.91),
        EmbeddingRow(3, [0.1, 0.8], 0.67),
        EmbeddingRow(4, [0.1, 1.6], 0.30),
    ],
    "dot_product": [
        EmbeddingRow(1, [0.1, 1.6], 0.585),
        EmbeddingRow(2, [0.1, 0.8], 0.545),
        EmbeddingRow(3, [0.2, 0.4], 0.53),
        EmbeddingRow(4, [0.1, 0.1], 0.51),
    ],
}

# Query vector used in ANN queries across rescoring tests.
ANN_QUERY_VECTOR = [0.1, 0.1]
# CQL literal for ANN_QUERY_VECTOR, kept in sync automatically.
ANN_QUERY_VECTOR_LITERAL = str(ANN_QUERY_VECTOR)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ANN_SYNTAX = [
    pytest.param("cassandra"),
    pytest.param("function", marks=pytest.mark.xfail(reason="function-style ANN syntax not yet implemented (SCYLLADB-2210)")),
]

def order_by_ann(column, vector, syntax):
    """Build ORDER BY clause for ANN query in Cassandra or function-style syntax."""
    if syntax == "cassandra":
        return f"ORDER BY {column} ANN OF {vector}"
    return f"ORDER BY ANN({column}, {vector})"

def reversed_ann_response(data):
    """Return a JSON mock ANN response with row ids in reversed order and
    descending similarity scores (0.01*N, ..., 0.01).

    Rescoring tests feed this to the mock so they can verify that Scylla
    reorders results from worst-first back to best-first similarity order."""
    ids = list(reversed([d_row.id for d_row in data]))
    scores = [0.01 * (len(ids) - i) for i in range(len(ids))]
    return json.dumps({"primary_keys": {"id": ids}, "similarity_scores": scores})


@contextmanager
def rescoring_test_table(cql, keyspace, data, extra_options=None):
    """Context manager that creates a table with schema (id int primary key,
    embedding vector<float, 2>), creates a vector_index configured for
    rescoring with b1 quantization and cosine similarity (default), inserts
    all rows, and yields the table name.

    Pass extra_options to override individual index options, e.g.
    extra_options={"similarity_function": "euclidean"} or
    extra_options={"quantization": "f32"} to test without rescoring."""
    options = {
        "quantization": "b1",
        "oversampling": "2.0",
        "rescoring": "true",
        "similarity_function": "cosine",
    }
    if extra_options:
        options.update(extra_options)
    opts_str = ", ".join(f"'{k}': '{v}'" for k, v in options.items())

    schema = "id int primary key, embedding vector<float, 2>"
    with new_test_table(cql, keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
            f"WITH OPTIONS = {{{opts_str}}}")
        for d_row in data:
            vec_str = ", ".join(str(x) for x in d_row.embedding)
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES ({d_row.id}, [{vec_str}])")
        yield table


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Verifies that the LIMIT sent to the vector store is ceil(oversampling * cql_limit).
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_oversampling_multiplies_limit_for_vector_store_query(cql, test_keyspace, vector_store_mock, ann_syntax):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '3.4'}}")

        order_by = order_by_ann("embedding", "[0.1, 0.2, 0.3]", ann_syntax)
        cql.execute(f"SELECT * FROM {table} {order_by} LIMIT 3")

        requests = vector_store_mock.ann_requests
        assert requests, "Expected at least one ANN request to the vector store"
        # ceil(3.4 * 3) = ceil(10.2) = 11
        assert json.loads(requests[-1].body)["limit"] == 11


# Verifies that when the vector store returns more results than the CQL LIMIT,
# the output is trimmed to the CQL LIMIT.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_oversampled_vector_store_results_are_limited_to_cql_limit(cql, test_keyspace, vector_store_mock, ann_syntax):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '2'}}")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (1, [1, 1, 1])")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (2, [1, 1, 1])")

        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"id": [1, 2]},
            "similarity_scores": [0, 0],
        }))
        order_by = order_by_ann("embedding", "[1, 1, 1]", ann_syntax)
        rows = list(cql.execute(f"SELECT id FROM {table} {order_by} LIMIT 1"))

        assert len(rows) == 1


# Verifies that per-request oversampling in the ANN() options map overrides the
# index-level oversampling option, affecting the limit sent to the vector store.
# Options are passed as map<text, text>.
#
# Reproduces SCYLLADB-553.
@pytest.mark.xfail(reason="per-request oversampling not yet implemented (SCYLLADB-553)")
def test_per_request_oversampling_override(cql, test_keyspace, vector_store_mock):
    schema = "id int primary key, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
                    f"WITH OPTIONS = {{'oversampling': '2.0'}}")

        prepared = cql.prepare(
            f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], ?) LIMIT 2")

        # Valid oversampling values and expected limits (CQL LIMIT=2).
        valid_cases = [
            ('1.0',        2),
            ('50.5',       101),
            ('100.0',      200),
            ('10',         20),
            ('1e1',        20),
            ('1000000e-4', 200),
        ]
        for factor, expected_limit in valid_cases:
            # Literal
            cql.execute(f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], "
                        f"{{'oversampling': '{factor}'}}) LIMIT 2")
            actual_limit = json.loads(vector_store_mock.ann_requests[-1].body)["limit"]
            assert actual_limit == expected_limit, (
                f"literal OVERSAMPLING '{factor}': expected limit {expected_limit}, got {actual_limit}")

            # Bound
            cql.execute(prepared, [{'oversampling': factor}])
            actual_limit = json.loads(vector_store_mock.ann_requests[-1].body)["limit"]
            assert actual_limit == expected_limit, (
                f"bound OVERSAMPLING '{factor}': expected limit {expected_limit}, got {actual_limit}")

        # UNSET map → falls back to index oversampling 2.0 → limit = ceil(2.0 * 2) = 4.
        cql.execute(prepared, [UNSET_VALUE])
        actual_limit = json.loads(vector_store_mock.ann_requests[-1].body)["limit"]
        assert actual_limit == 4

        # Empty map {} → falls back to index oversampling 2.0 → limit 4.
        cql.execute(prepared, [{}])
        actual_limit = json.loads(vector_store_mock.ann_requests[-1].body)["limit"]
        assert actual_limit == 4


# Verifies that invalid per-request oversampling values in the ANN() options
# map are rejected with InvalidRequest.
#
# Reproduces SCYLLADB-553.
@pytest.mark.xfail(reason="per-request oversampling not yet implemented (SCYLLADB-553)")
def test_per_request_oversampling_invalid_values(cql, test_keyspace):
    schema = "id int primary key, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        prepared = cql.prepare(
            f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], ?) LIMIT 2")

        # Out-of-range or unparseable oversampling values → InvalidRequest.
        invalid_values = ['0.9', '100.1', '0', '-5', 'NaN', 'INFINITY', '0x10', 'abc', 'inf']
        for factor in invalid_values:
            # Literal
            with pytest.raises(InvalidRequest):
                cql.execute(f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], "
                            f"{{'oversampling': '{factor}'}}) LIMIT 2")
            # Bound
            with pytest.raises(InvalidRequest):
                cql.execute(prepared, [{'oversampling': factor}])


# Verifies that reversed ANN order is rescored back to expected similarity order.
# Runs for all three similarity functions.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_result_returned_by_vector_store_is_rescored(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT id FROM {table} {order_by} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            assert len(rows[0]) == 1


# Verifies that per-request rescoring option in the ANN() options map overrides
# the index-level rescoring setting. Parametrized by index_rescoring to test
# both directions: disabling rescoring on a rescoring-enabled index, and
# enabling rescoring on a rescoring-disabled index.
# Tests literal, bound, UNSET and empty map paths.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
@pytest.mark.parametrize("index_rescoring", ["true", "false"])
def test_per_request_rescoring_override(cql, test_keyspace, vector_store_mock, index_rescoring):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name, "rescoring": index_rescoring}) as table:
            override_value = "false" if index_rescoring == "true" else "true"
            index_is_rescored = (index_rescoring == "true")

            # Expected id orders
            rescored_order = [d_row.id for d_row in data[:2]]
            non_rescored_order = list(reversed([d_row.id for d_row in data]))[:2]

            prepared = cql.prepare(
                f"SELECT id FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, ?) LIMIT 2")

            # Literal: override flips the index-level behavior
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT id FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
                f"{{'rescoring': '{override_value}'}}) LIMIT 2"))
            expected = rescored_order if not index_is_rescored else non_rescored_order
            assert [row.id for row in rows] == expected
            assert len(rows[0]) == 1

            # Bound: override flips the index-level behavior
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(prepared, [{'rescoring': override_value}]))
            assert [row.id for row in rows] == expected
            assert len(rows[0]) == 1

            # UNSET bind → falls back to index-level setting
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(prepared, [UNSET_VALUE]))
            fallback = rescored_order if index_is_rescored else non_rescored_order
            assert [row.id for row in rows] == fallback
            assert len(rows[0]) == 1

            # Empty map {} → falls back to index-level setting
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(prepared, [{}]))
            assert [row.id for row in rows] == fallback
            assert len(rows[0]) == 1


# Verifies that invalid per-request rescoring values in the ANN() options
# map are rejected with InvalidRequest.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
def test_per_request_rescoring_invalid_values(cql, test_keyspace):
    schema = "id int primary key, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        prepared = cql.prepare(
            f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], ?) LIMIT 2")

        # Values that are not valid boolean strings → InvalidRequest.
        invalid_values = ['1', '0', '1.0', 'yes', 'enabled', 'on', 'off', '']
        for value in invalid_values:
            # Literal
            with pytest.raises(InvalidRequest):
                cql.execute(f"SELECT id FROM {table} ORDER BY ANN(embedding, [0.5, 0.5], "
                            f"{{'rescoring': '{value}'}}) LIMIT 2")
            # Bound
            with pytest.raises(InvalidRequest):
                cql.execute(prepared, [{'rescoring': value}])


# Verifies that per-request rescoring and oversampling can be combined in a
# single ANN() options map.
#
# Reproduces SCYLLADB-553, SCYLLADB-184.
@pytest.mark.xfail(reason="per-request ANN options not yet implemented (SCYLLADB-553, SCYLLADB-184)")
def test_per_request_rescoring_and_oversampling_combined(cql, test_keyspace, vector_store_mock):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"oversampling": "1.0", "rescoring": "false"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))

        rows = list(cql.execute(
            f"SELECT id FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
            f"{{'rescoring': 'true', 'oversampling': '3.0'}}) LIMIT 2"))

        # Verify oversampling: limit sent to vector store = ceil(3.0 * 2) = 6
        actual_limit = json.loads(vector_store_mock.ann_requests[-1].body)["limit"]
        assert actual_limit == 6

        # Verify rescoring: results are in rescored (best similarity) order
        expected = data[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert len(rows[0]) == 1


# Verifies that f32 quantization disables rescoring -- results keep the
# vector store's original (reversed) order.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_f32_quantization_disables_rescoring(cql, test_keyspace, vector_store_mock, ann_syntax):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"quantization": "f32"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
        rows = list(cql.execute(
            f"SELECT id FROM {table} {order_by} LIMIT 2"))

        # Without rescoring the vector store's reversed order is preserved.
        expected = list(reversed(data))[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert len(rows[0]) == 1


# Verifies that per-request rescoring=true cannot override f32 quantization —
# f32 always disables rescoring regardless of per-request options.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
def test_f32_quantization_ignores_per_request_rescoring(cql, test_keyspace, vector_store_mock):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"quantization": "f32"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT id FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
            f"{{'rescoring': 'true'}}) LIMIT 2"))

        # f32 quantization overrides per-request rescoring; order stays reversed.
        expected = list(reversed(data))[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert len(rows[0]) == 1


# Verifies that a similarity function in the SELECT clause is computed with
# rescored (reordered) results.
# Kept separate from test_result_returned_by_vector_store_is_rescored to ensure
# the similarity-column and non-similarity-column code paths are both tested.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_similarity_function_returns_correctly_rescored_results(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT id"
                # Tested with both argument orderings of the similarity function to cover both code paths.
                f", similarity_{func_name}(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity1"
                f", similarity_{func_name}({ANN_QUERY_VECTOR_LITERAL}, embedding) AS similarity2"
                f" FROM {table} {order_by} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            for row, d_row in zip(rows, expected):
                assert row.similarity1 == pytest.approx(d_row.expected_similarity, abs=0.01)
                assert row.similarity2 == pytest.approx(d_row.expected_similarity, abs=0.01)
            assert len(rows[0]) == 3

# Verifies that ANN() in SELECT returns computed similarity scores when
# rescoring is enabled. Tests all similarity functions with all rows.
#
# Reproduces SCYLLADB-2212.
@pytest.mark.xfail(reason="ANN() function in SELECT not yet implemented (SCYLLADB-2212)")
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_with_rescoring_ann_function_returns_computed_scores(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity "
                f"FROM {table} {order_by} LIMIT 4"))

            # With rescoring, rows are reordered by actual computed similarity.
            assert [row.id for row in rows] == [d_row.id for d_row in data]
            for row, d_row in zip(rows, data):
                assert row.similarity == pytest.approx(d_row.expected_similarity, abs=0.01)
            assert len(rows[0]) == 2


# Verifies that ANN() in SELECT returns vector store similarity scores when
# rescoring is disabled.
#
# Reproduces SCYLLADB-2212.
@pytest.mark.xfail(reason="ANN() function in SELECT not yet implemented (SCYLLADB-2212)")
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_without_rescoring_ann_function_returns_vs_scores(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name, "rescoring": "false"}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity "
                f"FROM {table} {order_by} LIMIT 2"))

            # Without rescoring, order matches vector store (reversed) and
            # similarity values come directly from the vector store mock.
            expected = list(reversed(data))[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            # reversed_ann_response produces scores: 0.01*N, 0.01*(N-1), ...
            # With 4 rows and LIMIT 2, the first two returned are scores 0.04, 0.03.
            assert rows[0].similarity == pytest.approx(0.04, abs=0.001)
            assert rows[1].similarity == pytest.approx(0.03, abs=0.001)
            assert len(rows[0]) == 2


# rescoring=false, and the similarity function values are correctly computed.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
def test_per_request_rescoring_with_similarity_functions(cql, test_keyspace, vector_store_mock):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name, "rescoring": "false"}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT id"
                f", similarity_{func_name}(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity1"
                f", similarity_{func_name}({ANN_QUERY_VECTOR_LITERAL}, embedding) AS similarity2"
                f", ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, {{'rescoring': 'true'}}) AS ann_similarity"
                f" FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
                f"{{'rescoring': 'true'}}) LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            for row, d_row in zip(rows, expected):
                assert row.similarity1 == pytest.approx(d_row.expected_similarity, abs=0.01)
                assert row.similarity2 == pytest.approx(d_row.expected_similarity, abs=0.01)
                assert row.ann_similarity == pytest.approx(d_row.expected_similarity, abs=0.01)
            assert len(rows[0]) == 4


# Verifies that SELECT * with rescoring returns rows in the correct similarity
# order with correct embedding values. Tests a slightly different processing
# path compared to the explicit column SELECT in test_result_returned_by_vector_store_is_rescored.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_wildcard_select_is_correctly_rescored(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT * FROM {table} {order_by} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            for row, d_row in zip(rows, expected):
                assert list(row.embedding) == pytest.approx(d_row.embedding)
            assert len(rows[0]) == 2


# Verifies that per-request rescoring=true with SELECT * returns rows in the
# correct rescored order with correct embedding values.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
def test_wildcard_select_with_per_request_rescoring(cql, test_keyspace, vector_store_mock):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name, "rescoring": "false"}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT * FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
                f"{{'rescoring': 'true'}}) LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            for row, d_row in zip(rows, expected):
                assert list(row.embedding) == pytest.approx(d_row.embedding)
            assert len(rows[0]) == 2


# Verifies that when the similarity function argument in SELECT differs from the
# ANN ordering vector, the correct similarity values are computed. Uses a
# prepared statement so the argument difference is only visible at execution time.
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_select_similarity_function_other_than_ann_ordering(cql, test_keyspace, vector_store_mock, ann_syntax):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))

        order_by = order_by_ann("embedding", "?", ann_syntax)
        prepared = cql.prepare(
            f"SELECT id, similarity_cosine(embedding, ?) AS similarity FROM {table} "
            f"{order_by} LIMIT 2")
        # Compute similarity to data[1].embedding while ordering by ANN_QUERY_VECTOR.
        rows = list(cql.execute(prepared, [data[1].embedding, ANN_QUERY_VECTOR]))

        expected = data[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        # Similarity is computed against data[1].embedding, not the ANN query vector.
        # id=1 (rescored rank 0): similarity(data[0].embedding, data[1].embedding) = 0.97.
        # id=2 (rescored rank 1): similarity(data[1].embedding, data[1].embedding) = 1 (self-similarity).
        assert rows[0].similarity == pytest.approx(0.97, abs=0.01)
        assert rows[1].similarity == pytest.approx(1, abs=0.01)
        assert len(rows[0]) == 2


# Verifies that rescoring filters out results with invalid similarity scores.
# Inserts rows with a NULL embedding, a NaN component, an Infinity component,
# and a zero vector, then checks that only rows with valid similarity scores
# are returned. The expected set of surviving rows depends on the similarity
# function because some invalid inputs produce valid (non-NaN) scores for
# certain functions (e.g. [INFINITY, 0.1] gives 0 for euclidean, [0,0] gives
# 0.5 for dot_product via the (dot+1)/2 formula).
#
# This also reproduces SCYLLADB-456: zero vectors in the table used to cause
# an error during cosine rescoring; now similarity_cosine returns NaN for them.
#
# Reproduces SCYLLADB-2231
@pytest.mark.xfail(reason="SCYLLADB-2231 rescoring does not yet filter rows with invalid similarity scores (NaN)")
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_filters_invalid_similarity_scores_in_rescored_results(cql, test_keyspace, vector_store_mock, ann_syntax):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            # table already contains valid rows from TEST_DATA (id: 1-4);
            # insert additional rows with invalid similarity scores.
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (17, NULL)")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (16, [0.1, NaN])")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (15, [INFINITY, 0.1])")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (14, [0, 0])")

            # Mock returns a non-existing id (55), the four invalid rows, and id=1 (the only
            # valid row from the mock response). The expected surviving set depends on the
            # similarity function.
            vector_store_mock.set_next_ann_response(200, json.dumps({
                "primary_keys": {"id": [55, 17, 16, 15, 14, 1]},
                "similarity_scores": [0, 0, 0, 0, 0, 0],
            }))
            order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
            rows = list(cql.execute(
                f"SELECT id FROM {table} {order_by} LIMIT 4"))

            if func_name == "euclidean":
                # [INFINITY, 0.1] produces a valid 0 similarity score for euclidean.
                assert [row.id for row in rows] == [1, 14, 15]
            elif func_name == "cosine":
                # [0, 0] produces NaN for cosine; only id=1 survives.
                assert [row.id for row in rows] == [1]
            else:
                # dot_product: [0, 0] produces 0.5 (valid); [INFINITY, ..] is invalid.
                assert [row.id for row in rows] == [1, 14]
            assert len(rows[0]) == 1


# Verifies that per-request rescoring=true correctly filters invalid vectors
# (NULL, NaN, INFINITY, [0,0]) when rescoring is enabled via per-request
# override on an index with rescoring=false.
#
# Reproduces SCYLLADB-184.
@pytest.mark.xfail(reason="per-request rescoring not yet implemented (SCYLLADB-184)")
def test_per_request_rescoring_filters_invalid_vectors(cql, test_keyspace, vector_store_mock):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name, "rescoring": "false"}) as table:
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (17, NULL)")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (16, [0.1, NaN])")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (15, [INFINITY, 0.1])")
            cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (14, [0, 0])")

            vector_store_mock.set_next_ann_response(200, json.dumps({
                "primary_keys": {"id": [55, 17, 16, 15, 14, 1]},
                "similarity_scores": [0, 0, 0, 0, 0, 0],
            }))
            rows = list(cql.execute(
                f"SELECT id FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, "
                f"{{'rescoring': 'true'}}) LIMIT 4"))

            if func_name == "euclidean":
                assert [row.id for row in rows] == [1, 14, 15]
            elif func_name == "cosine":
                assert [row.id for row in rows] == [1]
            else:
                assert [row.id for row in rows] == [1, 14]
            assert len(rows[0]) == 1


# Verifies that a zero ANN query vector does not cause an error and that
# the result set is empty after rescoring filters out the NaN similarity
# scores it produces. Only tested with cosine similarity because zero vectors
# result in NaN only for cosine; other functions produce valid (non-NaN) scores.
#
# similarity_cosine used to throw on zero vectors (SCYLLADB-456); it now
# returns NaN instead. NaN scores should be filtered by rescoring, yielding
# an empty result set.
#
# Reproduces SCYLLADB-2231
@pytest.mark.xfail(reason="SCYLLADB-2231: rescoring does not yet filter NaN similarity scores")
@pytest.mark.parametrize("ann_syntax", ANN_SYNTAX)
def test_rescoring_with_zerovector_query(cql, test_keyspace, vector_store_mock, ann_syntax):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        order_by = order_by_ann("embedding", "[0, 0]", ann_syntax)
        rows = list(cql.execute(
            f"SELECT id FROM {table} {order_by} LIMIT 3"))

        # NaN similarity scores produced by the zero-vector query should be
        # filtered out by rescoring, leaving an empty result set.
        # What is most important - no error is thrown and the query completes successfully
        assert rows == []


# Verifies that ANN() in SELECT produces errors in invalid contexts:
# - Without a matching ANN ORDER BY clause.
# - When the ANN() expression differs from the one in ORDER BY (different
#   vector, options, or column).
#
# Reproduces SCYLLADB-2212.
@pytest.mark.xfail(reason="ANN() function in SELECT not yet implemented (SCYLLADB-2212)")
def test_ann_function_errors(cql, test_keyspace):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data) as table:
        # ANN() in SELECT without ANN ORDER BY is an error.
        with pytest.raises(InvalidRequest):
            cql.execute(
                f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) FROM {table} WHERE id = 1")

        # ANN() in SELECT with a different vector than ORDER BY is an error.
        with pytest.raises(InvalidRequest):
            cql.execute(
                f"SELECT id, ANN(embedding, [0.9, 0.9]) AS similarity "
                f"FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2")

        # ANN() in SELECT with different options than ORDER BY is an error.
        with pytest.raises(InvalidRequest):
            cql.execute(
                f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}, {{'rescoring': 'true'}}) AS similarity "
                f"FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2")

        # ANN() in SELECT with a different column than ORDER BY is an error.
        with pytest.raises(InvalidRequest):
            cql.execute(
                f"SELECT id, ANN(nonexistent, {ANN_QUERY_VECTOR_LITERAL}) AS similarity "
                f"FROM {table} ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2")
