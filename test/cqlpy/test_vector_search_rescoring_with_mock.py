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

from .test_vector_search_with_vector_store_mock import vector_store_mock, _vector_store_mock_session
from .util import new_test_table


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
def test_oversampling_multiplies_limit_for_vector_store_query(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '3.4'}}")

        cql.execute(f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 3")

        requests = vector_store_mock.ann_requests
        assert requests, "Expected at least one ANN request to the vector store"
        # ceil(3.4 * 3) = ceil(10.2) = 11
        assert json.loads(requests[-1].body)["limit"] == 11


# Verifies that when the vector store returns more results than the CQL LIMIT,
# the output is trimmed to the CQL LIMIT.
def test_oversampled_vector_store_results_are_limited_to_cql_limit(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '2'}}")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (1, [1, 1, 1])")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (2, [1, 1, 1])")

        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"id": [1, 2]},
            "similarity_scores": [0, 0],
        }))
        rows = list(cql.execute(f"SELECT id FROM {table} ORDER BY embedding ANN OF [1, 1, 1] LIMIT 1"))

        assert len(rows) == 1


# Verifies that reversed ANN order is rescored back to expected similarity order.
# Runs for all three similarity functions.
def test_result_returned_by_vector_store_is_rescored(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT id FROM {table} ORDER BY embedding ANN OF {ANN_QUERY_VECTOR_LITERAL} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            assert len(rows[0]) == 1


# Verifies that f32 quantization disables rescoring -- results keep the
# vector store's original (reversed) order.
def test_f32_quantization_disables_rescoring(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"quantization": "f32"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT id FROM {table} ORDER BY embedding ANN OF {ANN_QUERY_VECTOR_LITERAL} LIMIT 2"))

        # Without rescoring the vector store's reversed order is preserved.
        expected = list(reversed(data))[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert len(rows[0]) == 1


# Verifies that a similarity function in the SELECT clause is computed with
# rescored (reordered) results.
# Kept separate from test_result_returned_by_vector_store_is_rescored to ensure
# the similarity-column and non-similarity-column code paths are both tested.
def test_similarity_function_returns_correctly_rescored_results(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            # Tested with both argument orderings of the similarity function to cover both code paths.
            for func_args in [f"embedding, {ANN_QUERY_VECTOR_LITERAL}",
                              f"{ANN_QUERY_VECTOR_LITERAL}, embedding"]:
                vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
                rows = list(cql.execute(
                    f"SELECT id, similarity_{func_name}({func_args}) AS similarity FROM {table} "
                    f"ORDER BY embedding ANN OF {ANN_QUERY_VECTOR_LITERAL} LIMIT 2"))

                expected = data[:2]
                assert [row.id for row in rows] == [d_row.id for d_row in expected]
                for row, d_row in zip(rows, expected):
                    assert row.similarity == pytest.approx(d_row.expected_similarity, abs=0.01)
                assert len(rows[0]) == 2
