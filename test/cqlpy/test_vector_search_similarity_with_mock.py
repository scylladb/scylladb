# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for the similarity score of a vector search: the oversampling and
# rescoring that decide which rows come back and in what order, and the score
# ANN() reports for each of them.
#
# These tests use the vector store mock infrastructure from vector_store_mock.py.
# A test is named for the index configuration it covers, the score coming from a
# different place in each: recomputed by the coordinator when the index rescores,
# and reported by the vector store when it does not.
###############################################################################

import json
from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from cassandra.protocol import InvalidRequest

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

def order_by_ann(column, query_vector, syntax):
    """Render an ANN ORDER BY clause in the requested syntax.

    "ann_of" is the Cassandra-compatible 'column ANN OF query_vector';
    "ann_function" is the ScyllaDB function syntax 'ANN(column, query_vector)'.
    The two are equivalent, so every ANN test should pass with either."""
    if syntax == "ann_of":
        return f"ORDER BY {column} ANN OF {query_vector}"
    if syntax == "ann_function":
        return f"ORDER BY ANN({column}, {query_vector})"
    raise ValueError(f"unknown ANN syntax: {syntax}")


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
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_oversampling_multiplies_limit_for_vector_store_query(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", "[0.1, 0.2, 0.3]", ann_syntax)
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '3.4'}}")

        cql.execute(f"SELECT * FROM {table} {ann_order_by} LIMIT 3")

        requests = vector_store_mock.ann_requests
        assert requests, "Expected at least one ANN request to the vector store"
        # ceil(3.4 * 3) = ceil(10.2) = 11
        assert json.loads(requests[-1].body)["limit"] == 11


# Verifies that when the vector store returns more results than the CQL LIMIT,
# the output is trimmed to the CQL LIMIT.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_oversampled_vector_store_results_are_limited_to_cql_limit(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", "[1, 1, 1]", ann_syntax)
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '2'}}")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (1, [1, 1, 1])")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (2, [1, 1, 1])")

        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"id": [1, 2]},
            "similarity_scores": [0, 0],
        }))
        rows = list(cql.execute(f"SELECT id FROM {table} {ann_order_by} LIMIT 1"))

        assert len(rows) == 1


# Verifies that reversed ANN order is rescored back to expected similarity order.
# Runs for all three similarity functions.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_result_returned_by_vector_store_is_rescored(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT id FROM {table} {ann_order_by} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            assert len(rows[0]) == 1


# Verifies that f32 quantization disables rescoring -- results keep the
# vector store's original (reversed) order.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_f32_quantization_disables_rescoring(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"quantization": "f32"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT id FROM {table} {ann_order_by} LIMIT 2"))

        # Without rescoring the vector store's reversed order is preserved.
        expected = list(reversed(data))[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert len(rows[0]) == 1


# Verifies that a similarity function in the SELECT clause is computed with
# rescored (reordered) results.
# Kept separate from test_result_returned_by_vector_store_is_rescored to ensure
# the similarity-column and non-similarity-column code paths are both tested.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_similarity_function_returns_correctly_rescored_results(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            # Tested with both argument orderings of the similarity function to cover both code paths.
            for func_args in [f"embedding, {ANN_QUERY_VECTOR_LITERAL}",
                              f"{ANN_QUERY_VECTOR_LITERAL}, embedding"]:
                vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
                rows = list(cql.execute(
                    f"SELECT id, similarity_{func_name}({func_args}) AS similarity FROM {table} "
                    f"{ann_order_by} LIMIT 2"))

                expected = data[:2]
                assert [row.id for row in rows] == [d_row.id for d_row in expected]
                for row, d_row in zip(rows, expected):
                    assert row.similarity == pytest.approx(d_row.expected_similarity, abs=0.01)
                assert len(rows[0]) == 2


# Verifies that SELECT * with rescoring returns rows in the correct similarity
# order with correct embedding values. Tests a slightly different processing
# path compared to the explicit column SELECT in test_result_returned_by_vector_store_is_rescored.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_wildcard_select_is_correctly_rescored(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
    for func_name, data in TEST_DATA.items():
        with rescoring_test_table(cql, test_keyspace, data,
                extra_options={"similarity_function": func_name}) as table:
            vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
            rows = list(cql.execute(
                f"SELECT * FROM {table} {ann_order_by} LIMIT 2"))

            expected = data[:2]
            assert [row.id for row in rows] == [d_row.id for d_row in expected]
            for row, d_row in zip(rows, expected):
                assert list(row.embedding) == pytest.approx(d_row.embedding)
            assert len(rows[0]) == 2


# Verifies that when the similarity function argument in SELECT differs from the
# ANN ordering vector, the correct similarity values are computed. Uses a
# prepared statement so the argument difference is only visible at execution time.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_select_similarity_function_other_than_ann_ordering(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", "?", ann_syntax)
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))

        prepared = cql.prepare(
            f"SELECT id, similarity_cosine(embedding, ?) AS similarity FROM {table} "
            f"{ann_order_by} LIMIT 2")
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
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_filters_invalid_similarity_scores_in_rescored_results(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
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
            rows = list(cql.execute(
                f"SELECT id FROM {table} {ann_order_by} LIMIT 4"))

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
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_rescoring_with_zerovector_query(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", "[0, 0]", ann_syntax)
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT id FROM {table} {ann_order_by} LIMIT 3"))

        # NaN similarity scores produced by the zero-vector query should be
        # filtered out by rescoring, leaving an empty result set.
        # What is most important - no error is thrown and the query completes successfully
        assert rows == []


# ---------------------------------------------------------------------------
# ANN() in the SELECT clause
# ---------------------------------------------------------------------------
#
# ANN() selected reports the similarity of each returned row: the one the Vector
# Store reported alongside the candidate key, injected per row.

# Verifies that without rescoring, ANN() in SELECT reports the Vector Store's own
# scores, and the Vector Store's own order is kept.
@pytest.mark.parametrize("ann_syntax", ["ann_of", "ann_function"])
def test_without_rescoring_ann_function_returns_vs_scores(cql, test_keyspace, vector_store_mock, skip_without_tablets, ann_syntax):
    ann_order_by = order_by_ann("embedding", ANN_QUERY_VECTOR_LITERAL, ann_syntax)
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"rescoring": "false"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity FROM {table} "
            f"{ann_order_by} LIMIT 2"))

        # Without rescoring the mock's reversed order is preserved, and the scores
        # are the ones it reported: 0.01*N, 0.01*(N-1), ...
        expected = list(reversed(data))[:2]
        assert [row.id for row in rows] == [d_row.id for d_row in expected]
        assert rows[0].similarity == pytest.approx(0.04, abs=0.001)
        assert rows[1].similarity == pytest.approx(0.03, abs=0.001)
        assert len(rows[0]) == 2


# Verifies that a query selecting nothing but ANN() works, and that the primary-key
# columns fetched to match the scores to their rows do not leak to the client.
def test_without_rescoring_ann_function_alone_hides_pk_columns(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"rescoring": "false"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(
            f"SELECT ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity FROM {table} "
            f"ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2"))

        assert rows[0]._fields == ("similarity",)
        assert [row.similarity for row in rows] == pytest.approx([0.04, 0.03], abs=0.001)


# Verifies that ANN() may be selected more than once, and nested in an expression.
# Every occurrence reports the same score, so they are all served by one slot.
def test_without_rescoring_ann_function_repeated_and_nested(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"rescoring": "false"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        ann = f"ANN(embedding, {ANN_QUERY_VECTOR_LITERAL})"
        rows = list(cql.execute(
            f"SELECT id, {ann} AS s1, {ann} AS s2, CAST({ann} AS double) AS s3 FROM {table} "
            f"ORDER BY {ann} LIMIT 2"))

        assert [row.id for row in rows] == [4, 3]
        for row, score in zip(rows, [0.04, 0.03]):
            assert row.s1 == pytest.approx(score, abs=0.001)
            assert row.s2 == pytest.approx(score, abs=0.001)
            assert row.s3 == pytest.approx(score, abs=0.001)


# Verifies that an unaliased ANN() selector is named after the call the user wrote,
# and not after whatever it was lowered to.
def test_without_rescoring_ann_function_unaliased_column_name(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"rescoring": "false"}) as table:
        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        res = cql.execute(
            f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) FROM {table} "
            f"ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2")

        assert res.column_names == ["id", f"system.ann(embedding, {ANN_QUERY_VECTOR_LITERAL})"]


# Verifies that on a table with a clustering key each row gets the score the Vector Store
# reported for its own (pk, ck), skipping entries whose row is no longer in the base table.
# The provider matches rows to the response by primary key, and only a clustering key tells
# the rows of one partition apart.
def test_without_rescoring_ann_function_with_clustering_key(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk int, ck int, embedding vector<float, 2>, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
                    f"WITH OPTIONS = {{'rescoring': 'false'}}")

        # The index can lag the base table, so (7, 70) comes back with no row behind it. It has
        # to be passed over, leaving the rows around it with the scores reported for them.
        response = [(1, 10, 0.9), (1, 20, 0.8), (7, 70, 0.75), (2, 30, 0.7)]
        expected = [(pk, ck, score) for pk, ck, score in response if pk != 7]
        for pk, ck, _ in expected:
            cql.execute(f"INSERT INTO {table} (pk, ck, embedding) VALUES ({pk}, {ck}, {ANN_QUERY_VECTOR_LITERAL})")

        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"pk": [pk for pk, _, _ in response], "ck": [ck for _, ck, _ in response]},
            "similarity_scores": [score for _, _, score in response],
        }))

        rows = list(cql.execute(
            f"SELECT pk, ck, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity FROM {table} "
            f"ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT {len(response)}"))

        assert [(row.pk, row.ck) for row in rows] == [(pk, ck) for pk, ck, _ in expected]
        for row, (_, _, score) in zip(rows, expected):
            assert row.similarity == pytest.approx(score)


# Verifies that a bind-marker query vector is accepted when SELECT and ORDER BY are given the
# same value and rejected when they are not, which is only visible once the values arrive:
#   Case 1: a marker in both clauses.
#   Case 2: a marker in SELECT only, the ordering a literal.
#   Case 3: a marker in ORDER BY only, the SELECT a literal.
def test_without_rescoring_ann_function_bind_marker(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    data = TEST_DATA["cosine"]
    with rescoring_test_table(cql, test_keyspace, data,
            extra_options={"rescoring": "false"}) as table:
        # Case 1
        stmt = cql.prepare(
            f"SELECT id, ANN(embedding, ?) AS similarity FROM {table} ORDER BY ANN(embedding, ?) LIMIT 2")

        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        rows = list(cql.execute(stmt, [ANN_QUERY_VECTOR, ANN_QUERY_VECTOR]))
        assert [row.id for row in rows] == [4, 3]

        with pytest.raises(InvalidRequest, match="same query vector"):
            cql.execute(stmt, [[0.9, 0.9], ANN_QUERY_VECTOR])

        # Case 2: the call the marker was written in is the one replaced with a temporary, so the
        # copy kept for this check is all that mentions it.
        stmt = cql.prepare(
            f"SELECT id, ANN(embedding, ?) AS similarity FROM {table} "
            f"ORDER BY ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) LIMIT 2")

        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        assert [row.id for row in cql.execute(stmt, [ANN_QUERY_VECTOR])] == [4, 3]

        with pytest.raises(InvalidRequest, match="same query vector"):
            cql.execute(stmt, [[0.9, 0.9]])

        # Case 3: here the ordering's own expression is what mentions the marker.
        stmt = cql.prepare(
            f"SELECT id, ANN(embedding, {ANN_QUERY_VECTOR_LITERAL}) AS similarity FROM {table} "
            f"ORDER BY ANN(embedding, ?) LIMIT 2")

        vector_store_mock.set_next_ann_response(200, reversed_ann_response(data))
        assert [row.id for row in cql.execute(stmt, [ANN_QUERY_VECTOR])] == [4, 3]

        with pytest.raises(InvalidRequest, match="same query vector"):
            cql.execute(stmt, [[0.9, 0.9]])
