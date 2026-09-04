# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Hybrid search: one query served by a vector search and a full-text search at
# once, its rows sorted by a score fused from the ranks each of them gave.
###############################################################################

import json

import pytest
from cassandra.protocol import InvalidRequest
from test.pylib.skip_types import skip_env

from .util import new_test_table, unique_name

VECTOR = "[0.1, 0.2]"
ROWS = [(0, "the quick brown fox", [0.1, 0.2]),
        (1, "jumped over", [0.2, 0.3]),
        (2, "the lazy dog", [0.3, 0.4]),
        (3, "nothing here", [0.4, 0.5])]


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Hybrid search needs tablets enabled")


@pytest.fixture(scope="function")
def hybrid_table(cql, test_keyspace):
    schema = "id int primary key, content text, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
                    "WITH OPTIONS = {'similarity_function': 'cosine'}")
        for id, content, embedding in ROWS:
            cql.execute(f"INSERT INTO {table} (id, content, embedding) VALUES ({id}, '{content}', {embedding})")
        yield table


def bm25_answer(ids, scores=None):
    if scores is None:
        scores = [1.0 / (i + 1) for i in range(len(ids))]
    return json.dumps({"primary_keys": {"id": ids}, "scores": scores})


def ann_answer(ids, scores=None):
    # The two endpoints name the score differently, which is the only difference here.
    if scores is None:
        scores = [1.0 / (i + 1) for i in range(len(ids))]
    return json.dumps({"primary_keys": {"id": ids}, "similarity_scores": scores})


def rrf(*ranks, k=60):
    return sum(1.0 / (k + rank) for rank in ranks if rank is not None)


def test_hybrid_asks_both_indexes_once_and_unions_their_answers(cql, hybrid_table, vector_store_mock):
    """One request per search, and every row either of them returned comes back, sorted by RRF."""
    table = hybrid_table
    # id 1 is found by both, id 0 only by the vector search, id 2 only by the full-text one.
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([1, 2]))

    rows = list(cql.execute(
        f"SELECT id, ANN_RANK(embedding, {VECTOR}) AS ar, BM25_RANK(content, 'fox') AS br FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    assert len(vector_store_mock.ann_requests) == 1
    assert len(vector_store_mock.bm25_requests) == 1

    expected = sorted([(0, 1, None), (1, 2, 1), (2, None, 2)],
                      key=lambda r: -rrf(r[1], r[2]))
    assert [(row.id, row.ar, row.br) for row in rows] == expected


def test_hybrid_reports_what_each_search_said(cql, hybrid_table, vector_store_mock):
    """Each search's score and rank are reported per row, with nulls where it did not find the row."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([0], scores=[0.9]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([2], scores=[3.5]))

    rows = list(cql.execute(
        f"SELECT id, ANN(embedding, {VECTOR}) AS a, BM25(content, 'fox') AS b FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    by_id = {row.id: row for row in rows}
    assert by_id.keys() == {0, 2}
    assert by_id[0].a == pytest.approx((0.9, 1)) and by_id[0].b == (None, None)
    assert by_id[2].b == pytest.approx((3.5, 1)) and by_id[2].a == (None, None)


def test_hybrid_limit_is_applied_after_fusion(cql, hybrid_table, vector_store_mock):
    """The LIMIT cuts the fused order, not each search's answer."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([1, 0]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0, 2, 3]))

    rows = list(cql.execute(
        f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 2"))

    # id 0 is second in the vector search and first in the full-text one, so RRF gives it
    # 1/62 + 1/61, more than the 1/61 of id 1, which only the vector search found, in first place.
    # ids 2 and 3 score 1/62 and 1/63 and fall outside the limit.
    assert [row.id for row in rows] == [0, 1]


def test_hybrid_highlight(cql, hybrid_table, vector_store_mock):
    """An excerpt is asked for once, and lands only on the rows the full-text search found."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([3]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0]))
    vector_store_mock.set_next_highlight_response(200, json.dumps({"highlights": ["the quick <b>fox</b>", None]}))

    rows = list(cql.execute(
        f"SELECT id, BM25_HIGHLIGHT(content, 'fox') AS excerpt FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    assert len(vector_store_mock.highlight_requests) == 1
    assert {row.id for row in rows} == {0, 3}


@pytest.mark.xfail(reason="orderByClause parses a function call, not an arbitrary expression")
def test_hybrid_weighted_scores(cql, hybrid_table, vector_store_mock):
    """An ordering does not have to be a fusion: any expression over the searches' scores will do.

    Everything below the grammar is ready for this - the ordering is lowered and typed as an
    expression - but ORDER BY takes a function call, so the arithmetic cannot be written yet."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1], scores=[0.1, 0.9]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0, 1], scores=[9.0, 1.0]))

    rows = list(cql.execute(
        f"SELECT id FROM {table} "
        f"ORDER BY 0.5 * ANN_SCORE(embedding, {VECTOR}) + 0.1 * BM25_SCORE(content, 'fox') LIMIT 10"))
    # id 0: 0.05 + 0.9 = 0.95; id 1: 0.45 + 0.1 = 0.55
    assert [row.id for row in rows] == [0, 1]


def test_hybrid_rejects_a_where_clause(cql, hybrid_table):
    """A hybrid query takes no WHERE clause: the two indexes do not filter alike."""
    table = hybrid_table
    with pytest.raises(InvalidRequest, match="does not support a WHERE clause"):
        cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'fox') > 0 "
                    f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10")


def test_ordering_expression_naming_no_search_rejected(cql, hybrid_table):
    table = hybrid_table
    with pytest.raises(InvalidRequest, match="must name at least one search"):
        cql.execute(f"SELECT id FROM {table} ORDER BY RRF(1, 2) LIMIT 10")


def test_hybrid_rejects_a_rescoring_vector_index(cql, test_keyspace):
    """A rescoring index has no rank to fuse by, so ANN() on it is rejected in a hybrid query. The same
    index on its own, ordered by a bare ANN() call, is unaffected."""
    schema = "id int primary key, content text, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = "
                    "{'quantization': 'b1', 'rescoring': 'true', 'similarity_function': 'cosine'}")

        with pytest.raises(InvalidRequest, match=r"ANN\(\) is not supported with a rescoring vector index"):
            cql.execute(f"SELECT id FROM {table} "
                        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10")

        # The same index on its own is unaffected.
        cql.prepare(f"SELECT id FROM {table} ORDER BY ANN(embedding, {VECTOR}) LIMIT 10")
