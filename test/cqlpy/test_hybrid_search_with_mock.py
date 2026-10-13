# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Hybrid search: one query served by several external searches at once, its
# rows sorted by a score fused from the ranks each of them gave.
###############################################################################

import json
import re
from contextlib import contextmanager

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest
from test.pylib.skip_types import skip_env

from .util import new_function, new_test_table, unique_name

VECTOR = "[0.1, 0.2]"
ROWS = [(0, "the quick brown fox", [0.1, 0.2]),
        (1, "jumped over", [0.2, 0.3]),
        (2, "the lazy dog", [0.3, 0.4]),
        (3, "nothing here", [0.4, 0.5])]


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Hybrid search needs tablets enabled")


@contextmanager
def new_hybrid_table(cql, keyspace, vector_options="'similarity_function': 'cosine'"):
    schema = "id int primary key, content text, embedding vector<float, 2>"
    with new_test_table(cql, keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
                    f"WITH OPTIONS = {{{vector_options}}}")
        for id, content, embedding in ROWS:
            cql.execute(f"INSERT INTO {table} (id, content, embedding) VALUES ({id}, '{content}', {embedding})")
        yield table


@pytest.fixture(scope="function")
def hybrid_table(cql, test_keyspace):
    with new_hybrid_table(cql, test_keyspace) as table:
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
    # Without oversampling, each search is asked for the LIMIT.
    assert json.loads(vector_store_mock.ann_requests[0].body)["limit"] == 10
    assert json.loads(vector_store_mock.bm25_requests[0].body)["limit"] == 10

    expected = sorted([(0, 1, None), (1, 2, 1), (2, None, 2)],
                      key=lambda r: -rrf(r[1], r[2]))
    assert [(row.id, row.ar, row.br) for row in rows] == expected


def test_hybrid_reports_what_each_search_said(cql, hybrid_table, vector_store_mock):
    """Each search's score and rank are reported per row, with nulls where it did not find the row.
    id 1 is found by both, and gets each search's own score and rank."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1], scores=[0.9, 0.8]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([2, 1], scores=[3.5, 2.5]))

    rows = list(cql.execute(
        f"SELECT id, ANN(embedding, {VECTOR}) AS a, BM25(content, 'fox') AS b FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    by_id = {row.id: row for row in rows}
    assert by_id.keys() == {0, 1, 2}
    assert by_id[0].a == pytest.approx((0.9, 1)) and by_id[0].b == (None, None)
    assert by_id[1].a == pytest.approx((0.8, 2)) and by_id[1].b == pytest.approx((2.5, 2))
    assert by_id[2].b == pytest.approx((3.5, 1)) and by_id[2].a == (None, None)


def test_hybrid_score_that_is_not_a_number_is_no_hit(cql, hybrid_table, vector_store_mock):
    """A score that is not a finite number is no hit. A row another search found still comes back,
    with nulls for the search that scored it so; a row no search has a usable score for is left
    out. JSON has no literal for infinity, but 1e39 does not fit in a float."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1], scores=[1e39, -1e39]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0], scores=[5.0]))

    rows = list(cql.execute(
        f"SELECT id, ANN_RANK(embedding, {VECTOR}) AS ar, BM25_RANK(content, 'fox') AS br FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    assert [(row.id, row.ar, row.br) for row in rows] == [(0, None, 1)]


def test_hybrid_cuts_each_answer_to_the_limit_before_fusion(cql, test_keyspace, vector_store_mock):
    """An oversampling vector index is asked for LIMIT x oversampling keys, but only the first LIMIT
    of them are fused, as a Vector Store doing the oversampling itself would answer. The LIMIT then
    cuts the fused order."""
    with new_hybrid_table(cql, test_keyspace, "'similarity_function': 'cosine', 'oversampling': '2.0'") as table:
        vector_store_mock.set_next_ann_response(200, ann_answer([1, 3, 2, 0]))
        vector_store_mock.set_next_bm25_response(200, bm25_answer([3, 2]))

        rows = list(cql.execute(
            f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 2"))

    # The vector search is asked for the LIMIT times the oversampling, the full-text one for the LIMIT.
    assert [json.loads(r.body)["limit"] for r in vector_store_mock.ann_requests] == [4]
    assert [json.loads(r.body)["limit"] for r in vector_store_mock.bm25_requests] == [2]
    # The vector search's answer is cut to ids 1 and 3. id 3, found by both, scores 1/62 + 1/61; id 1
    # 1/61; id 2, whose vector rank 3 was cut, only its full-text 1/62. Without the cut, id 2 would
    # score 1/63 + 1/62 and come second. The fused order is 3, 1, not the order they arrived in.
    assert [row.id for row in rows] == [3, 1]


def test_hybrid_highlight(cql, hybrid_table, vector_store_mock):
    """An excerpt is asked for once, and lands only on the rows the full-text search found."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([3]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0]))
    vector_store_mock.set_next_highlight_response(200, json.dumps({"highlights": ["the quick brown <b>fox</b>"]}))

    rows = list(cql.execute(
        f"SELECT id, BM25_HIGHLIGHT(content, 'fox') AS excerpt FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    # Only the text of id 0, the row the full-text search returned, is sent. id 3, which only the
    # vector search returned, gets a null, as BM25() is null for it too.
    assert [json.loads(r.body)["documents"] for r in vector_store_mock.highlight_requests] == [["the quick brown fox"]]
    assert {row.id: row.excerpt for row in rows} == {0: "the quick brown <b>fox</b>", 3: None}


def test_hybrid_fails_when_one_search_fails(cql, hybrid_table, vector_store_mock):
    """One search's error fails the whole query; the other's answer is not returned on its own."""
    table = hybrid_table
    query = f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"

    vector_store_mock.set_next_ann_response(404, "index does not exist")
    vector_store_mock.set_next_bm25_response(200, bm25_answer([1, 2]))
    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(query)

    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(404, "index does not exist")
    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(query)


def test_hybrid_does_not_wait_for_the_other_search_once_one_fails(cql, hybrid_table, vector_store_mock):
    """The first failure is the query's error, and the requests still running are aborted rather than
    waited for. Waiting would report the same error, only once the delayed answer came in - by then
    the request has timed out."""
    table = hybrid_table
    # Far longer than a failing query takes, even in debug mode, and far shorter than the delay.
    timeout = 10
    vector_store_mock.set_ann_response_delay(6 * timeout)
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(404, "index does not exist")

    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10",
                    timeout=timeout)


def test_hybrid_does_not_wait_for_the_other_search_once_one_throws(cql, hybrid_table, vector_store_mock):
    """A request can also fail with an exception rather than an error the client reports, here on a
    key of the wrong type in its answer. It aborts the requests still running the same way."""
    table = hybrid_table
    # Far longer than a failing query takes, even in debug mode, and far shorter than the delay.
    timeout = 10
    vector_store_mock.set_ann_response_delay(6 * timeout)
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer(["not a number"]))

    # The exception is a server error, which the driver re-raises as NoHostAvailable.
    with pytest.raises(NoHostAvailable, match="marshaling error"):
        cql.execute(f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10",
                    timeout=timeout)


def test_hybrid_rejects_a_where_clause(cql, hybrid_table):
    """A hybrid query takes no WHERE clause: the two indexes do not filter alike."""
    table = hybrid_table
    with pytest.raises(InvalidRequest, match="does not support a WHERE clause"):
        cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'fox') > 0 "
                    f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10")


def test_hybrid_rejects_a_where_clause_on_an_unsearched_column(cql, two_of_each_table):
    """The WHERE clause is what is rejected, not the column it names: no search on that column in
    ORDER BY would make it acceptable."""
    table = two_of_each_table
    with pytest.raises(InvalidRequest, match="does not support a WHERE clause"):
        cql.execute(f"SELECT id FROM {table} WHERE BM25(title, 'fox') > 0 "
                    f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10")


def test_ordering_expression_naming_no_search_rejected(cql, hybrid_table):
    table = hybrid_table
    with pytest.raises(InvalidRequest, match="must name at least one search"):
        cql.execute(f"SELECT id FROM {table} ORDER BY RRF(1, 2) LIMIT 10")


def test_hybrid_rejects_a_rescoring_vector_index(cql, test_keyspace):
    """A rescoring index orders the rows only by ANN() or ANN_SCORE() called directly, so it takes
    no part in a fusion, through ANN() or through ANN_SCORE(). The same index on its own is
    unaffected."""
    schema = "id int primary key, content text, embedding vector<float, 2>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = "
                    "{'quantization': 'b1', 'rescoring': 'true', 'similarity_function': 'cosine'}")

        body = "(a float, b tuple<float, int>) CALLED ON NULL INPUT RETURNS float LANGUAGE lua AS 'return 0'"
        with new_function(cql, test_keyspace, body) as fuse:
            for ordering in [f"RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox'))",
                             f"{test_keyspace}.{fuse}(ANN_SCORE(embedding, {VECTOR}), BM25(content, 'fox'))"]:
                with pytest.raises(InvalidRequest, match=r"ORDER BY supports only ANN\(\) or ANN_SCORE\(\) called directly"):
                    cql.prepare(f"SELECT id FROM {table} ORDER BY {ordering} LIMIT 10")

        # The same index on its own is unaffected.
        cql.prepare(f"SELECT id FROM {table} ORDER BY ANN(embedding, {VECTOR}) LIMIT 10")


@pytest.fixture(scope="function")
def two_of_each_table(cql, test_keyspace):
    """A table with two full-text-indexed and two vector-indexed columns."""
    schema = ("id int primary key, content text, title text, "
              "embedding vector<float, 2>, embedding2 vector<float, 2>")
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'fulltext_index'")
        for column in ["embedding", "embedding2"]:
            cql.execute(f"CREATE CUSTOM INDEX ON {table}({column}) USING 'vector_index' "
                        "WITH OPTIONS = {'similarity_function': 'cosine'}")
        for id, content, embedding in ROWS:
            cql.execute(f"INSERT INTO {table} (id, content, title, embedding, embedding2) "
                        f"VALUES ({id}, '{content}', '{content}', {embedding}, {embedding})")
        yield table


# A search is identified by its family and its column, so a query is not limited to one search per
# family: two columns of one kind are two searches, and each gets its own request.
def test_two_full_text_searches_run_side_by_side(cql, two_of_each_table, vector_store_mock):
    table = two_of_each_table
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0, 1]))

    rows = list(cql.execute(
        f"SELECT id FROM {table} ORDER BY RRF(BM25(content, 'fox'), BM25(title, 'dog')) LIMIT 10"))

    reqs = vector_store_mock.bm25_requests
    assert len(reqs) == 2
    assert sorted(json.loads(r.body)["query"] for r in reqs) == ["dog", "fox"]
    assert len({r.path for r in reqs}) == 2
    # Both searches answered with the same two keys, so both rows scored twice, id 0 ahead of id 1.
    assert [row.id for row in rows] == [0, 1]


def test_two_vector_searches_run_side_by_side(cql, two_of_each_table, vector_store_mock):
    table = two_of_each_table
    vector_store_mock.set_next_ann_response(200, ann_answer([2, 3]))

    rows = list(cql.execute(
        f"SELECT id FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), ANN(embedding2, [0.9, 0.9])) LIMIT 10"))

    reqs = vector_store_mock.ann_requests
    assert len(reqs) == 2
    assert sorted(json.loads(r.body)["vector"] for r in reqs) == [[0.1, 0.2], [0.9, 0.9]]
    assert len({r.path for r in reqs}) == 2
    assert [row.id for row in rows] == [2, 3]


def test_three_searches_are_fused(cql, two_of_each_table, vector_store_mock):
    """Fusion is not limited to a pair: every search the ordering names gets a request."""
    table = two_of_each_table
    vector_store_mock.set_next_ann_response(200, ann_answer([3]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([0]))

    rows = list(cql.execute(
        f"SELECT id FROM {table} ORDER BY "
        f"RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox'), BM25(title, 'dog')) LIMIT 10"))

    assert len(vector_store_mock.ann_requests) == 1
    assert len(vector_store_mock.bm25_requests) == 2
    # id 0 was found by both full-text searches, id 3 only by the vector one.
    assert [row.id for row in rows] == [0, 3]


# One column carries one search, so two calls naming it are the same search and have to agree.
def test_two_full_text_calls_on_one_column_must_agree(cql, hybrid_table):
    table = hybrid_table
    with pytest.raises(InvalidRequest, match=re.escape(
            "BM25() in ORDER BY must use the same search term as the other BM25 calls on the same column")):
        cql.execute(f"SELECT id FROM {table} "
                    f"ORDER BY RRF(BM25(content, 'fox'), BM25(content, 'dog')) LIMIT 10")


def test_two_vector_calls_on_one_column_must_agree(cql, hybrid_table):
    table = hybrid_table
    with pytest.raises(InvalidRequest, match=re.escape(
            "ANN() in ORDER BY must use the same query vector as the other ANN calls on the same column")):
        cql.execute(f"SELECT id FROM {table} "
                    f"ORDER BY RRF(ANN(embedding, {VECTOR}), ANN(embedding, [0.9, 0.9])) LIMIT 10")


def test_two_calls_on_one_column_are_compared_once_bound(cql, hybrid_table, vector_store_mock):
    """Bind markers defer the comparison to execution, which reports the clause the calls were in."""
    table = hybrid_table
    stmt = cql.prepare(f"SELECT id FROM {table} ORDER BY RRF(ANN(embedding, ?), ANN(embedding, ?)) LIMIT 10")

    with pytest.raises(InvalidRequest, match=re.escape(
            "ANN() in ORDER BY must use the same query vector as the other ANN calls on the same column")):
        cql.execute(stmt, [[0.1, 0.2], [0.9, 0.9]])
    assert vector_store_mock.ann_requests == []

    # The same statement with one vector bound twice is a single search, and runs.
    vector_store_mock.set_next_ann_response(200, ann_answer([1]))
    assert [row.id for row in cql.execute(stmt, [[0.1, 0.2], [0.1, 0.2]])] == [1]
    assert len(vector_store_mock.ann_requests) == 1


# The fusion is an ordinary function call, so a user-defined function can rank the rows as well,
# given each search's (score, rank) tuple, its elements null where the search did not return the row.
def test_hybrid_orders_by_a_user_defined_function(cql, hybrid_table, vector_store_mock):
    table = hybrid_table
    keyspace = table.split(".")[0]
    # Three times the vector search's reciprocal rank plus the full-text one's.
    body = ("(a tuple<float, int>, b tuple<float, int>) CALLED ON NULL INPUT RETURNS float LANGUAGE lua AS "
            "'local function w(hit) if hit[2] == nil then return 0 end return 1 / hit[2] end "
            "return 3 * w(a) + w(b)'")
    with new_function(cql, keyspace, body) as fuse:
        vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
        vector_store_mock.set_next_bm25_response(200, bm25_answer([1, 2]))

        rows = list(cql.execute(
            f"SELECT id FROM {table} "
            f"ORDER BY {keyspace}.{fuse}(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

        # id 0: 3/1 = 3; id 1: 3/2 + 1/1 = 2.5; id 2: 1/2 = 0.5.
        assert [row.id for row in rows] == [0, 1, 2]


def test_hybrid_ordering_must_be_a_score(cql, hybrid_table):
    table = hybrid_table
    keyspace = table.split(".")[0]
    body = "(a tuple<float, int>) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 1'"
    with new_function(cql, keyspace, body) as not_a_score:
        with pytest.raises(InvalidRequest, match=rf"{keyspace}\.{not_a_score}\(\) cannot be used as a scoring function in ORDER BY"):
            cql.execute(f"SELECT id FROM {table} ORDER BY {keyspace}.{not_a_score}(ANN(embedding, {VECTOR})) LIMIT 10")


def test_hybrid_on_a_clustered_table(cql, test_keyspace, vector_store_mock):
    """Rows of one partition are told apart by their clustering key, and the two answers interleave
    the partitions in different orders."""
    schema = "p int, c int, content text, embedding vector<float, 2>, PRIMARY KEY (p, c)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' "
                    "WITH OPTIONS = {'similarity_function': 'cosine'}")
        for p, c in [(1, 1), (1, 2), (2, 1), (3, 1)]:
            cql.execute(f"INSERT INTO {table} (p, c, content, embedding) VALUES ({p}, {c}, 'fox', [0.1, 0.2])")

        def keys(pcs):
            return {"p": [p for p, _ in pcs], "c": [c for _, c in pcs]}
        vector_store_mock.set_next_ann_response(200, json.dumps(
            {"primary_keys": keys([(1, 1), (2, 1), (1, 2)]), "similarity_scores": [0.9, 0.8, 0.7]}))
        vector_store_mock.set_next_bm25_response(200, json.dumps(
            {"primary_keys": keys([(1, 2), (1, 1), (3, 1)]), "scores": [3.0, 2.0, 1.0]}))

        rows = list(cql.execute(
            f"SELECT p, c, ANN_RANK(embedding, {VECTOR}) AS ar, BM25_RANK(content, 'fox') AS br FROM {table} "
            f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

        expected = sorted([(1, 1, 1, 2), (2, 1, 2, None), (1, 2, 3, 1), (3, 1, None, 3)],
                          key=lambda r: -rrf(r[2], r[3]))
        assert [tuple(row) for row in rows] == expected


def test_hybrid_select_json(cql, hybrid_table, vector_store_mock):
    """SELECT JSON folds the selectors into one column; the fused score stays hidden beside it."""
    table = hybrid_table
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([1, 2]))

    rows = list(cql.execute(
        f"SELECT JSON id, ANN_RANK(embedding, {VECTOR}) AS ar FROM {table} "
        f"ORDER BY RRF(ANN(embedding, {VECTOR}), BM25(content, 'fox')) LIMIT 10"))

    assert [len(row) for row in rows] == [1, 1, 1]
    assert [json.loads(row[0]) for row in rows] == [{"id": 1, "ar": 2}, {"id": 0, "ar": 1}, {"id": 2, "ar": None}]


def test_hybrid_with_bind_markers(cql, hybrid_table, vector_store_mock):
    """Every search argument and the LIMIT can be bound, and a selector's bound value is checked
    against the search it names before any request is sent."""
    table = hybrid_table
    stmt = cql.prepare(f"SELECT id, BM25_RANK(content, ?) AS br FROM {table} "
                       f"ORDER BY RRF(ANN(embedding, ?), BM25(content, ?)) LIMIT ?")
    vector_store_mock.set_next_ann_response(200, ann_answer([0, 1]))
    vector_store_mock.set_next_bm25_response(200, bm25_answer([1, 2]))

    rows = list(cql.execute(stmt, ["fox", [0.1, 0.2], "fox", 2]))

    assert [(row.id, row.br) for row in rows] == [(1, 1), (0, None)]
    assert json.loads(vector_store_mock.ann_requests[0].body)["vector"] == pytest.approx([0.1, 0.2])
    assert json.loads(vector_store_mock.bm25_requests[0].body)["query"] == "fox"

    with pytest.raises(InvalidRequest, match=re.escape(
            "BM25_RANK() in SELECT must match a BM25 search in ORDER BY, with the same column and search term; the search term differs")):
        cql.execute(stmt, ["dog", [0.1, 0.2], "fox", 2])
    assert len(vector_store_mock.ann_requests) == 1
    assert len(vector_store_mock.bm25_requests) == 1


def test_each_search_gets_its_own_answer(cql, test_keyspace, vector_store_mock):
    """Four searches, two of each family, each answering differently: every rank a row reports comes
    from the search it names, not from another index of the same family."""
    schema = ("id int primary key, content text, title text, "
              "embedding vector<float, 2>, embedding2 vector<float, 2>")
    with new_test_table(cql, test_keyspace, schema) as table:
        index = {column: unique_name() for column in ["content", "title", "embedding", "embedding2"]}
        for column in ["content", "title"]:
            cql.execute(f"CREATE CUSTOM INDEX {index[column]} ON {table}({column}) USING 'fulltext_index'")
        for column in ["embedding", "embedding2"]:
            cql.execute(f"CREATE CUSTOM INDEX {index[column]} ON {table}({column}) USING 'vector_index' "
                        "WITH OPTIONS = {'similarity_function': 'cosine'}")
        for id, content, embedding in ROWS:
            cql.execute(f"INSERT INTO {table} (id, content, title, embedding, embedding2) "
                        f"VALUES ({id}, '{content}', '{content}', {embedding}, {embedding})")
        vector_store_mock.set_next_ann_response(200, ann_answer([0]), index=index["embedding"])
        vector_store_mock.set_next_ann_response(200, ann_answer([1, 0]), index=index["embedding2"])
        vector_store_mock.set_next_bm25_response(200, bm25_answer([2, 1, 0]), index=index["content"])
        vector_store_mock.set_next_bm25_response(200, bm25_answer([3, 2, 1, 0]), index=index["title"])

        rows = list(cql.execute(
            f"SELECT id, ANN_RANK(embedding, {VECTOR}) AS e1, ANN_RANK(embedding2, [0.9, 0.9]) AS e2, "
            f"BM25_RANK(content, 'fox') AS c, BM25_RANK(title, 'dog') AS t FROM {table} "
            f"ORDER BY RRF(ANN(embedding, {VECTOR}), ANN(embedding2, [0.9, 0.9]), "
            f"BM25(content, 'fox'), BM25(title, 'dog')) LIMIT 10"))

    # id 0 is found by all four searches, id 1 by three, id 2 by two, id 3 by one.
    assert [tuple(row) for row in rows] == [
        (0, 1, 2, 3, 4), (1, None, 1, 2, 3), (2, None, None, 1, 2), (3, None, None, None, 1)]
