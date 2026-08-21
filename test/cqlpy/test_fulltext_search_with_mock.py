# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for full-text search (FTS) query execution.
#
# These tests use the shared Vector Store mock from vector_store_mock.py to
# verify that Scylla correctly translates CQL queries with BM25 ordering into
# HTTP POST requests to the `/bm25` endpoint of the Vector Store service,
# and returns results in BM25 rank order.
###############################################################################

import json

import pytest
from cassandra.protocol import InvalidRequest
from test.pylib.skip_types import skip_env
from cassandra.query import SimpleStatement
from http import HTTPStatus

from .util import new_test_table, unique_name

NUM_ROWS = 5
RESPONSE_PK_REVERSED = list(reversed(range(NUM_ROWS)))


def bm25_response(ids, scores=None):
    if scores is None:
        scores = [1.0 / (i + 1) for i in range(len(ids))]
    return json.dumps({"primary_keys": {"id": ids}, "scores": scores})


def highlight_response(fragments):
    """A `/highlight` reply. Entries are positional and may be None, meaning no fragment for that document."""
    return json.dumps({"highlights": fragments})


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Full-Text Search needs tablets enabled")


@pytest.fixture(scope="module")
def fts_table(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, content text)")
    cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(content) USING 'fulltext_index'")
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, content) VALUES ({i}, 'hello')")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="function")
def fts_setup_with_mock(fts_table, vector_store_mock):
    table, idx = fts_table
    vector_store_mock.set_next_bm25_response(200, bm25_response(RESPONSE_PK_REVERSED))
    return table, idx


def test_fts_basic_query_executes(cql, fts_setup_with_mock):
    """A basic BM25 query with a string literal should execute end-to-end against the vector store, returning rows in BM25 rank order."""
    table, _ = fts_setup_with_mock

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}"))
    assert [r.id for r in rows] == RESPONSE_PK_REVERSED


def test_fts_bind_markers_execute(cql, fts_setup_with_mock):
    """A BM25 query with bind markers (?) should execute end-to-end against the vector store."""
    table, _ = fts_setup_with_mock

    stmt = cql.prepare(f"SELECT id FROM {table} WHERE BM25(content, ?) > 0 ORDER BY BM25(content, ?) LIMIT {NUM_ROWS}")
    rows = list(cql.execute(stmt, ["hello", "hello"]))
    assert [r.id for r in rows] == RESPONSE_PK_REVERSED


def test_bm25_mixed_constant_and_bind_marker_search_term(cql, fts_setup_with_mock):
    """Both WHERE and ORDER BY must use the same search term, even if that term is a bind marker."""
    table, _ = fts_setup_with_mock

    stmt = cql.prepare(f"SELECT * FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, ?) LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="same search term"):
        cql.execute(stmt, ['world'])
    cql.execute(stmt, ['hello'])

    stmt = cql.prepare(f"SELECT * FROM {table} WHERE BM25(content, ?) > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="same search term"):
        cql.execute(stmt, ['world'])
    cql.execute(stmt, ['hello'])


def test_bm25_two_bind_markers_search_term(cql, fts_setup_with_mock):
    """Both WHERE and ORDER BY must use the same search term, even if both are bind markers."""
    table, _ = fts_setup_with_mock

    stmt = cql.prepare(f"SELECT * FROM {table} WHERE BM25(content, ?) > 0 ORDER BY BM25(content, ?) LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="same search term"):
        cql.execute(stmt, ['hello', 'world'])
    cql.execute(stmt, ['hello', 'hello'])


def test_bm25_named_bind_markers_search_term(cql, fts_setup_with_mock):
    """BM25 query with named bind markers (:name) should execute."""
    table, _ = fts_setup_with_mock

    stmt = cql.prepare(f"SELECT id FROM {table} WHERE BM25(content, :term) > 0 ORDER BY BM25(content, :term) LIMIT {NUM_ROWS}")
    rows = list(cql.execute(stmt, {"term": "hello"}))
    assert [r.id for r in rows] == RESPONSE_PK_REVERSED


def test_fts_request_sent_to_correct_endpoint(cql, test_keyspace, vector_store_mock, fts_setup_with_mock):
    """Scylla must POST to /api/v1/indexes/<ks>/<idx>/bm25 with the query term and limit."""
    table, idx = fts_setup_with_mock

    cql.execute(
        f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 67")

    reqs = vector_store_mock.bm25_requests
    assert len(reqs) == 1
    assert reqs[0].path == f"/api/v1/indexes/{test_keyspace}/{idx}/bm25"
    body = json.loads(reqs[0].body)
    assert body["query"] == "hello"
    assert body["limit"] == 67


def test_fts_limit_returns_correct_number_of_rows(cql, fts_table, vector_store_mock):
    """A BM25 query with LIMIT N should return exactly N rows when the index returns exactly N keys."""
    table, _ = fts_table
    limit = 3
    ids = list(reversed(range(limit)))
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {limit}"))
    assert len(rows) == limit
    assert [r.id for r in rows] == ids


def test_fts_http_error_propagated_as_invalid_request(cql, vector_store_mock, fts_setup_with_mock):
    """An HTTP error from the vector store must be surfaced as an InvalidRequest."""
    table, _ = fts_setup_with_mock
    vector_store_mock.set_next_bm25_response(HTTPStatus.NOT_FOUND, "index does not exist")

    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}")


def test_fts_limit_exceeds_max_raises_error(cql, fts_setup_with_mock):
    """A LIMIT exceeding max_fts_query_limit (1000) must be rejected."""
    table, _ = fts_setup_with_mock

    with pytest.raises(InvalidRequest, match="1000"):
        cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 1001")


def test_fts_paging_warning_emitted(cql, fts_setup_with_mock):
    """A paging warning should be emitted when page_size < LIMIT."""
    table, _ = fts_setup_with_mock

    result = cql.execute(
        SimpleStatement(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 100",
        fetch_size=5))

    warnings = result.response_future.warnings
    assert warnings
    assert any("Paging is not supported" in w for w in warnings)


def test_fts_with_clustering_key_returns_rows_in_bm25_order(cql, test_keyspace, vector_store_mock):
    """FTS on a table with clustering keys must also preserve BM25 ordering."""
    schema = "p int, c int, content text, PRIMARY KEY (p, c)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        for p, c in [(1, 10), (1, 20), (2, 30)]:
            cql.execute(f"INSERT INTO {table} (p, c, content) VALUES ({p}, {c}, 'hello')")

        # Return (2, 30) first, then (1, 10).
        vector_store_mock.set_next_bm25_response(200, json.dumps({
            "primary_keys": {"p": [2, 1], "c": [30, 10]},
            "scores": [0.9, 0.5],
        }))

        rows = list(cql.execute(f"SELECT p, c FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 2"))
        assert [(r.p, r.c) for r in rows] == [(2, 30), (1, 10)]


def test_fts_ascii_column_executes(cql, test_keyspace, vector_store_mock):
    """BM25 on an ascii column must execute and return rows in BM25 rank order."""
    with new_test_table(cql, test_keyspace, "id int primary key, content ascii") as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        for i in range(NUM_ROWS):
            cql.execute(f"INSERT INTO {table} (id, content) VALUES ({i}, 'hello')")

        vector_store_mock.set_next_bm25_response(200, bm25_response(RESPONSE_PK_REVERSED))
        rows = list(cql.execute(f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}"))
        assert [r.id for r in rows] == RESPONSE_PK_REVERSED


def test_bm25_in_select_returns_scores(cql, fts_table, vector_store_mock):
    """SELECT BM25 must return each row's own score, in vector-store order, and skip a stale id absent from the base table."""
    table, _ = fts_table

    # id=99 is a stale key not present in the table; it must be skipped without
    # shifting or corrupting the scores of the surrounding real ids.
    mock_data = [(4, 1.00), (3, 1.25), (99, 1.50), (2, 1.75), (1, 2.00), (0, 2.25)]
    expected = [(id, score) for id, score in mock_data if id != 99]
    vector_store_mock.set_next_bm25_response(200, bm25_response(
        [id for id, _ in mock_data], scores=[s for _, s in mock_data]))

    rows = list(cql.execute(
        f"SELECT id, BM25(content, 'hello') AS score FROM {table} "
        f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {len(mock_data)}"))

    assert len(rows) == len(expected)
    for row, (rid, score) in zip(rows, expected):
        assert row.id == rid
        assert row.score == pytest.approx(score)


def test_bm25_in_select_skips_multiple_stale_keys(cql, fts_table, vector_store_mock):
    """SELECT BM25 must cope with several stale ids in a row, and with stale ids leading and trailing the result list."""
    table, _ = fts_table

    # The vector store index can be stale and return keys of rows that are no
    # longer in the base table. Here ids 9x are all absent, covering the three
    # shapes the skipping loop has to handle: stale ids before the first real
    # one, several consecutive stale ids in the middle, and a stale id last
    # (which drives the loop off the end of the result list).
    stale = {95, 96, 97, 98, 99}
    mock_data = [(97, 1.00), (98, 1.25), (4, 1.50), (3, 1.75),
                 (95, 2.00), (96, 2.25), (2, 2.50), (1, 2.75), (0, 3.00), (99, 3.25)]
    expected = [(id, score) for id, score in mock_data if id not in stale]
    vector_store_mock.set_next_bm25_response(200, bm25_response(
        [id for id, _ in mock_data], scores=[s for _, s in mock_data]))

    rows = list(cql.execute(
        f"SELECT id, BM25(content, 'hello') AS score FROM {table} "
        f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {len(mock_data)}"))

    assert len(rows) == len(expected)
    for row, (rid, score) in zip(rows, expected):
        assert row.id == rid
        assert row.score == pytest.approx(score)


def test_bm25_in_select_returns_correct_scores_with_clustering_key(cql, test_keyspace, vector_store_mock):
    """SELECT BM25 on a table with a clustering key must attach the correct score to each (pk, ck), skipping stale entries that don't exist in the table."""
    schema = "pk int, ck int, content text, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")

        # (99, 999) is a stale (pk,ck) not present in the table; it must be
        # skipped without shifting or corrupting the scores of surrounding rows.
        mock_data = [(3, 40, 1.00), (99, 999, 1.25), (1, 20, 1.37), (2, 30, 1.74), (1, 10, 2.11)]
        expected = [(pk, ck, score) for pk, ck, score in mock_data if (pk, ck) != (99, 999)]
        for pk, ck, _ in expected:
            cql.execute(f"INSERT INTO {table} (pk, ck, content) VALUES ({pk}, {ck}, 'hello')")

        vector_store_mock.set_next_bm25_response(200, json.dumps({
            "primary_keys": {"pk": [pk for pk, _, _ in mock_data], "ck": [ck for _, ck, _ in mock_data]},
            "scores": [score for _, _, score in mock_data],
        }))

        rows = list(cql.execute(
            f"SELECT pk, ck, BM25(content, 'hello') AS score FROM {table} "
            f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {len(mock_data)}"))

        assert len(rows) == len(expected)
        for row, (pk, ck, score) in zip(rows, expected):
            assert row.pk == pk
            assert row.ck == ck
            assert row.score == pytest.approx(score)


def test_bm25_in_select_bind_marker_mismatch_raises(cql, fts_setup_with_mock, vector_store_mock):
    """SELECT BM25 with a bind marker that evaluates to a different term than ORDER BY must raise."""
    table, _ = fts_setup_with_mock

    stmt = cql.prepare(
        f"SELECT id, BM25(content, ?) AS score FROM {table} "
        f"WHERE BM25(content, ?) > 0 ORDER BY BM25(content, ?) LIMIT {NUM_ROWS}")
    # All three markers with the same value: OK
    vector_store_mock.set_next_bm25_response(200, bm25_response(RESPONSE_PK_REVERSED))
    cql.execute(stmt, ["hello", "hello", "hello"])
    # SELECT marker differs from ORDER BY marker: raises
    with pytest.raises(InvalidRequest, match="same search term"):
        cql.execute(stmt, ["world", "hello", "hello"])


def test_bm25_in_select_nested_unaliased_column_name(cql, fts_setup_with_mock):
    """SELECT CAST(BM25(...) AS double) without an alias must not leak internal @external_value in the column name."""
    table, _ = fts_setup_with_mock

    rows = list(cql.execute(
        f"SELECT CAST(BM25(content, 'hello') AS double) FROM {table} "
        f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}"))
    assert len(rows) == NUM_ROWS
    col_names = rows[0]._fields
    assert any("bm25" in name.lower() for name in col_names), f"Expected BM25 column name, got {col_names}"
    assert not any("external_value" in name for name in col_names), f"Internal name leaked: {col_names}"


def test_bm25_hidden_pk_columns_not_leaked(cql, test_keyspace, vector_store_mock):
    """With SELECT BM25() PK/CK columns added internally to match scores to rows must never leak into the client-visible result."""
    schema = "pk int, ck int, content text, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")

        # (pk, ck, score) in the order the mock VS will return them.
        items = [(3, 30, 300.0), (2, 20, 200.0), (1, 10, 100.0)]
        for pk, ck, _ in items:
            cql.execute(f"INSERT INTO {table} (pk, ck, content) VALUES ({pk}, {ck}, 'hello')")

        vector_store_mock.set_next_bm25_response(200, json.dumps({
            "primary_keys": {"pk": [pk for pk, _, _ in items], "ck": [ck for _, ck, _ in items]},
            "scores": [score for _, _, score in items],
        }))

        # PK/CK are added internally to match BM25 scores to rows, regardless of column
        # order or which other columns are selected. They must never leak into the
        # client-visible result, and each score must stay attached to the right row.
        bm25_col = "BM25(content, 'hello') AS score"
        select_columns_variants = [
            [bm25_col],
            ["pk", bm25_col],
            [bm25_col, "pk"],
            ["content", bm25_col],
            [bm25_col, "content"],
        ]
        for columns in select_columns_variants:
            select_clause = ", ".join(columns)
            print(f"Testing hidden PK/CK columns with SELECT {select_clause}")
            rows = list(cql.execute(
                f"SELECT {select_clause} FROM {table} "
                f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 3"))
            expected_fields = {col.split(" AS ")[-1] for col in columns}
            assert set(rows[0]._fields) == expected_fields, (
                f"SELECT {select_clause}: expected fields {expected_fields}, got {rows[0]._fields}")
            for row, (pk, _ck, score) in zip(rows, items):
                assert row.score == pytest.approx(score)
                if "pk" in expected_fields:
                    assert row.pk == pk
                if "content" in expected_fields:
                    assert row.content == "hello"


def test_bm25_in_select_with_aggregate_rejected(cql, fts_table, vector_store_mock):
    """Aggregation over a full-text search is refused.

    Worth pinning, because it is what keeps the two users of the temporary slot
    space apart: the score arrives in a slot allocated during prepare, and an
    aggregate's running state would live in a slot of its own. They are laid out
    to coexist, but no query can currently produce both at once. If this
    restriction is ever lifted, that layout is what needs re-checking first -
    in particular that a group boundary does not clear the score of the row
    being processed.
    """
    table, _ = fts_table

    for select in ["max(BM25(content, 'hello'))",
                   "count(*), BM25(content, 'hello')"]:
        with pytest.raises(InvalidRequest, match="cannot be run with aggregation"):
            cql.execute(
                f"SELECT {select} FROM {table} "
                f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 5")


###############################################################################
# BM25_HIGHLIGHT() in the SELECT clause.
#
# The fragment is generated by the index from text the coordinator sends it, so
# a query that selects it makes a second request - to `/highlight` - after the
# base-table rows have been read. The reply is positional: entry i belongs to
# the i-th document sent, which is the i-th row read.
###############################################################################

# A table whose rows have distinct text, so that the documents in the request
# and the fragments in the reply can be told apart by position.
DISTINCT_CONTENT = ["the quick brown fox", "jumped over", "the lazy dog", "nothing here"]


@pytest.fixture(scope="function")
def distinct_fts_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int primary key, content text") as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        for i, content in enumerate(DISTINCT_CONTENT):
            cql.execute(f"INSERT INTO {table} (id, content) VALUES ({i}, '{content}')")
        yield table


def select_highlight(table, ids, limit=None):
    """A query selecting the fragment for the given ids, in the order the mock will return them."""
    return (f"SELECT id, BM25_HIGHLIGHT(content, 'fox') AS excerpt FROM {table} "
            f"WHERE BM25(content, 'fox') > 0 ORDER BY BM25(content, 'fox') LIMIT {limit or len(ids)}")


def test_highlight_sends_one_request_with_the_rows_text(cql, distinct_fts_table, vector_store_mock):
    """One `/highlight` request per query, carrying the search term and the base-table text of every row read, in result order."""
    table = distinct_fts_table
    ids = [2, 0, 3]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["a", "b", "c"]))

    rows = list(cql.execute(select_highlight(table, ids)))
    assert [row.id for row in rows] == ids

    reqs = vector_store_mock.highlight_requests
    assert len(reqs) == 1, f"expected exactly one highlight request, got {len(reqs)}"
    assert reqs[0].path.endswith("/highlight")
    body = json.loads(reqs[0].body)
    assert body["query"] == "fox"
    # The documents must be the rows' own text, in the order the rows came back.
    assert body["documents"] == [DISTINCT_CONTENT[i] for i in ids]
    assert [row.excerpt for row in rows] == ["a", "b", "c"]


def test_no_highlight_in_select_makes_no_highlight_request(cql, fts_setup_with_mock, vector_store_mock):
    """A query that does not select a fragment must not ask the index for one."""
    table, _ = fts_setup_with_mock

    cql.execute(f"SELECT id, BM25(content, 'hello') FROM {table} "
                f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT {NUM_ROWS}")

    assert vector_store_mock.highlight_requests == []


def test_highlight_absent_fragment_is_null_and_keeps_the_row(cql, distinct_fts_table, vector_store_mock):
    """A document the index found no fragment in yields a null column - never an empty string - and the row stays in the result."""
    table = distinct_fts_table
    ids = [0, 3, 2]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["the quick brown <b>fox</b>", None, ""]))

    rows = list(cql.execute(select_highlight(table, ids)))

    assert [row.id for row in rows] == ids, "a row without a fragment must not be dropped"
    assert rows[0].excerpt == "the quick brown <b>fox</b>"
    assert rows[1].excerpt is None
    # An empty string is a fragment the index chose to return, and stays distinct from null.
    assert rows[2].excerpt == ""


def test_highlight_markers_are_returned_verbatim(cql, distinct_fts_table, vector_store_mock):
    """Markers are the index's to choose and the client's to render, so the coordinator must not escape or rewrite them."""
    table = distinct_fts_table
    ids = [0]
    marked = '<em class="hit">fox</em> & <b>dog</b>'
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response([marked]))

    rows = list(cql.execute(select_highlight(table, ids)))
    assert rows[0].excerpt == marked


def test_highlight_reply_of_wrong_length_fails_the_query(cql, distinct_fts_table, vector_store_mock):
    """Fragments are matched to rows by position, so a reply of a different length must fail rather than misattribute them."""
    table = distinct_fts_table
    ids = [0, 3]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["only one"]))

    with pytest.raises(InvalidRequest, match="Vector Store"):
        cql.execute(select_highlight(table, ids))


def test_highlight_request_failure_fails_the_query(cql, distinct_fts_table, vector_store_mock):
    """A failed `/highlight` call is a query error - null is reserved for "no fragment exists for this row"."""
    table = distinct_fts_table
    ids = [0, 3]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(int(HTTPStatus.INTERNAL_SERVER_ERROR), '"boom"')

    with pytest.raises(InvalidRequest, match="Vector Store"):
        cql.execute(select_highlight(table, ids))


def test_highlight_makes_no_request_when_the_search_found_nothing(cql, distinct_fts_table, vector_store_mock):
    """With no rows to highlight there is nothing to ask about."""
    table = distinct_fts_table
    vector_store_mock.set_next_bm25_response(200, bm25_response([]))

    rows = list(cql.execute(select_highlight(table, [], limit=3)))

    assert rows == []
    assert vector_store_mock.highlight_requests == []


def test_highlight_stays_aligned_when_a_stale_key_drops_a_row(cql, distinct_fts_table, vector_store_mock):
    """A key the index still knows but the base table no longer has drops a row; the surviving fragments must not shift."""
    table = distinct_fts_table
    # id=99 is absent from the base table, so no document is collected for it and
    # the score provider drops it.
    vector_store_mock.set_next_bm25_response(200, bm25_response([0, 99, 3], scores=[3.0, 2.0, 1.0]))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["for id 0", "for id 3"]))

    rows = list(cql.execute(
        f"SELECT id, BM25(content, 'fox') AS score, BM25_HIGHLIGHT(content, 'fox') AS excerpt FROM {table} "
        f"WHERE BM25(content, 'fox') > 0 ORDER BY BM25(content, 'fox') LIMIT 3"))

    assert [row.id for row in rows] == [0, 3]
    assert [row.excerpt for row in rows] == ["for id 0", "for id 3"]
    assert [row.score for row in rows] == [pytest.approx(3.0), pytest.approx(1.0)]
    # Only the rows that survived were sent.
    body = json.loads(vector_store_mock.highlight_requests[0].body)
    assert body["documents"] == [DISTINCT_CONTENT[0], DISTINCT_CONTENT[3]]


def test_highlight_with_clustering_key(cql, test_keyspace, vector_store_mock):
    """Fragments stay attached to the right (pk, ck) - a table with clustering columns is read key by key and merged."""
    schema = "pk int, ck int, content text, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        rows_data = [(1, 10, "first text"), (1, 20, "second text"), (2, 30, "third text")]
        for pk, ck, content in rows_data:
            cql.execute(f"INSERT INTO {table} (pk, ck, content) VALUES ({pk}, {ck}, '{content}')")

        order = [(2, 30), (1, 10), (1, 20)]
        vector_store_mock.set_next_bm25_response(200, json.dumps({
            "primary_keys": {"pk": [pk for pk, _ in order], "ck": [ck for _, ck in order]},
            "scores": [3.0, 2.0, 1.0],
        }))
        vector_store_mock.set_next_highlight_response(200, highlight_response(["third!", "first!", "second!"]))

        rows = list(cql.execute(
            f"SELECT pk, ck, BM25_HIGHLIGHT(content, 'text') AS excerpt FROM {table} "
            f"WHERE BM25(content, 'text') > 0 ORDER BY BM25(content, 'text') LIMIT 3"))

        assert [(row.pk, row.ck) for row in rows] == order
        assert [row.excerpt for row in rows] == ["third!", "first!", "second!"]
        body = json.loads(vector_store_mock.highlight_requests[0].body)
        assert body["documents"] == ["third text", "first text", "second text"]


def test_highlight_does_not_leak_the_fetched_column(cql, distinct_fts_table, vector_store_mock):
    """The highlighted column has to be read to be sent, but a query that did not select it must not receive it."""
    table = distinct_fts_table
    ids = [0, 2]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["a", "b"]))

    rows = list(cql.execute(select_highlight(table, ids)))

    assert rows
    assert set(rows[0]._fields) == {"id", "excerpt"}, f"unexpected columns: {rows[0]._fields}"


def test_highlight_unaliased_column_name(cql, distinct_fts_table, vector_store_mock):
    """An unaliased fragment selector is named by the call the user wrote, not by the slot it was lowered to."""
    table = distinct_fts_table
    ids = [0]
    vector_store_mock.set_next_bm25_response(200, bm25_response(ids))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["a"]))

    rows = list(cql.execute(
        f"SELECT BM25_HIGHLIGHT(content, 'fox') FROM {table} "
        f"WHERE BM25(content, 'fox') > 0 ORDER BY BM25(content, 'fox') LIMIT 1"))

    col_names = rows[0]._fields
    assert any("bm25_highlight" in name.lower() for name in col_names), f"Expected the call as the column name, got {col_names}"
    assert not any("temporary" in name for name in col_names), f"Internal name leaked: {col_names}"


def test_highlight_bind_marker_mismatch_raises(cql, distinct_fts_table, vector_store_mock):
    """A bind marker that makes the fragment's term differ from the ordering's must raise, and the message must name BM25_HIGHLIGHT()."""
    table = distinct_fts_table
    stmt = cql.prepare(
        f"SELECT id, BM25_HIGHLIGHT(content, ?) AS excerpt FROM {table} "
        f"WHERE BM25(content, ?) > 0 ORDER BY BM25(content, ?) LIMIT 2")

    vector_store_mock.set_next_bm25_response(200, bm25_response([0]))
    vector_store_mock.set_next_highlight_response(200, highlight_response(["a"]))
    cql.execute(stmt, ["fox", "fox", "fox"])

    with pytest.raises(InvalidRequest, match="BM25_HIGHLIGHT\\(\\) in SELECT must use the same search term"):
        cql.execute(stmt, ["dog", "fox", "fox"])
