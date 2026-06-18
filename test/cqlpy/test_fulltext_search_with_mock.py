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
