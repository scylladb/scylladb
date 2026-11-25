# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for the Vector Store client metrics, read from the /metrics endpoint
# the way dashboards and alerts read them:
#   - scylla_vector_store_requests{op, status}: one increment per call, with
#     the status saying who has to act: ok, rejected, not_ready, unavailable,
#     error, aborted;
#   - scylla_vector_store_enabled: 1 when a Vector Store URI is configured.
#
# The counters are node-wide and only grow, so each check compares the value
# before and after the query.
###############################################################################

import json
import socket
import time
from http import HTTPStatus

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest

from test.pylib.skip_types import skip_env

from .util import ScyllaMetrics, config_value_context, new_test_table, unique_name

VECTOR_SCHEMA = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Vector Store indexes need tablets enabled")


ALL_STATUSES = {"ok", "rejected", "not_ready", "unavailable", "error", "aborted"}


def requests_metric(cql, op, status):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_requests", {"op": op, "status": status}) or 0


def enabled_metric(cql):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_enabled")


def closed_port(host):
    """A port nothing listens on, so a connection to it is refused."""
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def wait_for(condition, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.5)
    return condition()


@pytest.fixture(scope="module")
def vector_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, VECTOR_SCHEMA) as table:
        cql.execute(f"CREATE CUSTOM INDEX {unique_name()} ON {table}(embedding) USING 'vector_index'")
        cql.execute(f"INSERT INTO {table} (pk1, pk2, ck1, ck2, embedding) VALUES (5, 7, 9, 2, [0.1, 0.2, 0.3])")
        yield table


@pytest.fixture(scope="module")
def text_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, content text, title text") as table:
        cql.execute(f"CREATE CUSTOM INDEX {unique_name()} ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX {unique_name()} ON {table}(title) USING 'pattern_index'")
        cql.execute(f"INSERT INTO {table} (id, content, title) VALUES (1, 'hello', 'hello')")
        yield table


@pytest.fixture(scope="module")
def hybrid_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, content text, embedding vector<float, 2>") as table:
        cql.execute(f"CREATE CUSTOM INDEX {unique_name()} ON {table}(content) USING 'fulltext_index'")
        cql.execute(f"CREATE CUSTOM INDEX {unique_name()} ON {table}(embedding) USING 'vector_index'")
        cql.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (1, 'hello', [0.1, 0.2])")
        yield table


def ann_query(table):
    return f"SELECT pk1, pk2, ck1, ck2 FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 1"


def bm25_query(table):
    return f"SELECT id FROM {table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 1"


def like_query(table):
    return f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT 1"


def ann_response(pk1):
    return json.dumps({"primary_keys": {"pk1": [pk1], "pk2": [7], "ck1": [9], "ck2": [2]}, "similarity_scores": [0.1]})


def assert_counted(cql, op, status, query, exc=InvalidRequest, match=None):
    """Run `query`, which is expected to fail with `exc`, and check it was counted once under `op` and `status`."""
    before = requests_metric(cql, op, status)

    with pytest.raises(exc, match=match):
        cql.execute(query)

    assert requests_metric(cql, op, status) == before + 1, f"{op} not counted as {status}"


def test_requests_series_registered_up_front(cql, vector_store_mock):
    """Every op/status series exists from the start, so a node exports its first failure."""
    metrics = ScyllaMetrics.query(cql)

    assert metrics.label_values("scylla_vector_store_requests", "op") == {"ann", "bm25", "like", "highlight"}
    assert metrics.label_values("scylla_vector_store_requests", "status") == ALL_STATUSES


def test_requests_disabled(cql, vector_table, text_table, vector_store_mock):
    """With no Vector Store URI the gauge is 0 and each call is counted as unavailable, under its own op.
    This is the CUSTOMER-786 state: empty URIs, every query failing."""
    assert enabled_metric(cql) == 1

    with config_value_context(cql, "vector_store_primary_uri", ""):
        assert enabled_metric(cql) == 0
        assert_counted(cql, "ann", "unavailable", ann_query(vector_table), match="Vector Store is disabled")
        assert_counted(cql, "bm25", "unavailable", bm25_query(text_table), match="Vector Store is disabled")
        assert_counted(cql, "like", "unavailable", like_query(text_table), match="Vector Store is disabled")

    assert enabled_metric(cql) == 1


def test_requests_ok_ann(cql, vector_table, vector_store_mock):
    """A served ANN call is counted as ok."""
    vector_store_mock.set_next_ann_response(200, ann_response(5))
    before = requests_metric(cql, "ann", "ok")

    cql.execute(ann_query(vector_table))

    assert requests_metric(cql, "ann", "ok") == before + 1


def test_requests_ok_like(cql, text_table, vector_store_mock):
    """A served LIKE call is counted as ok."""
    vector_store_mock.set_next_like_response(200, json.dumps({"primary_keys": {"id": [1]}}))
    before = requests_metric(cql, "like", "ok")

    cql.execute(like_query(text_table))

    assert requests_metric(cql, "like", "ok") == before + 1


def test_requests_ok_bm25_with_highlight(cql, text_table, vector_store_mock):
    """A served BM25 call and the highlight asked for after it are each counted as ok, under their own op."""
    vector_store_mock.set_next_bm25_response(200, json.dumps({"primary_keys": {"id": [1]}, "scores": [1.0]}))
    vector_store_mock.set_next_highlight_response(200, json.dumps({"highlights": ["<b>hello</b>"]}))
    bm25_before = requests_metric(cql, "bm25", "ok")
    highlight_before = requests_metric(cql, "highlight", "ok")

    cql.execute(f"SELECT id, BM25_HIGHLIGHT(content, 'hello') FROM {text_table} "
                f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 1")

    assert requests_metric(cql, "bm25", "ok") == bm25_before + 1
    assert requests_metric(cql, "highlight", "ok") == highlight_before + 1


@pytest.mark.parametrize("status,body,want,exc,match", [
    # 503 without NODE_BOOTSTRAPPING, as the Vector Store answers while an index is building.
    (HTTPStatus.SERVICE_UNAVAILABLE, '{"reason":"INDEX_BUILDING"}', "not_ready", InvalidRequest, "503"),
    (HTTPStatus.BAD_REQUEST, '"ALLOW FILTERING required"', "rejected", InvalidRequest, "400"),
    (HTTPStatus.NOT_FOUND, '"index not found"', "rejected", InvalidRequest, "404"),
    (HTTPStatus.UNPROCESSABLE_ENTITY, '"unsupported column"', "rejected", InvalidRequest, "422"),
    (HTTPStatus.FORBIDDEN, '"TLS is required"', "error", InvalidRequest, "403"),
    (HTTPStatus.INTERNAL_SERVER_ERROR, '"boom"', "error", InvalidRequest, "500"),
    # A 200 with a body the client cannot parse.
    (HTTPStatus.OK, '{"primary_keys1":{}}', "error", InvalidRequest, "invalid JSON"),
    # A key of the wrong type makes the client throw rather than return an error; it is counted all the same.
    (HTTPStatus.OK, ann_response(True), "error", NoHostAvailable, "marshaling error"),
])
def test_requests_replies(cql, vector_table, vector_store_mock, status, body, want, exc, match):
    """Each reply of the Vector Store is counted under the status it maps to."""
    vector_store_mock.set_next_ann_response(status, body)

    assert_counted(cql, "ann", want, ann_query(vector_table), exc=exc, match=match)


def test_requests_unavailable(cql, vector_table, vector_store_mock):
    """A call to an endpoint that does not answer is counted as unavailable."""
    host = cql.hosts[0].endpoint.address

    with config_value_context(cql, "vector_store_primary_uri", f"http://{host}:{closed_port(host)}"):
        assert_counted(cql, "ann", "unavailable", ann_query(vector_table), match="Vector Store service is unavailable")


def test_requests_aborted(cql, hybrid_table, vector_store_mock):
    """A hybrid query aborts its ANN request once the BM25 one fails, and the ANN call is counted as aborted."""
    # Far longer than a failing query takes, even in debug mode, and far shorter than the delay.
    timeout = 10
    vector_store_mock.set_ann_response_delay(6 * timeout)
    vector_store_mock.set_next_bm25_response(404, "index does not exist")
    before = requests_metric(cql, "ann", "aborted")

    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(f"SELECT id FROM {hybrid_table} ORDER BY RRF(ANN(embedding, [0.1, 0.2]), BM25(content, 'hello')) LIMIT 10",
                    timeout=timeout)

    # The aborted call is counted when it unwinds, which may be after the query has returned.
    assert wait_for(lambda: requests_metric(cql, "ann", "aborted") == before + 1), "ann not counted as aborted"
