# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for the Vector Store client metrics, read from the /metrics endpoint
# the way dashboards and alerts read them:
#   - scylla_vector_store_requests{op, status}: one increment per call;
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

from test.pylib.skip_types import skip_env

from .util import ScyllaMetrics, config_value_context, new_test_table, unique_name

VECTOR_SCHEMA = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Vector Store indexes need tablets enabled")


def requests_metric(cql, op, status):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_requests", {"op": op, "status": status}) or 0


def enabled_metric(cql):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_enabled")


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


def ann_query(table):
    return f"SELECT pk1, pk2, ck1, ck2 FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 1"


def ann_response(pk1):
    return json.dumps({"primary_keys": {"pk1": [pk1], "pk2": [7], "ck1": [9], "ck2": [2]}, "similarity_scores": [0.1]})


def assert_counted(cql, op, status, query):
    """Run `query`, which is expected to fail, and check it was counted once under `op` and `status`."""
    before = requests_metric(cql, op, status)
    with pytest.raises(Exception):
        cql.execute(query)
    assert requests_metric(cql, op, status) == before + 1, f"{op} not counted as {status}"


def test_requests_series_registered_up_front(cql, vector_store_mock):
    """Every op/status series exists from the start, so a node exports its first failure."""
    metrics = ScyllaMetrics.query(cql)
    assert metrics.label_values("scylla_vector_store_requests", "op") == {"ann", "bm25", "like", "highlight"}
    assert metrics.label_values("scylla_vector_store_requests", "status") == {
        "ok", "disabled", "unavailable", "aborted", "bad_query", "forbidden", "index_not_found", "index_unservable",
        "index_not_ready", "service_error", "format_error", "unexpected_error"}


def test_requests_disabled(cql, vector_table, text_table, vector_store_mock):
    """With no Vector Store URI each call is counted as disabled, under its own op."""
    assert enabled_metric(cql) == 1
    with config_value_context(cql, "vector_store_primary_uri", ""):
        assert enabled_metric(cql) == 0
        assert_counted(cql, "ann", "disabled", ann_query(vector_table))
        assert_counted(cql, "bm25", "disabled",
                       f"SELECT id FROM {text_table} WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 1")
        assert_counted(cql, "like", "disabled", f"SELECT id FROM {text_table} WHERE title LIKE '%ell%' LIMIT 1")
    assert enabled_metric(cql) == 1


def test_requests_ok(cql, vector_table, text_table, vector_store_mock):
    """A served call is counted as ok, under its own op. A highlight is only asked for after a served BM25 search."""
    vector_store_mock.set_next_ann_response(200, ann_response(5))
    before = requests_metric(cql, "ann", "ok")
    cql.execute(ann_query(vector_table))
    assert requests_metric(cql, "ann", "ok") == before + 1

    vector_store_mock.set_next_like_response(200, json.dumps({"primary_keys": {"id": [1]}}))
    before = requests_metric(cql, "like", "ok")
    cql.execute(f"SELECT id FROM {text_table} WHERE title LIKE '%ell%' LIMIT 1")
    assert requests_metric(cql, "like", "ok") == before + 1

    vector_store_mock.set_next_bm25_response(200, json.dumps({"primary_keys": {"id": [1]}, "scores": [1.0]}))
    vector_store_mock.set_next_highlight_response(200, json.dumps({"highlights": ["<b>hello</b>"]}))
    before = (requests_metric(cql, "bm25", "ok"), requests_metric(cql, "highlight", "ok"))
    cql.execute(f"SELECT id, BM25_HIGHLIGHT(content, 'hello') FROM {text_table} "
                f"WHERE BM25(content, 'hello') > 0 ORDER BY BM25(content, 'hello') LIMIT 1")
    assert (requests_metric(cql, "bm25", "ok"), requests_metric(cql, "highlight", "ok")) == (before[0] + 1, before[1] + 1)


@pytest.mark.parametrize("status,body,want", [
    # 503 without NODE_BOOTSTRAPPING, as the Vector Store answers while an index is building.
    (HTTPStatus.SERVICE_UNAVAILABLE, '{"reason":"INDEX_BUILDING"}', "index_not_ready"),
    (HTTPStatus.BAD_REQUEST, '"ALLOW FILTERING required"', "bad_query"),
    (HTTPStatus.FORBIDDEN, '"TLS is required"', "forbidden"),
    (HTTPStatus.NOT_FOUND, '"index not found"', "index_not_found"),
    (HTTPStatus.UNPROCESSABLE_ENTITY, '"unsupported column"', "index_unservable"),
    (HTTPStatus.INTERNAL_SERVER_ERROR, '"boom"', "service_error"),
    # A 200 with a body the client cannot parse.
    (HTTPStatus.OK, '{"primary_keys1":{}}', "format_error"),
    # A key of the wrong type makes the client throw rather than return an error; it is counted all the same.
    (HTTPStatus.OK, ann_response(True), "format_error"),
])
def test_requests_replies(cql, vector_table, vector_store_mock, status, body, want):
    """Each reply of the Vector Store is counted under the status it maps to."""
    vector_store_mock.set_next_ann_response(status, body)
    assert_counted(cql, "ann", want, ann_query(vector_table))


def test_requests_unavailable(cql, vector_table, vector_store_mock):
    """A call to an endpoint that does not answer is counted as unavailable."""
    host = cql.hosts[0].endpoint.address
    with socket.socket() as s:
        s.bind((host, 0))
        port = s.getsockname()[1]
    # The port is closed now, so a connection to it is refused.
    with config_value_context(cql, "vector_store_primary_uri", f"http://{host}:{port}"):
        assert_counted(cql, "ann", "unavailable", ann_query(vector_table))


###############################################################################
# Per-endpoint metrics, which show a broken endpoint even while the failover
# to another endpoint keeps the queries working:
#   - scylla_vector_store_endpoint_requests{role, endpoint, status}: one
#     increment per attempt to an endpoint;
#   - scylla_vector_store_endpoint_addresses{role, endpoint}: the number of
#     addresses resolved for the endpoint, 0 when its host does not resolve.
###############################################################################

def endpoint_requests_metric(cql, endpoint, status):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_endpoint_requests",
                                        {"role": "primary", "endpoint": endpoint, "status": status}) or 0


def endpoint_addresses_metric(cql, endpoint):
    return ScyllaMetrics.query(cql).get("scylla_vector_store_endpoint_addresses", {"role": "primary", "endpoint": endpoint})


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


def test_endpoint_failover_counted(cql, vector_table, vector_store_mock):
    """With one dead and one working endpoint the queries succeed, and the dead endpoint is counted as unavailable."""
    host = cql.hosts[0].endpoint.address
    good = f"{host}:{vector_store_mock.port}"
    dead = f"{host}:{closed_port(host)}"
    vector_store_mock.set_next_ann_response(200, ann_response(5))
    with config_value_context(cql, "vector_store_primary_uri", f"http://{good},http://{dead}"):
        before = (requests_metric(cql, "ann", "ok"), requests_metric(cql, "ann", "unavailable"))
        queries = 0
        # The client picks the first endpoint at random, so it tries the dead one sooner or later.
        while endpoint_requests_metric(cql, dead, "unavailable") == 0 and queries < 30:
            cql.execute(ann_query(vector_table))
            queries += 1
        assert endpoint_requests_metric(cql, dead, "unavailable") > 0, f"dead endpoint not counted after {queries} queries"
        assert endpoint_requests_metric(cql, dead, "ok") == 0
        assert endpoint_requests_metric(cql, good, "ok") >= queries
        # The failover hides the dead endpoint from the per-call counter.
        assert (requests_metric(cql, "ann", "ok"), requests_metric(cql, "ann", "unavailable")) == (before[0] + queries, before[1])


def test_endpoint_series_follow_uris(cql, vector_store_mock):
    """Each configured URI has its own series, including one whose host does not resolve; a removed URI loses them."""
    host = cql.hosts[0].endpoint.address
    good = f"{host}:{vector_store_mock.port}"
    unresolved = "vector-store.invalid:6080"
    with config_value_context(cql, "vector_store_primary_uri", f"http://{good},http://{unresolved}"):
        metrics = ScyllaMetrics.query(cql)
        assert metrics.label_values("scylla_vector_store_endpoint_requests", "endpoint") == {good, unresolved}
        assert metrics.label_values("scylla_vector_store_endpoint_requests", "status") == {"ok", "unavailable"}
        assert wait_for(lambda: endpoint_addresses_metric(cql, good) == 1), "the mock endpoint did not resolve"
        assert endpoint_addresses_metric(cql, unresolved) == 0
    assert ScyllaMetrics.query(cql).label_values("scylla_vector_store_endpoint_requests", "endpoint") == {good}
