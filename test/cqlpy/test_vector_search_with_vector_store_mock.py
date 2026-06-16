# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for vector search (SELECT with ANN ordering).
#
# These tests use a mock vector store HTTP server to verify that Scylla
# correctly translates CQL queries with ANN ordering into HTTP requests
# for the vector store service and returns the expected results.
###############################################################################

from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from itertools import combinations
import json
import threading

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

from test.pylib.skip_types import skip_env
from .util import config_value_context, local_process_id, new_test_table, unique_name, is_scylla


@dataclass
class Request:
    path: str
    body: str


@dataclass
class Response:
    status: int = 200
    body: str = '{"primary_keys":{"pk1":[],"pk2":[],"ck1":[],"ck2":[]},"similarity_scores":[]}'


class VectorStoreMock:
    def __init__(self):
        self._ann_requests: list[Request] = []
        self._lock = threading.Lock()
        self._next_ann_response = Response()
        # Maps "{keyspace}/{index}" -> Response for index status queries.
        self._index_status_responses: dict[str, Response] = {}
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._server.server_address[1] if self._server else 0

    @property
    def ann_requests(self) -> list[Request]:
        with self._lock:
            return self._ann_requests.copy()

    def set_next_ann_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_ann_response = Response(status=status, body=body)

    def set_index_status(self, keyspace: str, index: str, status: str, count: int, build_progress: float) -> None:
        body = json.dumps({"status": status, "count": count, "build_progress": build_progress})
        with self._lock:
            self._index_status_responses[f"{keyspace}/{index}"] = Response(status=200, body=body)

    def reset(self) -> None:
        with self._lock:
            self._ann_requests.clear()
            self._next_ann_response = Response()
            self._index_status_responses.clear()

    def _handle_ann(self, request: Request, send_response: Callable[[Response], None]) -> None:
        with self._lock:
            self._ann_requests.append(request)
            response = self._next_ann_response
        send_response(response)

    def _handle_get(self, path: str, send_response: Callable[[Response], None]) -> None:
        # Node health check: report the node as serving so it appears "up".
        if path == "/api/v1/status":
            send_response(Response(status=200, body='"SERVING"'))
            return
        # Per-index status: /api/v1/indexes/{keyspace}/{index}/status
        prefix = "/api/v1/indexes/"
        suffix = "/status"
        if path.startswith(prefix) and path.endswith(suffix):
            key = path[len(prefix):-len(suffix)]
            with self._lock:
                response = self._index_status_responses.get(key)
            if response is not None:
                send_response(response)
                return
            send_response(Response(status=404, body="index not found"))
            return
        send_response(Response(status=404, body="not found"))

    def start(self, host: str):
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode()
                mock._handle_ann(
                    Request(path=self.path, body=body), self._send_response)

            def do_GET(self):
                mock._handle_get(self.path, self._send_response)

            def _send_response(self, response: Response):
                payload = response.body.encode()
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self._server = HTTPServer((host, 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()

        if self._thread:
            self._thread.join()


@pytest.fixture(scope="module")
def _vector_store_mock_session(cql):
    mock = VectorStoreMock()
    if not is_scylla(cql):
        # Yield a mock without starting the HTTP server so tests can run
        # on Cassandra (where the vector store service is not needed).
        yield mock
        return

    if not local_process_id(cql):
        skip_env("Vector store mock requires a local Scylla process")
    host = cql.hosts[0].endpoint.address
    mock.start(host)
    try:
        with config_value_context(cql, "vector_store_primary_uri", f"http://{host}:{mock.port}"):
            yield mock
    finally:
        mock.stop()


@pytest.fixture
def vector_store_mock(_vector_store_mock_session):
    _vector_store_mock_session.reset()
    yield _vector_store_mock_session


# Verify that partition key IN restriction is forwarded to the vector store.
def test_vector_search_ann_with_partition_key_in_restriction(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, embedding) VALUES (5, 7, 9, 2, [0.1, 0.2, 0.3])")
        vector_store_mock.set_next_ann_response(200, json.dumps({"primary_keys": {
                                                "pk1": [5], "pk2": [7], "ck1": [9], "ck2": [2]}, "similarity_scores": [0.1]}))

        result = cql.execute(
            f"SELECT pk1, pk2, ck1, ck2 FROM {table} WHERE pk1 IN (5, 6) ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 2")

        # Assert CQL SELECT results are returned according to the vector store mock response.
        assert list(result) == [(5, 7, 9, 2)]

        # Assert Scylla sent the expected ANN request to the vector store mock.
        requests = vector_store_mock.ann_requests
        assert len(requests) == 1
        assert requests[0].path == f"/api/v1/indexes/{test_keyspace}/{index_name}/ann"
        assert json.loads(requests[0].body) == {
            "vector": [0.1, 0.2, 0.3],
            "limit": 2,
            "filter": {
                "restrictions": [{"type": "IN", "lhs": "pk1", "rhs": [5, 6]}],
                "allow_filtering": False,
            },
        }


# Verify that HTTP error responses from the vector store are propagated through CQL InvalidRequest.
def test_vector_search_cql_error_contains_http_error_description(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        vector_store_mock.set_next_ann_response(HTTPStatus.NOT_FOUND, "index does not exist")

        with pytest.raises(InvalidRequest, match="404.*index does not exist"):
            cql.execute(
                f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


# Create a vector index with an additional filtering column.
# Because the local secondary index logic was used to determine the index target column,
# the implementation wrongly selects last column as the target(vectors) column, leading to
# an exception on the SELECT query:
#     ANN ordering by vector requires the column to be indexed using 'vector_index'.
# Reproduces SCYLLADB-635.
def test_vector_search_vector_index_with_additional_filtering_column(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding, ck1) USING 'vector_index'")

        cql.execute(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


def test_vector_search_local_vector_index_create_and_query_do_not_fail(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}((pk1, pk2), embedding) USING 'vector_index'")

        cql.execute(
            f"SELECT * FROM {table} WHERE pk1 = 1 AND pk2 = 2 ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


# Verify that a paging warning is emitted when page size is smaller than LIMIT.
def test_vector_search_paging_warning_when_page_size_smaller_than_limit(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100", fetch_size=5))

        warnings = result.response_future.warnings
        assert warnings
        assert len(warnings) == 1
        assert "Paging is not supported for Vector Search queries. The entire result set has been returned." == warnings[0]


# Verify no paging warning is emitted when paging is disabled (fetch_size=0).
def test_vector_search_no_paging_warning_when_paging_disabled(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100", fetch_size=0))

        assert not result.response_future.warnings


# Verify no paging warning is emitted when LIMIT is less than page size.
def test_vector_search_no_paging_warning_when_limit_less_than_page_size(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5", fetch_size=100))

        assert not result.response_future.warnings

# Vector Search allows filtering on partition and clustering key columns in any combination.
# It also assumes responsibility for filtering, so post-filtering in the coordinator is not allowed:
# only selected columns are retrieved from the base table, and remaining columns are unavailable for filtering.
# 
# This test verifies all the combinations of partition key and clustering key columns filtering.
# It reproduces bugs from when post filtering was not disabled in general and the normal filtering logic was used.
# In that case some combinations triggered post-filtering, which failed or crashed (on primary key columns)
# due to missing columns in the SELECT result set.
#
# Reproduces SCYLLADB-2737 (all cases without pk1 or pk2)
def test_vector_search_ann_with_restricted_key_column_not_selected(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, category int, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"


# Poll system.vector_store_indexes until a row matching the predicate appears,
# or the timeout elapses. Returns the matching row, or None on timeout.
def _wait_for_vector_store_index_row(cql, keyspace, index, predicate, timeout=30):
    import time
    deadline = time.time() + timeout
    last_rows = []
    while time.time() < deadline:
        rows = list(cql.execute(
            "SELECT * FROM system.vector_store_indexes "
            f"WHERE keyspace_name = '{keyspace}' AND index_name = '{index}'"))
        last_rows = rows
        for row in rows:
            if predicate(row):
                return row
        time.sleep(0.5)
    # Help debugging on failure by surfacing what was actually observed.
    print(f"last observed rows for {keyspace}.{index}: {last_rows}")
    return None


# The system.vector_store_indexes virtual table reports, per vector store node,
# the state of each vector index. For a serving index the row exposes the index
# state, build progress, size and schema-derived metadata (dimensions,
# similarity function).
def test_vector_store_indexes_serving(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) "
            "USING 'vector_index' WITH OPTIONS = {'similarity_function': 'cosine'}")
        vector_store_mock.set_index_status(test_keyspace, index_name, "SERVING", 123, 100.0)

        row = _wait_for_vector_store_index_row(
            cql, test_keyspace, index_name,
            lambda r: r.node_status == 'up' and r.index_state == 'SERVING')
        assert row is not None
        assert row.vector_store_role == 'primary'
        assert row.node_status == 'up'
        assert row.index_state == 'SERVING'
        assert row.build_progress == 100.0
        assert row.size == 123
        assert row.dimensions == 3
        assert row.similarity_function == 'cosine'


    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, category, embedding) VALUES (5, 7, 9, 1, 7, [0.1, 0.2, 0.3])")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, category, embedding) VALUES (5, 7, 9, 2, 8, [0.1, 0.2, 0.3])")
        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"pk1": [5, 5], "pk2": [7, 7], "ck1": [9, 9], "ck2": [2, 1]},
            "similarity_scores": [0.2, 0.1],
        }))

        restriction_tokens = [
            "pk1 = 5",
            "pk2 = 7",
            "ck1 = 9",
            "ck2 IN (1, 2)",
        ]

        for size in range(1, len(restriction_tokens) + 1):
            for subset in combinations(restriction_tokens, size):
                where_clause = " AND ".join(subset)
                print(f"Testing ANN query with restrictions: {where_clause}")
                result = cql.execute(
                    f"SELECT category FROM {table} WHERE {where_clause} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 2")

                assert list(result) == [(8,), (7,)]


# Regression test: a map subscript restriction in a vector ANN query
# (e.g. WHERE metadata_s['source'] = 'docs') was silently dropped by
# binary_operator_to_prepared instead of raising an error, causing the
# vector store to receive a request with no filter and return results
# that violated the predicate.
def test_vector_search_map_subscript_restriction_raises_error(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk text, metadata_s map<text, text>, embedding vector<float, 3>, PRIMARY KEY (pk)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        with pytest.raises(InvalidRequest, match="Unsupported restriction"):
            cql.execute(
                f"SELECT pk FROM {table}"
                " WHERE metadata_s['source'] = 'docs'"
                " ORDER BY embedding ANN OF [1.0, 0.0, 0.0]"
                " LIMIT 10 ALLOW FILTERING"
            )


# While the index is still being built, the table reports the BOOTSTRAPPING
# state together with the partial build progress reported by the vector store.
def test_vector_store_indexes_building_stage(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        # Simulate a paused/in-progress backfill scan: the index is bootstrapping
        # and only 42% of the initial table scan has completed.
        vector_store_mock.set_index_status(test_keyspace, index_name, "BOOTSTRAPPING", 42, 42.0)

        row = _wait_for_vector_store_index_row(
            cql, test_keyspace, index_name,
            lambda r: r.node_status == 'up' and r.index_state == 'BOOTSTRAPPING')
        assert row is not None
        assert row.index_state == 'BOOTSTRAPPING'
        assert row.build_progress == 42.0
        assert row.size == 42


# A configured vector store node that cannot be resolved/reached is still
# reported, with an unknown/down connectivity and no index state.
def test_vector_store_indexes_unreachable_node(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        vector_store_mock.set_index_status(test_keyspace, index_name, "SERVING", 1, 100.0)

        # Point the secondary URI at a host that cannot be resolved.
        with config_value_context(cql, "vector_store_secondary_uri", "http://nonexistent.invalid:6080"):
            row = _wait_for_vector_store_index_row(
                cql, test_keyspace, index_name,
                lambda r: r.vector_store_role == 'secondary')
            assert row is not None
            assert row.vector_store_role == 'secondary'
            assert row.vector_store_host == 'nonexistent.invalid'
            assert row.node_status in ('unknown', 'down')
            # No index information is available for an unreachable node.
            assert row.index_state is None
            assert row.build_progress is None
            assert row.size is None
