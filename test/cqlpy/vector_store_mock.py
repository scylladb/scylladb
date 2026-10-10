# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Shared vector store mock for CQL Python tests.

Provides VectorStoreMock - a minimal HTTP server for handling the ANN (`/ann`),
BM25 (`/bm25`), highlight (`/highlight`) and pattern (`/like`) POST requests
from a local Scylla process.
"""

from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading


@dataclass
class Request:
    path: str
    body: str


@dataclass
class Response:
    """The reply names the key columns of the queried table, so the default fits a table keyed ((pk1, pk2), ck1, ck2).

    A test on another table that lets an ANN request happen has to set a reply with its key columns.
    """

    status: int = 200
    body: str = '{"primary_keys":{"pk1":[],"pk2":[],"ck1":[],"ck2":[]},"similarity_scores":[]}'


@dataclass
class BM25Response:
    """The reply names the key columns of the queried table, so the default, which names none, fits no table.

    A test that lets a BM25 request happen has to set a reply with its table's key columns.
    """

    status: int = 200
    body: str = '{"primary_keys":{},"scores":[]}'


@dataclass
class HighlightResponse:
    """The reply is aligned with the documents that were sent, so the default answers no documents.

    A test that lets a highlight request happen has to set a reply of the right length.
    """

    status: int = 200
    body: str = '{"highlights":[]}'


@dataclass
class LikeResponse:
    """The reply names the key columns of the queried table, so the default, which names none, fits no table.

    A test that lets a pattern request happen has to set a reply with its table's key columns.
    """

    status: int = 200
    body: str = '{"primary_keys":{}}'


class VectorStoreMock:
    def __init__(self):
        self._ann_requests: list[Request] = []
        self._bm25_requests: list[Request] = []
        self._highlight_requests: list[Request] = []
        self._like_requests: list[Request] = []
        self._status_requests: list[Request] = []
        self._lock = threading.Lock()
        self._next_ann_response = Response()
        self._next_bm25_response = BM25Response()
        self._next_highlight_response = HighlightResponse()
        self._next_like_response = LikeResponse()
        self._next_status_response = Response(status=200, body='"SERVING"')
        # Maps "{keyspace}/{index}" -> Response for per-index status queries.
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

    @property
    def bm25_requests(self) -> list[Request]:
        with self._lock:
            return self._bm25_requests.copy()

    @property
    def highlight_requests(self) -> list[Request]:
        with self._lock:
            return self._highlight_requests.copy()

    @property
    def like_requests(self) -> list[Request]:
        with self._lock:
            return self._like_requests.copy()

    @property
    def status_requests(self) -> list[Request]:
        with self._lock:
            return self._status_requests.copy()

    def set_next_ann_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_ann_response = Response(status=status, body=body)

    def set_next_bm25_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_bm25_response = BM25Response(status=status, body=body)

    def set_next_highlight_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_highlight_response = HighlightResponse(status=status, body=body)

    def set_next_like_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_like_response = LikeResponse(status=status, body=body)

    def set_next_status_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_status_response = Response(status=status, body=body)

    def set_index_status(self, keyspace: str, index: str, status: str, count: int, build_progress: float) -> None:
        body = json.dumps({"status": status, "count": count, "build_progress": build_progress})
        with self._lock:
            self._index_status_responses[f"{keyspace}/{index}"] = Response(status=200, body=body)

    def reset(self) -> None:
        with self._lock:
            self._ann_requests.clear()
            self._bm25_requests.clear()
            self._highlight_requests.clear()
            self._like_requests.clear()
            self._status_requests.clear()
            self._next_ann_response = Response()
            self._next_bm25_response = BM25Response()
            self._next_highlight_response = HighlightResponse()
            self._next_like_response = LikeResponse()
            self._next_status_response = Response(status=200, body='"SERVING"')
            self._index_status_responses.clear()

    def _handle_ann(self, request: Request, send_response: Callable[[Response], None]) -> None:
        with self._lock:
            self._ann_requests.append(request)
            response = self._next_ann_response
        send_response(response)

    def _handle_bm25(self, request: Request, send_response: Callable[[BM25Response], None]) -> None:
        with self._lock:
            self._bm25_requests.append(request)
            response = self._next_bm25_response
        send_response(response)

    def _handle_highlight(self, request: Request, send_response: Callable[[HighlightResponse], None]) -> None:
        with self._lock:
            self._highlight_requests.append(request)
            response = self._next_highlight_response
        send_response(response)

    def _handle_like(self, request: Request, send_response: Callable[[LikeResponse], None]) -> None:
        with self._lock:
            self._like_requests.append(request)
            response = self._next_like_response
        send_response(response)

    def _handle_status(self, request: Request, send_response: Callable[[Response], None]) -> None:
        with self._lock:
            self._status_requests.append(request)
            response = self._next_status_response
        send_response(response)

    def _handle_index_status(self, path: str, send_response: Callable[[Response], None]) -> None:
        # Per-index status: /api/v1/indexes/{keyspace}/{index}/status
        prefix = "/api/v1/indexes/"
        suffix = "/status"
        key = path[len(prefix):-len(suffix)]
        with self._lock:
            response = self._index_status_responses.get(key)
        if response is not None:
            send_response(response)
        else:
            send_response(Response(status=404, body="index not found"))

    def start(self, host: str):
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode()
                req = Request(path=self.path, body=body)
                if self.path.endswith("/ann"):
                    mock._handle_ann(req, self._send_response)
                elif self.path.endswith("/bm25"):
                    mock._handle_bm25(req, self._send_response)
                elif self.path.endswith("/highlight"):
                    mock._handle_highlight(req, self._send_response)
                elif self.path.endswith("/like"):
                    mock._handle_like(req, self._send_response)
                else:
                    self.send_response(404)
                    self.end_headers()

            def do_GET(self):
                if self.path == "/api/v1/status":
                    req = Request(path=self.path, body="")
                    mock._handle_status(req, self._send_response)
                elif self.path.startswith("/api/v1/indexes/") and self.path.endswith("/status"):
                    mock._handle_index_status(self.path, self._send_response)
                else:
                    self.send_response(404)
                    self.end_headers()

            def _send_response(self, response):
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
