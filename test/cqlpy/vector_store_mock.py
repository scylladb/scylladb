# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Shared vector store mock for CQL Python tests.

Provides VectorStoreMock - a minimal HTTP server for handling both ANN
(`/ann`) and BM25 (`/bm25`) POST requests from a local Scylla process.
"""

from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading


@dataclass
class Request:
    path: str
    body: str


@dataclass
class Response:
    status: int = 200
    body: str = '{"primary_keys":{"pk1":[],"pk2":[],"ck1":[],"ck2":[]},"similarity_scores":[]}'


@dataclass
class BM25Response:
    status: int = 200
    body: str = '{"primary_keys":{},"scores":[]}'


class VectorStoreMock:
    def __init__(self):
        self._ann_requests: list[Request] = []
        self._bm25_requests: list[Request] = []
        self._lock = threading.Lock()
        self._next_ann_response = Response()
        self._next_bm25_response = BM25Response()
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

    def set_next_ann_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_ann_response = Response(status=status, body=body)

    def set_next_bm25_response(self, status: int, body: str) -> None:
        with self._lock:
            self._next_bm25_response = BM25Response(status=status, body=body)

    def reset(self) -> None:
        with self._lock:
            self._ann_requests.clear()
            self._bm25_requests.clear()
            self._next_ann_response = Response()
            self._next_bm25_response = BM25Response()

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
