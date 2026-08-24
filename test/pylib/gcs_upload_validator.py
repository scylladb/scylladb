#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
A strict resumable-upload validator that sits in front of fake-gcs-server.

fake-gcs-server does not check resumable-upload Content-Range headers at all:
it stores whatever body arrives and derives the object size from it.  Sending
"bytes 500-999/1000" with a 3 byte body stores a 3 byte object and returns 200,
and "bytes 0-0/0" -- a range naming a byte that cannot exist in an object
declared to be empty -- is likewise accepted.  Real GCS rejects both with 400.

That blind spot hid SCYLLADB-3889, where every object storage sstable failed to
write its zero length refs/nodes/<host_id>/<gen> marker against real GCS while
the whole *_gcs test suite stayed green.

This proxy tracks the state of each upload session and computes what the next
Content-Range must be, rejecting anything inconsistent with 400 before it
reaches the mock.  Everything else is forwarded untouched, so tests observe the
same behaviour they always did apart from the added checks.

A chunk is validated from its Content-Length before any payload is read, so
bodies stream through in both directions rather than being buffered.  The tests
using this move real sstables around and must not pay for a copy per request.

See https://cloud.google.com/storage/docs/performing-resumable-uploads
"""

import argparse
import http.client
import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# "bytes <first>-<last>/<total>", "bytes */<total>" or "bytes */*"
CONTENT_RANGE_RE = re.compile(r"^bytes (?:(\d+)-(\d+)|\*)/(?:(\d+)|\*)$")
# a 308 reply reports what the server holds as "bytes=<first>-<last>"
REPLY_RANGE_RE = re.compile(r"^bytes=(\d+)-(\d+)$")

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
              "te", "trailers", "transfer-encoding", "upgrade"}

BLOCK = 128 * 1024


class session:
    """State of one resumable upload session."""

    def __init__(self):
        self.received = 0          # bytes the server has committed so far
        self.total = None          # final size, once the client declares it
        self.finalized = False


class registry:
    def __init__(self):
        self._lock = threading.Lock()
        self._sessions = {}

    def get(self, upload_id):
        with self._lock:
            return self._sessions.setdefault(upload_id, session())

    def drop(self, upload_id):
        with self._lock:
            self._sessions.pop(upload_id, None)


SESSIONS = registry()


class limited_reader:
    """Hands http.client exactly `remaining` bytes off a socket, no buffering."""

    def __init__(self, src, remaining):
        self._src = src
        self._remaining = remaining

    def read(self, size=-1):
        if self._remaining <= 0:
            return b""
        if size is None or size < 0:
            size = self._remaining
        buf = self._src.read(min(size, self._remaining))
        self._remaining -= len(buf)
        return buf


def upload_id_of(path):
    return (parse_qs(urlparse(path).query).get("upload_id") or [None])[0]


def check(content_range, body_len, s):
    """Validate content_range against session state.

    Returns an error string, or None when the header is acceptable.
    """
    m = CONTENT_RANGE_RE.match(content_range)
    if not m:
        return f"malformed Content-Range {content_range!r}"

    first, last, total = m.group(1), m.group(2), m.group(3)

    if first is None:
        # "bytes */<total>" finalizes the session, "bytes */*" queries it.
        # Neither carries a payload.
        if body_len:
            return (f"Content-Range {content_range!r} carries no byte range but the "
                    f"request body is {body_len} bytes")
        if total is not None and int(total) != s.received:
            return (f"Content-Range {content_range!r} finalizes the upload at {total} bytes "
                    f"but {s.received} bytes have been received; expected "
                    f"'bytes */{s.received}'")
        return None

    first, last = int(first), int(last)

    if first > last:
        return f"Content-Range {content_range!r} has first byte {first} past last byte {last}"

    declared = last - first + 1
    if declared != body_len:
        return (f"Content-Range {content_range!r} declares {declared} bytes but the "
                f"request body is {body_len} bytes")

    if total is not None and last >= int(total):
        # The whole point of the check: a chunk can never name a byte at or past
        # the declared total size. For an empty object there is no valid
        # <first>-<last> at all, only 'bytes */0'.
        expected = f"bytes */{total}" if int(total) == s.received else \
                   f"a last byte below {total}"
        return (f"Content-Range {content_range!r} names last byte {last} in an object "
                f"declared to be {total} bytes (valid byte indices are 0..{int(total) - 1}); "
                f"expected {expected}")

    if first != s.received:
        return (f"Content-Range {content_range!r} starts at {first} but {s.received} bytes "
                f"have been received; expected a chunk starting at {s.received}")

    return None


def commit(content_range, reply_range, s):
    """Advance session state after the upstream accepted a chunk."""
    m = CONTENT_RANGE_RE.match(content_range)
    if not m:
        return
    first, last, total = m.group(1), m.group(2), m.group(3)

    if first is None:
        if total is not None:
            s.total = int(total)
            s.finalized = True
        return

    # Prefer what the server says it holds, fall back to what we sent.
    if reply_range:
        rm = REPLY_RANGE_RE.match(reply_range)
        if rm:
            s.received = int(rm.group(2)) + 1
            return
    s.received = int(last) + 1
    if total is not None:
        s.total = int(total)
        if s.received == s.total:
            s.finalized = True


class handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # keep the test log readable; rejections are reported explicitly below
    def log_message(self, fmt, *args):
        pass

    def _note(self, msg):
        sys.stderr.write(f"gcs-upload-validator: {msg}\n")
        sys.stderr.flush()

    def _body_length(self):
        """Length of the request body, or None when it is chunked."""
        if "Content-Length" in self.headers:
            return int(self.headers["Content-Length"])
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            return None
        return 0

    def _read_chunked(self):
        chunks = []
        while True:
            size = int(self.rfile.readline().strip().split(b";")[0], 16)
            if size == 0:
                self.rfile.readline()
                break
            chunks.append(self.rfile.read(size))
            self.rfile.readline()
        return b"".join(chunks)

    def _reject(self, message, body_len):
        self._note(f"REJECT {self.command} {self.path}: {message}")
        # drain whatever the client is sending so the reply is not mistaken for
        # a response to a later request, then drop the connection
        if body_len:
            remaining = body_len
            while remaining > 0:
                got = self.rfile.read(min(BLOCK, remaining))
                if not got:
                    break
                remaining -= len(got)
        self.close_connection = True

        payload = json.dumps({
            "error": {
                "code": 400,
                "message": message,
                "errors": [{"domain": "global", "reason": "badRequest", "message": message}],
            }
        }).encode()
        self.send_response(400)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)

    def _proxy(self):
        body_len = self._body_length()
        buffered = None
        if body_len is None:
            # chunked: no length up front, so this one has to be collected
            buffered = self._read_chunked()
            body_len = len(buffered)

        upload_id = upload_id_of(self.path)
        content_range = self.headers.get("Content-Range")
        s = SESSIONS.get(upload_id) if upload_id else None

        if content_range is not None and s is not None:
            err = check(content_range, body_len, s)
            if err:
                self._reject(err, 0 if buffered is not None else body_len)
                return

        conn = http.client.HTTPConnection(self.server.upstream_host,
                                          self.server.upstream_port, timeout=600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
        headers["Host"] = f"{self.server.upstream_host}:{self.server.upstream_port}"
        headers["Content-Length"] = str(body_len)

        body = buffered if buffered is not None else limited_reader(self.rfile, body_len)
        conn.request(self.command, self.path, body=body, headers=headers)
        reply = conn.getresponse()

        if content_range is not None and s is not None and reply.status < 400:
            commit(content_range, reply.getheader("Range"), s)

        if self.command == "DELETE" and upload_id:
            SESSIONS.drop(upload_id)

        reply_len = reply.getheader("Content-Length")
        self.send_response(reply.status)
        for k, v in reply.getheaders():
            if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                continue
            if k.lower() == "location":
                # make the client come back through us for the rest of the session
                v = v.replace(f"{self.server.upstream_host}:{self.server.upstream_port}",
                              f"{self.server.public_host}:{self.server.server_address[1]}")
            self.send_header(k, v)

        if reply_len is not None:
            # stream it through, downloads here can be whole sstables
            self.send_header("Content-Length", reply_len)
            self.end_headers()
            if self.command != "HEAD":
                remaining = int(reply_len)
                while remaining > 0:
                    buf = reply.read(min(BLOCK, remaining))
                    if not buf:
                        break
                    self.wfile.write(buf)
                    remaining -= len(buf)
        else:
            payload = reply.read()
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)

        conn.close()

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = do_PATCH = _proxy


class validating_server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            # a client going away mid request is routine during teardown, and
            # a traceback per occurrence only makes the test log harder to read
            return
        super().handle_error(request, client_address)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--upstream-host", default="127.0.0.1")
    ap.add_argument("--upstream-port", type=int, required=True,
                    help="port of the fake-gcs-server to forward to")
    ap.add_argument("--bind-host", default="127.0.0.1",
                    help="address to listen on")
    ap.add_argument("--port", type=int, default=0,
                    help="port to listen on, 0 to pick a free one")
    args = ap.parse_args()

    srv = validating_server((args.bind_host, args.port), handler)
    srv.upstream_host = args.upstream_host
    srv.upstream_port = args.upstream_port
    srv.public_host = args.bind_host

    # the fixtures grep stderr for this line to learn the port
    sys.stderr.write(f"Starting GCS upload validator on {srv.server_address} "
                     f"-> ('{args.upstream_host}', {args.upstream_port})\n")
    sys.stderr.flush()

    srv.serve_forever()


if __name__ == "__main__":
    main()
