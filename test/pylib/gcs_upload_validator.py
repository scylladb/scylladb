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


FAULTS = ("unacknowledged_chunks", "failed_chunks", "failed_cancels", "cancelled_uploads")


STATUS_ANSWERS = "status_answers"
UNACKNOWLEDGED_AT = "unacknowledged_at"

# no scripted answer for this status query: report what the session really holds
UNSET = object()


class injector:
    """Faults a test asked for over the control path, see handler._control()."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts = dict.fromkeys(FAULTS, 0)
        self._status_answers = []
        self._unacknowledged_at = set()

    def arm(self, **counts):
        with self._lock:
            self._counts.update(counts)

    def arm_status_answers(self, answers):
        with self._lock:
            self._status_answers = list(answers)

    def arm_unacknowledged_at(self, offsets):
        with self._lock:
            self._unacknowledged_at = set(offsets)

    def reset(self):
        with self._lock:
            self._counts = dict.fromkeys(FAULTS, 0)
            self._status_answers = []
            self._unacknowledged_at = set()

    def counts(self):
        with self._lock:
            return dict(self._counts, **{
                STATUS_ANSWERS: len(self._status_answers),
                UNACKNOWLEDGED_AT: len(self._unacknowledged_at),
            })

    def take_unacknowledged_at(self, first):
        with self._lock:
            if first not in self._unacknowledged_at:
                return False
            self._unacknowledged_at.discard(first)
            return True

    def take_status_answer(self):
        with self._lock:
            return self._status_answers.pop(0) if self._status_answers else UNSET

    def _take(self, name):
        with self._lock:
            left = self._counts[name]
            if left <= 0:
                return False
            self._counts[name] = left - 1
            return True

    def take_unacknowledged_chunk(self):
        return self._take("unacknowledged_chunks")

    def take_failed_chunk(self):
        return self._take("failed_chunks")

    def take_failed_cancel(self):
        return self._take("failed_cancels")

    def take_cancelled_upload(self):
        return self._take("cancelled_uploads")


INJECTED = injector()

CONTROL_PATH = "/__inject"


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

    def _control(self):
        """Arm faults, or report which are still armed.

        PUT /__inject                    -- report the counts, change nothing
        PUT /__inject?<fault>=<n>[&...]  -- arm <n> of each fault
        PUT /__inject?reset=1            -- disarm every fault

        unacknowledged_chunks -- answer a chunk with a Range-less 308
        failed_chunks         -- answer a chunk with 400
        failed_cancels        -- answer a session DELETE with 403
        cancelled_uploads     -- answer a session DELETE with 499, the way real GCS does

        status_answers=<v>[,<v>...] scripts the next status queries instead of answering
        them from the session. Each <v> is a byte count, or "none"/0 for a Range-less
        reply. A count below the chunk's offset is a session that lost acknowledged
        bytes, which a client cannot recover from.

        unacknowledged_at=<off>[,<off>...] drops the chunk starting at each offset, the
        way unacknowledged_chunks does but without depending on how many chunks the
        client happens to send first. Uploads are serialized, so naming an offset picks
        out one chunk exactly.

        Each count is absolute and applies to the next <n> matching requests whatever
        session they belong to, so a test arms it immediately before the upload it
        wants faulted. The reply carries the counts still armed: a test that expects a
        fault to fire can require it was taken, and one that over-arms can disarm the
        remainder so it does not reach the next test.

        An unknown parameter is an error rather than a no-op -- a misspelled fault would
        otherwise arm nothing and report success, leaving a test green over an upload it
        never faulted.
        """
        body_len = self._body_length() or 0
        if body_len:
            self.rfile.read(body_len)

        q = parse_qs(urlparse(self.path).query)
        known = list(FAULTS) + [STATUS_ANSWERS, UNACKNOWLEDGED_AT]
        unknown = sorted(set(q) - set(known) - {"reset"})
        if unknown:
            self._reject(f"unknown parameter(s) {unknown}; known faults are {known}", 0)
            return

        if "reset" in q:
            INJECTED.reset()
        counts = {name: int(q[name][0]) for name in FAULTS if name in q}
        if counts:
            INJECTED.arm(**counts)
        if STATUS_ANSWERS in q:
            INJECTED.arm_status_answers(
                0 if tok.strip() in ("", "none") else int(tok)
                for tok in q[STATUS_ANSWERS][0].split(","))
        if UNACKNOWLEDGED_AT in q:
            INJECTED.arm_unacknowledged_at(int(tok) for tok in q[UNACKNOWLEDGED_AT][0].split(","))

        left = INJECTED.counts()
        self._note(f"armed: {left}")
        payload = json.dumps(left).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _unacknowledged(self, body_len, buffered):
        """Answer a chunk with "308, nothing persisted": a 308 carrying no Range.

        Google documents a Range-less 308 as "start your upload from the beginning",
        so the client has to send the chunk again. The body is dropped instead of
        forwarded, which makes the reply true -- the session's received count does
        not move, so a client that skips ahead instead trips the Content-Range check
        on its next chunk.
        """
        self._note(f"dropping chunk {self.headers.get('Content-Range')!r}, "
                   f"answering 308 with no Range")
        if buffered is None:
            remaining = body_len
            while remaining > 0:
                got = self.rfile.read(min(BLOCK, remaining))
                if not got:
                    break
                remaining -= len(got)
        self.send_response(308)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _status(self, held):
        """Answer a "bytes */*" status query.

        A session holding nothing has no valid "bytes=0-<last>" to report, so the Range
        header is omitted entirely - which is exactly the case a client must not read as
        "this chunk was not persisted".
        """
        self._note(f"status query: session holds {held} bytes")
        self.send_response(308)
        if held:
            self.send_header("Range", f"bytes=0-{held - 1}")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _proxy(self):
        if urlparse(self.path).path == CONTROL_PATH:
            self._control()
            return

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
            # Only a chunk carrying bytes can be faulted: a "bytes */<total>" finalize
            # and a "bytes */*" status probe name no chunk to drop or to reject, and
            # letting either consume a fault would spend it on the wrong request.
            m = CONTENT_RANGE_RE.match(content_range)
            if m.group(1) is not None:
                if INJECTED.take_unacknowledged_at(int(m.group(1))) \
                        or INJECTED.take_unacknowledged_chunk():
                    self._unacknowledged(body_len, buffered)
                    return
                if INJECTED.take_failed_chunk():
                    self._reject("injected chunk failure", 0 if buffered is not None else body_len)
                    return
            elif m.group(3) is None:
                # "bytes */*" asks where the session stands. Answer it here instead of
                # forwarding: this proxy is what tracks the session, and fake-gcs-server
                # does not have to implement the status check at all.
                held = INJECTED.take_status_answer()
                self._status(s.received if held is UNSET else held)
                return

        if self.command == "DELETE" and upload_id and INJECTED.take_cancelled_upload():
            # Real GCS reports a cancelled resumable upload with 499. fake-gcs-server
            # answers the DELETE with a 2xx, so this is the only way a test reaches the
            # client's 499 path.
            self._note("answering the cancel of an upload with 499")
            SESSIONS.drop(upload_id)
            self.send_response(499, "Client Closed Request")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.command == "DELETE" and upload_id and INJECTED.take_failed_cancel():
            # 403 rather than a 5xx: the client retries retryable statuses, and this
            # fault is only useful if it survives to the caller
            self._note("failing the cancel of an upload")
            self.close_connection = True
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
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
