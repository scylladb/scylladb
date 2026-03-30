#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import dataclasses
import hashlib
import socket
import struct

import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import unique_name
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


# ---------------------------------------------------------------------------
# Minimal raw CQL v4 socket helpers with SCYLLA_USE_METADATA_ID extension.
#
# The standard Python driver never negotiates SCYLLA_USE_METADATA_ID and
# therefore never includes result_metadata_id in EXECUTE requests for
# protocol v4.  In CQL v5 result_metadata_id exchange is mandatory and
# built into the wire format; until Scylla implements v5, this extension
# provides the same semantics on v4.  The helpers below implement just
# enough of the CQL wire protocol to exercise the server-side prepared
# metadata promotion path introduced for v5 compatibility.
# ---------------------------------------------------------------------------

# CQL opcodes
_OP_STARTUP = 0x01
_OP_AUTH_RESPONSE = 0x0F
_OP_PREPARE = 0x09
_OP_EXECUTE = 0x0A
_OP_READY = 0x02
_OP_AUTHENTICATE = 0x03
_OP_RESULT = 0x08
_OP_AUTH_SUCCESS = 0x10

# RESULT kind codes
_RESULT_KIND_ROWS = 0x00000002
_RESULT_KIND_PREPARED = 0x00000004

# Rows metadata flags (bit positions in the uint32 flags field)
_META_NO_METADATA = 1 << 2
_META_METADATA_CHANGED = 1 << 3

# EXECUTE options flags (1-byte field in CQL v4)
_FLAG_SKIP_METADATA = 0x02

_FRAME_HEADER_SIZE = 9  # version(1)+flags(1)+stream(2)+opcode(1)+length(4)
_CQL_VERSION = "3.0.0"
_DEFAULT_CONSISTENCY = 0x0006  # LOCAL_QUORUM


def _pack_short(v: int) -> bytes:
    return struct.pack(">H", v)


def _pack_int(v: int) -> bytes:
    return struct.pack(">I", v)


def _short_bytes(b: bytes) -> bytes:
    """CQL [short bytes]: uint16 length prefix + payload."""
    return _pack_short(len(b)) + b


def _long_string(s: str) -> bytes:
    """CQL [long string]: uint32 length prefix + UTF-8 bytes."""
    b = s.encode()
    return _pack_int(len(b)) + b


def _string_map(d: dict[str, str]) -> bytes:
    """CQL [string map]: uint16 count + (uint16-prefixed-string, uint16-prefixed-string)*."""
    out = _pack_short(len(d))
    for k, v in d.items():
        out += _short_bytes(k.encode())
        out += _short_bytes(v.encode())
    return out


def _frame(opcode: int, body: bytes, stream: int) -> bytes:
    """Build a CQL v4 request frame."""
    return struct.pack(">BBHBI", 0x04, 0x00, stream, opcode, len(body)) + body


def _recv_frame(sock: socket.socket) -> tuple[int, int, bytes]:
    """Read one CQL v4 response frame; return (stream, opcode, body)."""
    header = b""
    while len(header) < _FRAME_HEADER_SIZE:
        chunk = sock.recv(_FRAME_HEADER_SIZE - len(header))
        assert chunk, "Connection closed while reading frame header"
        header += chunk
    _version, _flags = struct.unpack(">BB", header[0:2])
    stream = struct.unpack(">H", header[2:4])[0]
    opcode = header[4]
    length = struct.unpack(">I", header[5:9])[0]
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        assert chunk, "Connection closed while reading frame body"
        body += chunk
    return stream, opcode, body


@dataclasses.dataclass
class ExecuteResult:
    """Parsed outcome of a ROWS EXECUTE response."""

    metadata_changed: bool
    no_metadata: bool
    column_count: int
    result_metadata_id: bytes | None


def _cql_connect(host: str, port: int, username: str, password: str) -> socket.socket:
    """
    Open a raw TCP socket to *host*:*port* and perform the CQL v4 handshake,
    negotiating the SCYLLA_USE_METADATA_ID extension so that result_metadata_id
    is exchanged on the wire — identical to the mandatory CQL v5 behaviour.
    """
    sock = socket.create_connection((host, port))
    stream = 1

    # STARTUP with SCYLLA_USE_METADATA_ID enables the v5-style metadata_id
    # exchange for this v4 connection.
    startup_opts = {"CQL_VERSION": _CQL_VERSION, "SCYLLA_USE_METADATA_ID": ""}
    sock.sendall(_frame(_OP_STARTUP, _string_map(startup_opts), stream))
    _, opcode, payload = _recv_frame(sock)

    if opcode == _OP_READY:
        return sock

    assert opcode == _OP_AUTHENTICATE, (
        f"Expected AUTHENTICATE(0x{_OP_AUTHENTICATE:02x}), got 0x{opcode:02x}"
    )

    # PlainText SASL token: NUL + username + NUL + password
    creds = b"\x00" + username.encode() + b"\x00" + password.encode()
    stream += 1
    sock.sendall(_frame(_OP_AUTH_RESPONSE, _short_bytes(creds), stream))
    _, auth_op, _ = _recv_frame(sock)
    assert auth_op == _OP_AUTH_SUCCESS, f"Authentication failed: opcode=0x{auth_op:02x}"
    return sock


def _cql_prepare(sock: socket.socket, stream: int, query: str) -> bytes:
    """PREPARE *query* and return the server-assigned query_id."""
    sock.sendall(_frame(_OP_PREPARE, _long_string(query), stream))
    _, opcode, payload = _recv_frame(sock)
    assert opcode == _OP_RESULT, f"Expected RESULT, got 0x{opcode:02x}"

    pos = 0
    kind = struct.unpack(">I", payload[pos : pos + 4])[0]
    pos += 4
    assert kind == _RESULT_KIND_PREPARED, f"Expected PREPARED kind, got {kind}"

    id_len = struct.unpack(">H", payload[pos : pos + 2])[0]
    pos += 2
    return bytes(payload[pos : pos + id_len])


def _cql_execute_with_metadata_id(
    sock: socket.socket,
    stream: int,
    query_id: bytes,
    result_metadata_id: bytes,
    consistency: int = _DEFAULT_CONSISTENCY,
) -> ExecuteResult:
    """
    Send EXECUTE carrying *result_metadata_id* on the wire.

    With SCYLLA_USE_METADATA_ID active the server reads result_metadata_id
    immediately after query_id (before the options block), mirroring CQL v5
    wire format.  SKIP_METADATA is set so a normal response returns no column
    specs; only the METADATA_CHANGED promotion path returns actual metadata.
    """
    # options block: [consistency: uint16][flags: byte]
    options = struct.pack(">HB", consistency, _FLAG_SKIP_METADATA)
    body = _short_bytes(query_id) + _short_bytes(result_metadata_id) + options
    sock.sendall(_frame(_OP_EXECUTE, body, stream))
    _, opcode, payload = _recv_frame(sock)
    assert opcode == _OP_RESULT, f"Expected RESULT, got 0x{opcode:02x}"

    pos = 0
    kind = struct.unpack(">I", payload[pos : pos + 4])[0]
    pos += 4
    assert kind == _RESULT_KIND_ROWS, f"Expected ROWS kind, got {kind}"

    meta_flags = struct.unpack(">I", payload[pos : pos + 4])[0]
    pos += 4
    column_count = struct.unpack(">I", payload[pos : pos + 4])[0]
    pos += 4

    metadata_changed = bool(meta_flags & _META_METADATA_CHANGED)
    no_metadata = bool(meta_flags & _META_NO_METADATA)

    response_metadata_id: bytes | None = None
    if metadata_changed:
        id_len = struct.unpack(">H", payload[pos : pos + 2])[0]
        pos += 2
        response_metadata_id = bytes(payload[pos : pos + id_len])

    return ExecuteResult(
        metadata_changed=metadata_changed,
        no_metadata=no_metadata,
        column_count=column_count,
        result_metadata_id=response_metadata_id,
    )


def _prepare_and_execute(
    host: str, query: str, stale_metadata_id: bytes
) -> ExecuteResult:
    """
    Open a raw socket connection (negotiating SCYLLA_USE_METADATA_ID), prepare
    *query*, execute it with *stale_metadata_id*, and return the parsed result.
    Intended to be called via ``asyncio.to_thread`` to avoid blocking the event loop.
    """
    sock = _cql_connect(host, 9042, "cassandra", "cassandra")
    try:
        stream = 1
        stream += 1
        query_id = _cql_prepare(sock, stream, query)
        stream += 1
        return _cql_execute_with_metadata_id(sock, stream, query_id, stale_metadata_id)
    finally:
        sock.close()


@pytest.mark.asyncio
async def test_list_roles_of_prepared_metadata_promotion(
    manager: ManagerClient,
) -> None:
    """Verify that the server promotes the prepared metadata_id for statements
    whose PREPARE response carries empty result metadata (NO_METADATA).

    ``LIST ROLES OF <role>`` is such a statement: at PREPARE time the server
    does not know the result set schema because the statement implementation
    builds the metadata dynamically at execute time.  The server therefore
    returns the metadata_id of empty metadata in the PREPARE response.

    When the client later sends EXECUTE with SKIP_METADATA and the stale
    empty metadata_id, the server should detect the mismatch (the actual rows
    have real metadata) and respond with a ``METADATA_CHANGED`` result that
    carries the real metadata_id so the client can update its cache.  This is
    the behaviour mandated by CQL v5; on CQL v4 it is exercised via the
    SCYLLA_USE_METADATA_ID Scylla protocol extension which enables the same
    wire-level exchange.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}")

    # Any non-empty bytes that differ from the real metadata_id serves as the
    # "stale" cache entry the client would send after a PREPARE that returned
    # empty metadata.
    stale_metadata_id = hashlib.sha256(b"").digest()[:16]

    result = await asyncio.to_thread(
        _prepare_and_execute, server.ip_addr, f"LIST ROLES OF {role}", stale_metadata_id
    )

    assert result.metadata_changed, (
        f"expected EXECUTE for LIST ROLES OF {role} to return METADATA_CHANGED "
        f"after PREPARE returned an empty result_metadata_id"
    )
    assert not result.no_metadata, (
        f"expected EXECUTE for LIST ROLES OF {role} to not have NO_METADATA flag "
        f"when METADATA_CHANGED is set"
    )
    assert result.result_metadata_id is not None, (
        f"expected EXECUTE for LIST ROLES OF {role} to return a result_metadata_id "
        f"alongside METADATA_CHANGED"
    )


@pytest.mark.asyncio
@pytest.mark.skip_mode(
    mode="release", reason="error injection is disabled in release mode"
)
async def test_list_roles_of_prepared_metadata_promotion_suppressed_by_injection(
    manager: ManagerClient,
) -> None:
    """Verify that the ``skip_rows_metadata_changed_response`` error injection
    suppresses the metadata promotion, leaving the response with NO_METADATA
    and without METADATA_CHANGED.

    This is the negative/regression counterpart of
    ``test_list_roles_of_prepared_metadata_promotion``: it confirms that the
    happy-path test is not a false positive by showing that the promotion can
    be disabled, and that the injection point itself works correctly.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}")

    stale_metadata_id = hashlib.sha256(b"").digest()[:16]

    async with inject_error(
        manager.api, server.ip_addr, "skip_prepared_result_metadata_promotion"
    ):
        async with inject_error(
            manager.api, server.ip_addr, "skip_rows_metadata_changed_response"
        ):
            result = await asyncio.to_thread(
                _prepare_and_execute,
                server.ip_addr,
                f"LIST ROLES OF {role}",
                stale_metadata_id,
            )

    assert not result.metadata_changed, (
        f"expected injected EXECUTE for LIST ROLES OF {role} to suppress "
        f"METADATA_CHANGED, but the flag was set"
    )
    assert result.no_metadata, (
        f"expected injected EXECUTE for LIST ROLES OF {role} to keep the "
        f"stale NO_METADATA path, but no_metadata flag was not set"
    )
