# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Talking to Scylla's client-facing listeners with no client library in the way.

The listener tests have to reach a socket exactly as it was configured - on an
address and port of their choosing, with or without TLS, with or without a PROXY
protocol header in front - and have to be able to tell a listener that isn't
there from one that is there but refuses what it is offered. A CQL driver or a
boto3 client hides all of that, so the helpers here speak the two protocols
directly, far enough to prove which listener answered and how.

Shared by test_listeners.py (the "listeners" option) and
test_scylla_yaml_listeners.py (the per-protocol port options).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import ssl
import struct
from typing import NamedTuple

import pytest

logger = logging.getLogger(__name__)

# The TLS material every test node is set up with - the certificate and key the
# harness drops into each server's workdir.
TLS_OPTIONS = {
    'certificate': 'conf/scylla.crt',
    'keyfile': 'conf/scylla.key',
}


class Endpoint(NamedTuple):
    """Where a listener listens, and how one is to talk to it."""
    address: str
    port: int
    tls: bool = False
    proxy_protocol: bool = False


# The address a PROXY protocol header claims the connection comes from.
PROXY_SRC_ADDR = '203.0.113.42'
PROXY_SRC_PORT = 12345

PROXY_V2_SIGNATURE = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a'


def make_proxy_v2_header(src_addr: str, src_port: int, dst_addr: str, dst_port: int) -> bytes:
    """A proxy protocol v2 header for an IPv4 TCP connection.
    See: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
    """
    return b''.join([
        PROXY_V2_SIGNATURE,
        bytes([0x21]),  # version 2, PROXY command
        bytes([0x11]),  # AF_INET, STREAM/TCP
        struct.pack('!H', 12),  # address block length
        socket.inet_aton(src_addr),
        socket.inet_aton(dst_addr),
        struct.pack('!HH', src_port, dst_port),
    ])


async def connect(endpoint: Endpoint, *, tls: bool | None = None, proxy_protocol: bool | None = None,
                  timeout: float = 30) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connect to a listener the way it expects to be talked to - announcing
    ourselves with a PROXY protocol header (which precedes the TLS handshake)
    and upgrading to TLS as needed. Either can be overridden, to check that a
    listener doesn't accept what it wasn't configured for.
    """
    tls = endpoint.tls if tls is None else tls
    proxy_protocol = endpoint.proxy_protocol if proxy_protocol is None else proxy_protocol

    reader, writer = await asyncio.wait_for(
            asyncio.open_connection(endpoint.address, endpoint.port), timeout)
    if proxy_protocol:
        writer.write(make_proxy_v2_header(PROXY_SRC_ADDR, PROXY_SRC_PORT,
                                          endpoint.address, endpoint.port))
        await writer.drain()
    if tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # The test certificate is self-signed and isn't issued to this address.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            await asyncio.wait_for(writer.start_tls(ctx), timeout)
        except BaseException:
            # A handshake that fails leaves the plain transport behind; asyncio
            # complains about it at garbage collection time if we don't.
            writer.close()
            raise
    return reader, writer


# CQL binary protocol v4, see doc/native_protocol_v4.spec.
CQL_HEADER = struct.Struct('>BBhBI')
CQL_OPCODE_OPTIONS = 0x05
CQL_OPCODE_SUPPORTED = 0x06


def parse_string_multimap(body: bytes) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    pos = 0

    def read_short() -> int:
        nonlocal pos
        value, = struct.unpack_from('>H', body, pos)
        pos += 2
        return value

    def read_string() -> str:
        nonlocal pos
        length = read_short()
        value = body[pos:pos + length].decode()
        pos += length
        return value

    for _ in range(read_short()):
        key = read_string()
        result[key] = [read_string() for _ in range(read_short())]
    return result


async def cql_options_on(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> dict[str, list[str]]:
    """Perform a CQL OPTIONS/SUPPORTED exchange over a connection that is already
    open, and return what the server says it supports.

    Separate from cql_options() so that a test can hold a connection across a
    reconfiguration and then ask whether it still works.
    """
    writer.write(CQL_HEADER.pack(0x04, 0x00, 0, CQL_OPCODE_OPTIONS, 0))
    await writer.drain()
    version, _, _, opcode, length = CQL_HEADER.unpack(await reader.readexactly(CQL_HEADER.size))
    body = await reader.readexactly(length)
    assert version == 0x84, f"Not a CQL v4 response frame: {version:#x}"
    assert opcode == CQL_OPCODE_SUPPORTED, f"Expected SUPPORTED, got opcode {opcode:#x}: {body!r}"
    return parse_string_multimap(body)


async def cql_options(endpoint: Endpoint, **kwargs) -> dict[str, list[str]]:
    """Perform a CQL OPTIONS/SUPPORTED exchange, and return what the server says
    it supports. Doubles as a check that the listener speaks CQL.
    """
    reader, writer = await connect(endpoint, **kwargs)
    try:
        return await cql_options_on(reader, writer)
    finally:
        writer.close()


async def alternator_list_tables(endpoint: Endpoint, **kwargs) -> dict:
    """Perform an unauthenticated DynamoDB API ListTables request, and return the
    parsed response. The server is configured not to enforce authorization.
    """
    body = b'{}'
    request = b''.join([
        b'POST / HTTP/1.1\r\n',
        f'Host: {endpoint.address}:{endpoint.port}\r\n'.encode(),
        b'Content-Type: application/x-amz-json-1.0\r\n',
        b'X-Amz-Target: DynamoDB_20120810.ListTables\r\n',
        f'Content-Length: {len(body)}\r\n'.encode(),
        b'Connection: close\r\n\r\n',
        body,
    ])
    reader, writer = await connect(endpoint, **kwargs)
    try:
        writer.write(request)
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=30)
        headers = head.decode().split('\r\n')
        content_length = next(int(header.split(':')[1])
                              for header in headers if header.lower().startswith('content-length:'))
        content = await asyncio.wait_for(reader.readexactly(content_length), timeout=30)
    finally:
        writer.close()
    status = headers[0].split()[1]
    assert status == '200', f"ListTables failed: {head!r} {content!r}"
    return json.loads(content)


async def assert_not_listening(endpoint: Endpoint) -> None:
    with pytest.raises((ConnectionRefusedError, asyncio.TimeoutError, OSError)):
        _, writer = await asyncio.wait_for(
                asyncio.open_connection(endpoint.address, endpoint.port), timeout=5)
        writer.close()


# How a request dies when the listener is there but doesn't accept what it is
# offered: the TLS handshake fails (ssl.SSLError), or the peer hangs up on us
# (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError), or
# it says nothing at all and we give up waiting (asyncio.TimeoutError). A plain
# OSError - a refused or reset connection - counts too. AssertionError
# deliberately does not: that would mean the exchange went through and only the
# answer was wrong, which is not a refusal.
CONNECTION_FAILURES = (
    ssl.SSLError,
    ConnectionError,
    asyncio.IncompleteReadError,
    asyncio.LimitOverrunError,
    asyncio.TimeoutError,
    EOFError,
    OSError,
)


async def assert_request_refused(request, *, timeout: float = 30) -> None:
    """Assert that an exchange with a listener fails to get off the ground.

    The timeout is part of the assertion: a listener that accepts the connection
    and then waits forever for bytes it will never understand has refused the
    request as surely as one that hangs up, and the test must not hang on it.
    """
    with pytest.raises(CONNECTION_FAILURES):
        await asyncio.wait_for(request, timeout)


async def wait_for_listening(endpoint: Endpoint, timeout: float = 60) -> None:
    """Wait until something accepts connections on the endpoint.

    Needed by tests that cannot let the harness wait for the node to serve -
    those whose configuration leaves no plain CQL port for the harness's own
    driver to connect to.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            _, writer = await asyncio.wait_for(
                    asyncio.open_connection(endpoint.address, endpoint.port), timeout=5)
            writer.close()
            return
        except (OSError, asyncio.TimeoutError):
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.2)
