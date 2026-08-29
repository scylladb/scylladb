# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Tests for the "listeners" configuration option, which spells out the
client-facing sockets to listen on, and overrides the per-protocol options
(native_transport_port, alternator_port and friends).

The listeners are spread over two addresses leased from the harness, so that
several CQL - and several Alternator - listeners serve side by side, on
addresses and ports no other test is using.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import ssl
import struct
from typing import Any, NamedTuple, TYPE_CHECKING

import pytest

from test.pylib.host_registry import HostRegistry

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from test.pylib.internal_types import ServerInfo
    from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The CQL port on the server's own address, which the test harness connects to.
# It is the one listener the test doesn't get to choose.
DEFAULT_CQL_PORT = 9042

# Ports for the listeners the test does get to choose - deliberately unusual
# ones, to show that nothing assumes the well-known ports. They are outside the
# ephemeral port range, so that they cannot clash with an outgoing connection of
# ours that happens to be bound to a leased address.
CQL_PORT = 23011
CQL_SHARD_AWARE_PORT = 24127
CQL_TLS_PORT = 25439
CQL_SHARD_AWARE_TLS_PORT = 26591
CQL_PROXY_PORT = 27653
ALTERNATOR_PORT = 28711
ALTERNATOR_TLS_PROXY_PORT = 29819
# Configured through a per-protocol option, which the "listeners" option
# overrides - nothing may listen on it.
IGNORED_LEGACY_PORT = 31063

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


class Deployment(NamedTuple):
    server: ServerInfo
    # The "listeners" option the server was configured with, and where each of
    # its listeners can be reached.
    listeners: dict[str, dict[str, Any]]
    endpoints: dict[str, Endpoint]


def make_listeners(first: str, second: str) -> dict[str, dict[str, Any]]:
    """The listeners to configure, spread over the two given addresses.

    Both protocols are served on both addresses, so every listener has a
    same-protocol sibling listening elsewhere at the same time.
    """
    return {
        # The harness's own connection, on the server's own address.
        'cql_default': {'protocol': 'cql', 'port': DEFAULT_CQL_PORT},

        'cql': {'protocol': 'cql', 'address': first, 'port': CQL_PORT,
                'shard_aware_listener': 'cql_shard_aware'},
        'cql_shard_aware': {'protocol': 'cql', 'address': first, 'port': CQL_SHARD_AWARE_PORT,
                            'shard_aware': True},
        'cql_proxy_protocol': {'protocol': 'cql', 'address': first, 'port': CQL_PROXY_PORT,
                               'proxy_protocol': True, 'keepalive': True},
        # TLS options spelled out in the listener itself...
        'cql_ssl': {'protocol': 'cql', 'address': second, 'port': CQL_TLS_PORT, 'tls': TLS_OPTIONS,
                    'shard_aware_listener': 'cql_shard_aware_ssl'},
        # ... or taken from client_encryption_options, by asking for plain "tls: true".
        'cql_shard_aware_ssl': {'protocol': 'cql', 'address': second, 'port': CQL_SHARD_AWARE_TLS_PORT,
                                'shard_aware': True, 'tls': True},

        'alternator': {'protocol': 'alternator', 'address': first, 'port': ALTERNATOR_PORT},
        # TLS options taken from alternator_encryption_options.
        'alternator_https_proxy_protocol': {'protocol': 'alternator', 'address': second,
                                            'port': ALTERNATOR_TLS_PROXY_PORT,
                                            'tls': True, 'proxy_protocol': True},
    }


def make_config(listeners: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        'listeners': listeners,
        # Overridden by the "listeners" option, so nothing listens on it.
        'native_transport_port': IGNORED_LEGACY_PORT,
        'client_encryption_options': TLS_OPTIONS,
        'alternator_encryption_options': TLS_OPTIONS,
        'alternator_write_isolation': 'only_rmw_uses_lwt',
        'alternator_enforce_authorization': False,
        'alternator_warn_authorization': False,
    }


def endpoints_of(listeners: dict[str, dict[str, Any]], default_address: str) -> dict[str, Endpoint]:
    return {name: Endpoint(address=listener.get('address', default_address),
                           port=listener['port'],
                           tls=bool(listener.get('tls', False)),
                           proxy_protocol=listener.get('proxy_protocol', False))
            for name, listener in listeners.items()}


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
        await asyncio.wait_for(writer.start_tls(ctx), timeout)
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


async def cql_options(endpoint: Endpoint, **kwargs) -> dict[str, list[str]]:
    """Perform a CQL OPTIONS/SUPPORTED exchange, and return what the server says
    it supports. Doubles as a check that the listener speaks CQL.
    """
    reader, writer = await connect(endpoint, **kwargs)
    try:
        writer.write(CQL_HEADER.pack(0x04, 0x00, 0, CQL_OPCODE_OPTIONS, 0))
        await writer.drain()
        version, _, _, opcode, length = CQL_HEADER.unpack(await reader.readexactly(CQL_HEADER.size))
        body = await reader.readexactly(length)
        assert version == 0x84, f"Not a CQL v4 response frame: {version:#x}"
        assert opcode == CQL_OPCODE_SUPPORTED, f"Expected SUPPORTED, got opcode {opcode:#x}: {body!r}"
        return parse_string_multimap(body)
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


@pytest.fixture(scope="function")
async def leased_addresses() -> AsyncIterator[tuple[str, str]]:
    """Two addresses of our own, so that the listeners below cannot collide with
    a server - or a listener - of a concurrently running test.
    """
    registry = HostRegistry()
    addresses = [await registry.lease_host() for _ in range(2)]
    try:
        yield tuple(addresses)
    finally:
        for address in addresses:
            await registry.release_host(address)


@pytest.fixture(scope="function")
async def deployment(manager: ScyllaClusterManager,
                     leased_addresses: tuple[str, str]) -> Deployment:
    listeners = make_listeners(*leased_addresses)
    server = await manager.server_add(config=make_config(listeners))
    return Deployment(server=server, listeners=listeners,
                      endpoints=endpoints_of(listeners, server.ip_addr))


async def test_cql_listeners(manager: ScyllaClusterManager, deployment: Deployment) -> None:
    """Every configured CQL listener serves CQL, on the address and port it was
    given - several of them, on two addresses, at the same time.
    """
    # The default listener is the one the test harness itself connected through.
    cql = manager.get_cql()
    await cql.run_async("SELECT key FROM system.local WHERE key = 'local'")

    for name, endpoint in deployment.endpoints.items():
        if deployment.listeners[name]['protocol'] != 'cql':
            continue
        logger.info("Checking the CQL listener %s on %s", name, endpoint)
        assert 'CQL_VERSION' in await cql_options(endpoint)


async def test_alternator_listeners(deployment: Deployment) -> None:
    """Every configured Alternator listener serves the DynamoDB API, on the
    address and port it was given.
    """
    for name, endpoint in deployment.endpoints.items():
        if deployment.listeners[name]['protocol'] != 'alternator':
            continue
        logger.info("Checking the Alternator listener %s on %s", name, endpoint)
        assert 'TableNames' in await alternator_list_tables(endpoint)


async def test_cql_shard_aware_listener_is_advertised(deployment: Deployment) -> None:
    """A listener advertises the shard-aware listener it names, and only that
    one, under the key matching that listener's own encryption.
    """
    endpoints = deployment.endpoints

    supported = await cql_options(endpoints['cql'])
    assert supported['SCYLLA_SHARD_AWARE_PORT'] == [str(CQL_SHARD_AWARE_PORT)]
    assert 'SCYLLA_SHARD_AWARE_PORT_SSL' not in supported

    supported = await cql_options(endpoints['cql_ssl'])
    assert supported['SCYLLA_SHARD_AWARE_PORT_SSL'] == [str(CQL_SHARD_AWARE_TLS_PORT)]
    assert 'SCYLLA_SHARD_AWARE_PORT' not in supported

    # A listener that names no shard-aware listener offers no shard-aware port.
    for name in ('cql_default', 'cql_proxy_protocol'):
        supported = await cql_options(endpoints[name])
        assert 'SCYLLA_SHARD_AWARE_PORT' not in supported
        assert 'SCYLLA_SHARD_AWARE_PORT_SSL' not in supported


async def test_cql_listener_without_proxy_protocol(deployment: Deployment) -> None:
    """A listener that wasn't asked to expect a PROXY protocol header doesn't
    accept one - the header is taken for a (malformed) CQL frame.
    """
    with pytest.raises((AssertionError, asyncio.IncompleteReadError, asyncio.TimeoutError, OSError)):
        await asyncio.wait_for(cql_options(deployment.endpoints['cql'], proxy_protocol=True),
                               timeout=30)


async def test_per_protocol_options_are_ignored(deployment: Deployment) -> None:
    """With "listeners" set, the per-protocol options are not honored."""
    await assert_not_listening(Endpoint(deployment.server.ip_addr, IGNORED_LEGACY_PORT))


async def test_listeners_are_reconfigurable(manager: ScyllaClusterManager,
                                            deployment: Deployment,
                                            leased_addresses: tuple[str, str]) -> None:
    """A listener follows the address and port it is reconfigured with, and
    leaves the ones it had behind.
    """
    first, second = leased_addresses
    moved = Endpoint(second, IGNORED_LEGACY_PORT)

    listeners = dict(deployment.listeners)
    listeners['cql'] = {'protocol': 'cql', 'address': moved.address, 'port': moved.port}
    # The listener it used to hand its clients over to goes away with it.
    del listeners['cql_shard_aware']

    await manager.server_update_config(deployment.server.server_id, 'listeners', listeners)
    await manager.server_restart(deployment.server.server_id)

    assert 'CQL_VERSION' in await cql_options(moved)
    await assert_not_listening(deployment.endpoints['cql'])
    await assert_not_listening(deployment.endpoints['cql_shard_aware'])
    # The listeners that weren't touched are still there.
    assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql_ssl'])
    assert 'TableNames' in await alternator_list_tables(deployment.endpoints['alternator'])
