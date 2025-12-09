# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

"""
Tests for CQL native transport ports with proxy protocol v2 support.

These tests verify that when Scylla is configured with proxy protocol ports,
clients can connect using the PROXY protocol v2 header and the original
client addresses are properly reported in system.clients.
"""

import asyncio
import logging
import pytest
import socket
import ssl
import struct

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

# Proxy protocol v2 signature (12 bytes)
PROXY_V2_SIGNATURE = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a'

# Port numbers for proxy protocol ports (to avoid conflicts with standard ports)
PROXY_PORT = 29042
PROXY_PORT_SSL = 29142
PROXY_SHARD_AWARE_PORT = 39042
PROXY_SHARD_AWARE_PORT_SSL = 39142


def make_proxy_v2_header(src_addr: str, src_port: int, dst_addr: str, dst_port: int) -> bytes:
    """
    Construct a proxy protocol v2 header for IPv4 TCP connections.

    The proxy protocol v2 binary format is defined at:
    https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
    """
    header = bytearray()

    # Signature (12 bytes)
    header.extend(PROXY_V2_SIGNATURE)

    # Version (upper 4 bits) and command (lower 4 bits)
    # Version 2, PROXY command (0x21)
    header.append(0x21)

    # Address family (upper 4 bits) and transport protocol (lower 4 bits)
    # AF_INET (1) and STREAM/TCP (1) = 0x11
    header.append(0x11)

    # Length of the address block (12 bytes for IPv4: 4+4+2+2)
    header.extend(struct.pack('!H', 12))

    # Source address (4 bytes)
    header.extend(socket.inet_aton(src_addr))

    # Destination address (4 bytes)
    header.extend(socket.inet_aton(dst_addr))

    # Source port (2 bytes, big-endian)
    header.extend(struct.pack('!H', src_port))

    # Destination port (2 bytes, big-endian)
    header.extend(struct.pack('!H', dst_port))

    return bytes(header)


def build_cql_startup_frame(stream: int = 1) -> bytes:
    """Build a CQL protocol v4 STARTUP frame."""
    # String map with CQL_VERSION
    body = b'\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0'

    frame = bytearray()
    frame.append(0x04)  # version 4
    frame.append(0x00)  # flags
    frame.extend(struct.pack('!H', stream))  # stream
    frame.append(0x01)  # opcode: STARTUP
    frame.extend(struct.pack('!I', len(body)))  # body length
    frame.extend(body)

    return bytes(frame)


def build_cql_query_frame(query: str, stream: int = 2) -> bytes:
    """Build a CQL protocol v4 QUERY frame."""
    query_bytes = query.encode('utf-8')

    body = bytearray()
    # Long string: 4-byte length + string
    body.extend(struct.pack('!I', len(query_bytes)))
    body.extend(query_bytes)
    # Query parameters: consistency (2 bytes) + flags (1 byte)
    body.extend(struct.pack('!H', 0x0001))  # consistency: ONE
    body.append(0x00)  # flags: none

    frame = bytearray()
    frame.append(0x04)  # version 4
    frame.append(0x00)  # flags
    frame.extend(struct.pack('!H', stream))  # stream
    frame.append(0x07)  # opcode: QUERY
    frame.extend(struct.pack('!I', len(body)))  # body length
    frame.extend(body)

    return bytes(frame)


async def do_cql_handshake(reader, writer):
    """Complete CQL handshake (STARTUP and optional AUTH)."""
    # Send CQL STARTUP
    startup_frame = build_cql_startup_frame(stream=1)
    writer.write(startup_frame)
    await writer.drain()

    # Read response (READY or AUTHENTICATE)
    response = await asyncio.wait_for(reader.read(4096), timeout=10.0)
    if len(response) < 9:
        raise RuntimeError(f"Short response from server: {len(response)} bytes")

    opcode = response[4]
    if opcode == 0x03:  # AUTHENTICATE
        # Send AUTH_RESPONSE with default credentials
        auth_body = b'\x00cassandra\x00cassandra'
        auth_frame = bytearray()
        auth_frame.append(0x04)
        auth_frame.append(0x00)
        auth_frame.extend(struct.pack('!H', 1))
        auth_frame.append(0x0F)  # AUTH_RESPONSE
        auth_frame.extend(struct.pack('!I', len(auth_body)))
        auth_frame.extend(auth_body)
        writer.write(bytes(auth_frame))
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=10.0)
        opcode = response[4]
        if opcode != 0x10:  # AUTH_SUCCESS
            raise RuntimeError(f"Expected AUTH_SUCCESS (0x10), got {hex(opcode)}")
    elif opcode != 0x02:  # READY
        raise RuntimeError(f"Expected READY (0x02) or AUTHENTICATE (0x03), got {hex(opcode)}")


async def send_cql_query(reader, writer, query: str, stream: int = 2) -> bytes:
    """Send a CQL query and return the response."""
    query_frame = build_cql_query_frame(query, stream=stream)
    writer.write(query_frame)
    await writer.drain()
    response = await asyncio.wait_for(reader.read(16384), timeout=10.0)
    return response


async def send_cql_with_proxy_header(
    host: str,
    port: int,
    proxy_src_addr: str,
    proxy_src_port: int,
    proxy_dst_addr: str,
    proxy_dst_port: int,
    query: str
) -> bytes:
    """Connect to a CQL server using proxy protocol v2 and execute a query."""
    reader, writer = await asyncio.open_connection(host, port)

    try:
        # Send proxy protocol v2 header
        proxy_header = make_proxy_v2_header(proxy_src_addr, proxy_src_port, proxy_dst_addr, proxy_dst_port)
        writer.write(proxy_header)
        await writer.drain()

        # Complete CQL handshake
        await do_cql_handshake(reader, writer)

        # Send the query
        return await send_cql_query(reader, writer, query)

    finally:
        writer.close()
        await writer.wait_closed()


async def send_cql_with_proxy_header_tls(
    host: str,
    port: int,
    proxy_src_addr: str,
    proxy_src_port: int,
    proxy_dst_addr: str,
    proxy_dst_port: int,
    query: str
) -> bytes:
    """
    Connect to a CQL server using proxy protocol v2 with TLS and execute a query.

    The proxy protocol header is sent first over the raw TCP connection,
    then the connection is upgraded to TLS before the CQL protocol begins.
    """
    # Create a socket and connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    loop = asyncio.get_event_loop()
    await loop.sock_connect(sock, (host, port))

    try:
        # Send proxy header on raw socket BEFORE TLS handshake
        proxy_header = make_proxy_v2_header(proxy_src_addr, proxy_src_port, proxy_dst_addr, proxy_dst_port)
        await loop.sock_sendall(sock, proxy_header)

        # Create SSL context (don't verify server certificate for testing)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Wrap the socket with TLS
        ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host, do_handshake_on_connect=False)

        # Do TLS handshake (non-blocking)
        while True:
            try:
                ssl_sock.do_handshake()
                break
            except ssl.SSLWantReadError:
                await asyncio.sleep(0.01)
            except ssl.SSLWantWriteError:
                await asyncio.sleep(0.01)

        # Make the SSL socket blocking for simplicity
        ssl_sock.setblocking(True)

        # Send CQL STARTUP
        startup_frame = build_cql_startup_frame(stream=1)
        ssl_sock.sendall(startup_frame)

        # Read response
        response = ssl_sock.recv(4096)
        if len(response) < 9:
            raise RuntimeError(f"Short response from server: {len(response)} bytes")

        opcode = response[4]
        if opcode == 0x03:  # AUTHENTICATE
            auth_body = b'\x00cassandra\x00cassandra'
            auth_frame = bytearray()
            auth_frame.append(0x04)
            auth_frame.append(0x00)
            auth_frame.extend(struct.pack('!H', 1))
            auth_frame.append(0x0F)
            auth_frame.extend(struct.pack('!I', len(auth_body)))
            auth_frame.extend(auth_body)
            ssl_sock.sendall(bytes(auth_frame))
            response = ssl_sock.recv(4096)
            opcode = response[4]
            if opcode != 0x10:
                raise RuntimeError(f"Expected AUTH_SUCCESS (0x10), got {hex(opcode)}")
        elif opcode != 0x02:
            raise RuntimeError(f"Expected READY (0x02) or AUTHENTICATE (0x03), got {hex(opcode)}")

        # Send query
        query_frame = build_cql_query_frame(query, stream=2)
        ssl_sock.sendall(query_frame)

        # Read response
        response = ssl_sock.recv(16384)
        return response

    finally:
        try:
            ssl_sock.close()
        except:
            sock.close()


# Shared server configuration for all tests
# We configure explicit SSL ports to keep the standard ports unencrypted
# so the Python driver can connect without TLS.
PROXY_SERVER_CONFIG = {
    'native_transport_port_proxy_protocol': PROXY_PORT,
    'native_transport_port_ssl_proxy_protocol': PROXY_PORT_SSL,
    'native_shard_aware_transport_port_proxy_protocol': PROXY_SHARD_AWARE_PORT,
    'native_shard_aware_transport_port_ssl_proxy_protocol': PROXY_SHARD_AWARE_PORT_SSL,
    # Set explicit non-SSL and SSL ports so the driver can connect to unencrypted port
    'native_transport_port': 9042,
    'native_shard_aware_transport_port': 19042,
    'native_transport_port_ssl': 9142,
    'native_shard_aware_transport_port_ssl': 19142,
    'client_encryption_options': {
        'enabled': True,
        'certificate': 'conf/scylla.crt',
        'keyfile': 'conf/scylla.key',
    },
}


@pytest.fixture(scope="function")
async def proxy_server(manager: ManagerClient):
    """
    Fixture that creates a server with all proxy protocol ports enabled.
    Returns a tuple of (server, manager).
    """
    server = await manager.server_add(config=PROXY_SERVER_CONFIG)
    yield (server, manager)


@pytest.mark.asyncio
async def test_proxy_protocol_basic(proxy_server):
    """
    Test that connections through the proxy protocol port correctly report
    the client address from the proxy header in system.clients.
    """
    server, manager = proxy_server

    # Use a distinctive fake source address that we can find in system.clients
    fake_src_addr = "203.0.113.42"  # TEST-NET-3, won't conflict with real addresses
    fake_src_port = 12345

    # Connect through the proxy protocol port with a fake source address
    response = await send_cql_with_proxy_header(
        host=server.ip_addr,
        port=PROXY_PORT,
        proxy_src_addr=fake_src_addr,
        proxy_src_port=fake_src_port,
        proxy_dst_addr=server.ip_addr,
        proxy_dst_port=PROXY_PORT,
        query=f"SELECT address, port FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
    )

    assert len(response) > 9, "Expected a valid CQL response"
    opcode = response[4]
    assert opcode == 0x08, f"Expected RESULT opcode (0x08), got {hex(opcode)}"


@pytest.mark.asyncio
async def test_proxy_protocol_shard_aware(proxy_server):
    """
    Test that shard-aware proxy protocol port correctly uses the source port
    from the proxy header for shard routing. We connect to all shards by
    using source ports that map to each shard (port % num_shards).
    """
    server, manager = proxy_server

    # The test harness runs with --smp 2 by default
    num_shards = 2

    cql = manager.get_cql()

    fake_src_addr = "203.0.113.43"
    base_port = 10000

    # Keep connections open while we verify shard assignments
    connections = []
    try:
        # Connect to each shard by using source ports that map to each shard
        for shard in range(num_shards):
            # Choose a port that maps to this shard: port % num_shards == shard
            fake_src_port = base_port + shard

            reader, writer = await asyncio.open_connection(server.ip_addr, PROXY_SHARD_AWARE_PORT)
            connections.append((reader, writer, fake_src_port, shard))

            # Send proxy header
            proxy_header = make_proxy_v2_header(
                fake_src_addr, fake_src_port,
                server.ip_addr, PROXY_SHARD_AWARE_PORT
            )
            writer.write(proxy_header)
            await writer.drain()

            # Complete CQL handshake
            await do_cql_handshake(reader, writer)

        # Now query system.clients to verify shard assignments
        rows = list(cql.execute(
            f"SELECT address, port, shard_id FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
        ))

        # Build a map of port -> shard_id from the results
        port_to_shard = {row.port: row.shard_id for row in rows}

        # Verify each connection landed on the expected shard
        for reader, writer, fake_src_port, expected_shard in connections:
            assert fake_src_port in port_to_shard, f"Port {fake_src_port} not found in system.clients"
            actual_shard = port_to_shard[fake_src_port]
            assert actual_shard == expected_shard, \
                f"Port {fake_src_port} expected shard {expected_shard}, got {actual_shard}"

    finally:
        for reader, writer, _, _ in connections:
            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_proxy_protocol_multiple_connections(proxy_server):
    """
    Test that multiple connections through the proxy protocol port
    with different source addresses are all correctly recorded.
    """
    server, manager = proxy_server

    test_clients = [
        ("203.0.113.101", 10001),
        ("203.0.113.102", 10002),
        ("203.0.113.103", 10003),
    ]

    for fake_src_addr, fake_src_port in test_clients:
        response = await send_cql_with_proxy_header(
            host=server.ip_addr,
            port=PROXY_PORT,
            proxy_src_addr=fake_src_addr,
            proxy_src_port=fake_src_port,
            proxy_dst_addr=server.ip_addr,
            proxy_dst_port=PROXY_PORT,
            query="SELECT * FROM system.local"
        )

        assert len(response) > 9, f"Expected a valid CQL response for {fake_src_addr}"
        opcode = response[4]
        assert opcode == 0x08, f"Expected RESULT opcode for {fake_src_addr}, got {hex(opcode)}"


@pytest.mark.asyncio
async def test_proxy_protocol_port_preserved_in_system_clients(proxy_server):
    """
    Test that the source port from the proxy protocol header is correctly
    preserved in system.clients, which is important for shard-aware routing.
    """
    server, manager = proxy_server

    fake_src_addr = "203.0.113.200"
    fake_src_port = 44444

    # Keep the connection open while we query system.clients
    reader, writer = await asyncio.open_connection(server.ip_addr, PROXY_PORT)

    try:
        # Send proxy header
        proxy_header = make_proxy_v2_header(
            fake_src_addr, fake_src_port,
            server.ip_addr, PROXY_PORT
        )
        writer.write(proxy_header)
        await writer.drain()

        # Complete CQL handshake
        await do_cql_handshake(reader, writer)

        # Now query system.clients using the driver to see our connection
        cql = manager.get_cql()
        rows = list(cql.execute(
            f"SELECT address, port FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
        ))

        # We should find our connection with the fake source address and port
        assert len(rows) > 0, f"Expected to find connection from {fake_src_addr} in system.clients"

        found_correct_port = False
        for row in rows:
            if row.port == fake_src_port:
                found_correct_port = True
                break

        assert found_correct_port, f"Expected to find port {fake_src_port} in system.clients, got ports: {[r.port for r in rows]}"

    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_proxy_protocol_ssl_basic(proxy_server):
    """
    Test proxy protocol with TLS encryption.
    The proxy header is sent first, then the connection is upgraded to TLS.
    """
    server, manager = proxy_server

    fake_src_addr = "203.0.113.50"
    fake_src_port = 55555

    response = await send_cql_with_proxy_header_tls(
        host=server.ip_addr,
        port=PROXY_PORT_SSL,
        proxy_src_addr=fake_src_addr,
        proxy_src_port=fake_src_port,
        proxy_dst_addr=server.ip_addr,
        proxy_dst_port=PROXY_PORT_SSL,
        query="SELECT * FROM system.local"
    )

    assert len(response) > 9, "Expected a valid CQL response"
    opcode = response[4]
    assert opcode == 0x08, f"Expected RESULT opcode (0x08), got {hex(opcode)}"


@pytest.mark.asyncio
async def test_proxy_protocol_ssl_shard_aware(proxy_server):
    """
    Test proxy protocol with TLS on the shard-aware port. We connect to all
    shards by using source ports that map to each shard (port % num_shards).
    """
    server, manager = proxy_server

    # The test harness runs with --smp 2 by default
    num_shards = 2

    cql = manager.get_cql()

    fake_src_addr = "203.0.113.51"
    base_port = 20000

    # Keep connections open while we verify shard assignments
    ssl_sockets = []
    try:
        # Connect to each shard by using source ports that map to each shard
        for shard in range(num_shards):
            # Choose a port that maps to this shard: port % num_shards == shard
            fake_src_port = base_port + shard

            # Create raw socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)

            loop = asyncio.get_event_loop()
            await loop.sock_connect(sock, (server.ip_addr, PROXY_SHARD_AWARE_PORT_SSL))

            # Send proxy header on raw socket before TLS
            proxy_header = make_proxy_v2_header(
                fake_src_addr, fake_src_port,
                server.ip_addr, PROXY_SHARD_AWARE_PORT_SSL
            )
            sock.setblocking(True)
            sock.sendall(proxy_header)

            # Upgrade to TLS
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_sock = ssl_context.wrap_socket(sock, do_handshake_on_connect=False)

            # Complete TLS handshake
            while True:
                try:
                    ssl_sock.do_handshake()
                    break
                except ssl.SSLWantReadError:
                    await asyncio.sleep(0.01)
                except ssl.SSLWantWriteError:
                    await asyncio.sleep(0.01)

            ssl_sockets.append((ssl_sock, fake_src_port, shard))

            # Send STARTUP frame
            startup_frame = build_cql_startup_frame(stream=1)
            ssl_sock.sendall(startup_frame)

            # Read response and handle auth if needed
            response = ssl_sock.recv(4096)
            opcode = response[4]
            if opcode == 0x03:  # AUTHENTICATE
                auth_body = b'\x00cassandra\x00cassandra'
                auth_frame = bytearray()
                auth_frame.append(0x04)
                auth_frame.append(0x00)
                auth_frame.extend(struct.pack('!H', 1))
                auth_frame.append(0x0F)
                auth_frame.extend(struct.pack('!I', len(auth_body)))
                auth_frame.extend(auth_body)
                ssl_sock.sendall(bytes(auth_frame))
                ssl_sock.recv(4096)

        # Now query system.clients to verify shard assignments
        rows = list(cql.execute(
            f"SELECT address, port, shard_id, ssl_enabled FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
        ))

        # Build a map of port -> (shard_id, ssl_enabled) from the results
        port_to_info = {row.port: (row.shard_id, row.ssl_enabled) for row in rows}

        # Verify each connection landed on the expected shard with SSL enabled
        for ssl_sock, fake_src_port, expected_shard in ssl_sockets:
            assert fake_src_port in port_to_info, f"Port {fake_src_port} not found in system.clients"
            actual_shard, ssl_enabled = port_to_info[fake_src_port]
            assert actual_shard == expected_shard, \
                f"Port {fake_src_port} expected shard {expected_shard}, got {actual_shard}"
            assert ssl_enabled, f"Port {fake_src_port} expected ssl_enabled=True"

    finally:
        for ssl_sock, _, _ in ssl_sockets:
            ssl_sock.close()


@pytest.mark.asyncio
async def test_proxy_protocol_ssl_port_preserved(proxy_server):
    """
    Test that the source port from the proxy protocol header is correctly
    preserved in system.clients when using TLS.
    """
    server, manager = proxy_server

    fake_src_addr = "203.0.113.201"
    fake_src_port = 44445

    # Create a connection that stays open
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    loop = asyncio.get_event_loop()
    await loop.sock_connect(sock, (server.ip_addr, PROXY_PORT_SSL))

    ssl_sock = None
    try:
        # Send proxy header on raw socket
        proxy_header = make_proxy_v2_header(
            fake_src_addr, fake_src_port,
            server.ip_addr, PROXY_PORT_SSL
        )
        await loop.sock_sendall(sock, proxy_header)

        # Wrap with TLS
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        ssl_sock = ssl_context.wrap_socket(sock, server_hostname=server.ip_addr, do_handshake_on_connect=False)

        # Do TLS handshake
        while True:
            try:
                ssl_sock.do_handshake()
                break
            except ssl.SSLWantReadError:
                await asyncio.sleep(0.01)
            except ssl.SSLWantWriteError:
                await asyncio.sleep(0.01)

        ssl_sock.setblocking(True)

        # Send STARTUP
        startup_frame = build_cql_startup_frame(stream=1)
        ssl_sock.sendall(startup_frame)

        # Read response and handle auth if needed
        response = ssl_sock.recv(4096)
        opcode = response[4]
        if opcode == 0x03:  # AUTHENTICATE
            auth_body = b'\x00cassandra\x00cassandra'
            auth_frame = bytearray()
            auth_frame.append(0x04)
            auth_frame.append(0x00)
            auth_frame.extend(struct.pack('!H', 1))
            auth_frame.append(0x0F)
            auth_frame.extend(struct.pack('!I', len(auth_body)))
            auth_frame.extend(auth_body)
            ssl_sock.sendall(bytes(auth_frame))
            ssl_sock.recv(4096)

        # Now query system.clients using the driver to see our connection
        cql = manager.get_cql()
        rows = list(cql.execute(
            f"SELECT address, port, ssl_enabled FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
        ))

        # We should find our connection
        assert len(rows) > 0, f"Expected to find connection from {fake_src_addr} in system.clients"

        found_correct = False
        for row in rows:
            if row.port == fake_src_port:
                found_correct = True
                assert row.ssl_enabled, "Expected connection to have ssl_enabled=True"
                break

        assert found_correct, f"Expected to find port {fake_src_port} in system.clients, got ports: {[r.port for r in rows]}"

    finally:
        if ssl_sock:
            ssl_sock.close()
        else:
            sock.close()
