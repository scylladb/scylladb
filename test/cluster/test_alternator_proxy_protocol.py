# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

"""
Tests for Alternator (DynamoDB API) ports with proxy protocol v2 support.

These tests verify that when Scylla is configured with Alternator proxy protocol
ports, clients can connect using the PROXY protocol v2 header.
"""

import asyncio
import datetime
import hashlib
import hmac
import json
import logging
import socket
import ssl
import struct
from typing import Optional

import aiohttp
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)

# Proxy protocol v2 signature (12 bytes)
PROXY_V2_SIGNATURE = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a'

# Port numbers
ALTERNATOR_PORT = 8000
ALTERNATOR_HTTPS_PORT = 8043
ALTERNATOR_PROXY_PORT = 28000
ALTERNATOR_HTTPS_PROXY_PORT = 28043


def make_proxy_v2_header(src_addr: str, src_port: int, dst_addr: str, dst_port: int) -> bytes:
    """
    Construct a proxy protocol v2 header for IPv4 TCP connections.
    See: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
    """
    header = bytearray()
    header.extend(PROXY_V2_SIGNATURE)
    header.append(0x21)  # Version 2, PROXY command
    header.append(0x11)  # AF_INET, STREAM/TCP
    header.extend(struct.pack('!H', 12))  # Address block length
    header.extend(socket.inet_aton(src_addr))
    header.extend(socket.inet_aton(dst_addr))
    header.extend(struct.pack('!H', src_port))
    header.extend(struct.pack('!H', dst_port))
    return bytes(header)


def sign_request(method: str, host: str, uri: str, headers: dict, body: str,
                 access_key: str, secret_key: str,
                 region: str = 'us-east-1', service: str = 'dynamodb') -> dict:
    """Sign an AWS request using Signature Version 4."""
    t = datetime.datetime.now(datetime.UTC)
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')

    canonical_headers = f'host:{host}\nx-amz-date:{amz_date}\n'
    signed_headers = 'host;x-amz-date'
    payload_hash = hashlib.sha256(body.encode('utf-8')).hexdigest()
    canonical_request = f'{method}\n{uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}'

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = f'{date_stamp}/{region}/{service}/aws4_request'
    string_to_sign = f'{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()}'

    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    k_date = sign(('AWS4' + secret_key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, service)
    k_signing = sign(k_service, 'aws4_request')
    signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    new_headers = dict(headers)
    new_headers['x-amz-date'] = amz_date
    new_headers['Authorization'] = f'{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}'
    return new_headers


class ProxyProtocolConnector(aiohttp.TCPConnector):
    """
    Custom aiohttp connector that injects a proxy protocol v2 header
    immediately after establishing the TCP connection, before any HTTP traffic.
    """

    def __init__(self, proxy_src_addr: str, proxy_src_port: int,
                 ssl_context: Optional[ssl.SSLContext] = None, **kwargs):
        # For TLS connections, we need to disable aiohttp's SSL handling
        # so we can inject the proxy header before the TLS handshake.
        # If ssl_context is None (HTTP), we must pass ssl=False to avoid default SSL validation.
        ssl_param = ssl_context if ssl_context is not None else False
        super().__init__(ssl=ssl_param, **kwargs)
        self._proxy_src_addr = proxy_src_addr
        self._proxy_src_port = proxy_src_port
        self._custom_ssl_context = ssl_context

    async def _create_connection(self, req, traces, timeout):
        """Override to inject proxy protocol header after TCP connect."""
        # Get the target host and port
        host = req.url.host
        port = req.url.port or (443 if req.url.scheme == 'https' else 80)

        # Create raw TCP connection
        _, protocol = await self._loop.create_connection(
            lambda: aiohttp.client_proto.ResponseHandler(self._loop),
            host, port
        )

        # Send proxy protocol header immediately
        proxy_header = make_proxy_v2_header(
            self._proxy_src_addr, self._proxy_src_port, host, port
        )
        protocol.transport.write(proxy_header)

        # If TLS is needed, upgrade the connection
        if self._custom_ssl_context is not None:
            transport = await self._loop.start_tls(
                protocol.transport, protocol, self._custom_ssl_context,
                server_hostname=host
            )
            protocol.transport = transport

        return protocol


async def send_alternator_request(
    host: str,
    port: int,
    operation: str,
    request_body: dict,
    use_ssl: bool = False,
    proxy_src_addr: Optional[str] = None,
    proxy_src_port: Optional[int] = None,
    access_key: str = 'alternator',
    secret_key: str = 'secret_pass'
) -> tuple[int, dict]:
    """
    Send an Alternator request using aiohttp.

    If proxy_src_addr and proxy_src_port are provided, a proxy protocol v2 header
    is injected after TCP connect (and before TLS handshake for HTTPS).

    Returns (status_code, response_body_dict).
    """
    body = json.dumps(request_body)
    headers = {
        'Content-Type': 'application/x-amz-json-1.0',
        'X-Amz-Target': f'DynamoDB_20120810.{operation}',
    }
    signed_headers = sign_request('POST', f'{host}:{port}', '/', headers, body, access_key, secret_key)

    scheme = 'https' if use_ssl else 'http'
    url = f'{scheme}://{host}:{port}/'

    if proxy_src_addr is not None and proxy_src_port is not None:
        # Use custom connector that injects proxy protocol header
        ssl_context = None
        if use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        connector = ProxyProtocolConnector(proxy_src_addr, proxy_src_port, ssl_context=ssl_context)
    else:
        # Regular connection without proxy protocol
        connector = aiohttp.TCPConnector(ssl=False) if use_ssl else None

    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.post(url, headers=signed_headers, data=body) as resp:
            resp_body = await resp.text()
            return resp.status, json.loads(resp_body) if resp_body else {}


# Server configuration for all proxy protocol tests
ALTERNATOR_PROXY_SERVER_CONFIG = {
    'alternator_port': ALTERNATOR_PORT,
    'alternator_https_port': ALTERNATOR_HTTPS_PORT,
    'alternator_port_proxy_protocol': ALTERNATOR_PROXY_PORT,
    'alternator_https_port_proxy_protocol': ALTERNATOR_HTTPS_PROXY_PORT,
    'alternator_write_isolation': 'only_rmw_uses_lwt',
    'alternator_enforce_authorization': False,
    'alternator_encryption_options': {
        'certificate': 'conf/scylla.crt',
        'keyfile': 'conf/scylla.key',
    },
}


@pytest.fixture(scope="function")
async def alternator_proxy_server(manager: ManagerClient):
    """Fixture that creates a server with Alternator proxy protocol ports enabled."""
    server = await manager.server_add(config=ALTERNATOR_PROXY_SERVER_CONFIG)
    yield (server, manager)


@pytest.mark.asyncio
async def test_alternator_proxy_protocol_basic(alternator_proxy_server):
    """Test that connections through the Alternator proxy protocol port work."""
    server, _ = alternator_proxy_server

    status_code, response = await send_alternator_request(
        host=server.ip_addr,
        port=ALTERNATOR_PROXY_PORT,
        operation='ListTables',
        request_body={},
        proxy_src_addr="203.0.113.42",
        proxy_src_port=12345
    )

    assert status_code == 200, f"Expected status 200, got {status_code}: {response}"
    assert 'TableNames' in response, f"Expected TableNames in response: {response}"


@pytest.mark.asyncio
async def test_alternator_proxy_protocol_multiple_connections(alternator_proxy_server):
    """Test multiple connections with different source addresses."""
    server, _ = alternator_proxy_server

    test_clients = [
        ("203.0.113.101", 10001),
        ("203.0.113.102", 10002),
        ("203.0.113.103", 10003),
    ]

    for fake_src_addr, fake_src_port in test_clients:
        status_code, response = await send_alternator_request(
            host=server.ip_addr,
            port=ALTERNATOR_PROXY_PORT,
            operation='ListTables',
            request_body={},
            proxy_src_addr=fake_src_addr,
            proxy_src_port=fake_src_port
        )
        assert status_code == 200, f"Expected status 200 for {fake_src_addr}, got {status_code}: {response}"
        assert 'TableNames' in response, f"Expected TableNames in response for {fake_src_addr}: {response}"


@pytest.mark.asyncio
async def test_alternator_proxy_protocol_ssl_basic(alternator_proxy_server):
    """Test proxy protocol with TLS encryption."""
    server, _ = alternator_proxy_server

    status_code, response = await send_alternator_request(
        host=server.ip_addr,
        port=ALTERNATOR_HTTPS_PROXY_PORT,
        operation='ListTables',
        request_body={},
        use_ssl=True,
        proxy_src_addr="203.0.113.50",
        proxy_src_port=55555
    )

    assert status_code == 200, f"Expected status 200, got {status_code}: {response}"
    assert 'TableNames' in response, f"Expected TableNames in response: {response}"


@pytest.mark.asyncio
async def test_alternator_regular_port_still_works(alternator_proxy_server):
    """Test that the regular Alternator port still works when proxy protocol ports are configured."""
    server, _ = alternator_proxy_server

    status_code, response = await send_alternator_request(
        host=server.ip_addr,
        port=ALTERNATOR_PORT,
        operation='ListTables',
        request_body={}
    )

    assert status_code == 200, f"Expected status 200, got {status_code}: {response}"
    assert 'TableNames' in response, f"Expected TableNames in response: {response}"


@pytest.mark.asyncio
async def test_alternator_proxy_header_to_regular_port_fails(alternator_proxy_server):
    """Test that sending a proxy protocol header to the regular port fails.

    The regular port does not expect proxy protocol, so the server should
    reject the connection or fail to parse the request.
    """
    server, _ = alternator_proxy_server

    # Sending proxy header to regular port should fail - the server will
    # interpret the proxy header bytes as HTTP and fail to parse
    with pytest.raises(Exception):
        await send_alternator_request(
            host=server.ip_addr,
            port=ALTERNATOR_PORT,
            operation='ListTables',
            request_body={},
            proxy_src_addr="203.0.113.42",
            proxy_src_port=12345
        )


@pytest.mark.asyncio
async def test_alternator_no_proxy_header_to_proxy_port_fails(alternator_proxy_server):
    """Test that sending a request without proxy header to the proxy port fails.

    The proxy protocol port expects a proxy protocol header first, so sending
    raw HTTP should fail.
    """
    server, _ = alternator_proxy_server

    # Sending without proxy header to proxy port should fail - the server
    # expects a proxy protocol header first
    with pytest.raises(Exception):
        await send_alternator_request(
            host=server.ip_addr,
            port=ALTERNATOR_PROXY_PORT,
            operation='ListTables',
            request_body={}
            # No proxy_src_addr/proxy_src_port means no proxy header
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("use_ssl", [False, True], ids=["http", "https"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_alternator_proxy_protocol_address_in_system_clients(alternator_proxy_server, use_ssl):
    """Test that the source address from the proxy protocol header is correctly
    reported in system.clients.

    Uses error injection to pause the Alternator request so we can query
    system.clients while the request is in flight. Tests both HTTP and HTTPS
    variants and verifies the ssl_enabled column is set correctly.
    """
    server, manager = alternator_proxy_server

    fake_src_addr = "203.0.113.100"
    fake_src_port = 54321
    port = ALTERNATOR_HTTPS_PROXY_PORT if use_ssl else ALTERNATOR_PROXY_PORT

    async def wait_for_injection_waiting(injection_name: str, timeout: float = 10.0):
        """Poll until the injection has set 'waiting' parameter, indicating it's paused."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            injection_state = await manager.api.get_injection(server.ip_addr, injection_name)
            # Each shard returns an object with 'enabled' and 'parameters' (array of {key, value})
            for state in injection_state:
                if state.get('enabled'):
                    for param in state.get('parameters', []):
                        if param.get('key') == 'waiting' and str(param.get('value')).lower() in ('true', '1'):
                            return
            await asyncio.sleep(0.01)
        raise TimeoutError(f"Injection {injection_name} did not reach waiting state within {timeout}s")

    # Enable the error injection that pauses ListTables execution
    async with inject_error(manager.api, server.ip_addr, "alternator_list_tables"):
        # Start the request in the background - it will pause at the injection point
        request_task = asyncio.create_task(send_alternator_request(
            host=server.ip_addr,
            port=port,
            operation='ListTables',
            request_body={},
            use_ssl=use_ssl,
            proxy_src_addr=fake_src_addr,
            proxy_src_port=fake_src_port
        ))

        def log_task_error(t):
            if t.done() and t.exception():
                print(f"DEBUG: Request task failed with: {t.exception()}")
        request_task.add_done_callback(log_task_error)

        # Wait until the injection has paused the request
        await wait_for_injection_waiting("alternator_list_tables")

        # Query system.clients while the request is paused
        cql = manager.get_cql()
        rows = list(cql.execute(
            f"SELECT address, port, ssl_enabled FROM system.clients WHERE address = '{fake_src_addr}' ALLOW FILTERING"
        ))

        # Send message to release the injection and let the request complete
        await manager.api.message_injection(server.ip_addr, "alternator_list_tables")

        # Wait for the request to complete
        status_code, response = await request_task

        # Verify the request succeeded
        assert status_code == 200, f"Expected status 200, got {status_code}: {response}"
        assert 'TableNames' in response, f"Expected TableNames in response: {response}"

        # Verify we found the fake source address in system.clients
        assert len(rows) > 0, f"Expected to find connection from {fake_src_addr} in system.clients"
        matching_rows = [row for row in rows if row.port == fake_src_port]
        assert len(matching_rows) > 0, \
            f"Expected to find port {fake_src_port} in system.clients, got {[row.port for row in rows]}"
        # Verify ssl_enabled matches whether we used SSL
        assert matching_rows[0].ssl_enabled == use_ssl, \
            f"Expected ssl_enabled={use_ssl}, got {matching_rows[0].ssl_enabled}"
