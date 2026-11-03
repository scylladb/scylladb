# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Tests for how the CQL server handles connections to its TLS-encrypted port.
"""

import asyncio
import socket
import struct
import time

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

CQL_TLS_PORT = 9142

# Keep the standard ports unencrypted so the Python driver can connect without TLS.
TLS_SERVER_CONFIG = {
    'native_transport_port': 9042,
    'native_shard_aware_transport_port': 19042,
    'native_transport_port_ssl': CQL_TLS_PORT,
    'native_shard_aware_transport_port_ssl': 19142,
    'client_encryption_options': {
        'enabled': True,
        'certificate': 'conf/scylla.crt',
        'keyfile': 'conf/scylla.key',
    },
}

# A complete TLS 1.2 record carrying a fatal handshake_failure alert.
TLS_FATAL_ALERT = b'\x15\x03\x03\x00\x02\x02\x28'


async def wait_for_tls_client(cql, port: int, present: bool) -> None:
    """Waits until a TLS connection from local `port` is (or is no longer) listed in system.clients."""
    async def check():
        rows = await cql.run_async('SELECT port, ssl_enabled FROM system.clients')
        listed = any(r.port == port and r.ssl_enabled for r in rows)
        return True if listed == present else None
    await wait_for(check, time.time() + 60, period=0.1)


async def connect_and_close(cql, loop, address, before_close=None) -> None:
    """Opens a TCP connection to the TLS port, waits until the server sees it, then closes it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setblocking(False)
        await loop.sock_connect(sock, (address, CQL_TLS_PORT))
        port = sock.getsockname()[1]
        await wait_for_tls_client(cql, port, present=True)
        if before_close:
            await before_close(sock)
    await wait_for_tls_client(cql, port, present=False)


async def test_tls_connection_closed_before_handshake_is_not_logged(manager: ScyllaClusterManager):
    """
    Reproduces SCYLLADB-5676: a client (e.g. a TCP health check or a load balancer) that
    connects to the TLS port and closes the connection before sending a TLS ClientHello
    must not be logged, the same as any other client disconnect. A real TLS failure, such
    as a client aborting the handshake with a fatal alert, must still be logged.
    """
    server = await manager.server_add(config=TLS_SERVER_CONFIG)
    cql = manager.get_cql()
    log = await manager.server_open_log(server.server_id)
    loop = asyncio.get_running_loop()
    mark = await log.mark()

    # The client closes gracefully (FIN) without sending anything.
    await connect_and_close(cql, loop, server.ip_addr)

    # The client resets the connection (RST) without sending anything.
    async def reset(sock):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    await connect_and_close(cql, loop, server.ip_addr, before_close=reset)

    # The client aborts the handshake. The server closes the connection on its own.
    async def send_fatal_alert(sock):
        await loop.sock_sendall(sock, TLS_FATAL_ALERT)
        await wait_for_tls_client(cql, sock.getsockname()[1], present=False)
    await connect_and_close(cql, loop, server.ip_addr, before_close=send_fatal_alert)

    # Each connection is gone on the server before the next one opens, so by the time the
    # fatal alert is logged, an error logged for an earlier connection is in the log too.
    await log.wait_for('Inspecting TLS connection failed', from_mark=mark, timeout=60)
    errors = await log.grep('Inspecting TLS connection failed', from_mark=mark)
    assert len(errors) == 1, \
        'Expected only the fatal alert to be logged, got:\n' + ''.join(line for line, _ in errors)
