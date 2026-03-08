# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Integration tests for the CQL unified port feature.

The unified port auto-detects proxy protocol v2, shard selection headers,
and TLS vs plain CQL on a single port.

UNIFIED_PORT selects a plain endpoint that accepts trusted PPv2 headers.
UNIFIED_TLS_PORT selects a TLS endpoint. UNIFIED_PLAIN_ONLY_PORT selects an
endpoint that must reject TLS. Unset endpoints skip their related tests.

Shard selection header format (4 bytes):
  Byte 0:   Magic = 0xAA
  Byte 1:   Flags (bit 0 = TLS follows)
  Byte 2-3: Desired shard ID (uint16_t, big-endian)
"""

import errno
import os
import socket
import ssl
import struct

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

UNIFIED_PORT_MAGIC = 0xAA
SHARD_SELECT_FLAG_TLS = 0x01


@pytest.fixture
def unified_port():
    """Get the unified port from the UNIFIED_PORT environment variable.

    Skip if UNIFIED_PORT is unset or empty to avoid testing the regular CQL port.
    """
    port = os.environ.get("UNIFIED_PORT")
    if not port:
        pytest.skip("UNIFIED_PORT environment variable not set")
    return int(port)


@pytest.fixture
def unified_tls_port():
    """Select an explicitly configured TLS endpoint; TLS failures must fail the test."""
    port = os.environ.get("UNIFIED_TLS_PORT")
    if not port:
        pytest.skip("UNIFIED_TLS_PORT environment variable not set")
    return int(port)


@pytest.fixture
def unified_plain_only_port():
    """Select an explicitly configured endpoint that must reject TLS."""
    port = os.environ.get("UNIFIED_PLAIN_ONLY_PORT")
    if not port:
        pytest.skip("UNIFIED_PLAIN_ONLY_PORT environment variable not set")
    return int(port)


def build_shard_selection_header(shard_id, tls=False):
    """Build a 4-byte shard selection header."""
    flags = SHARD_SELECT_FLAG_TLS if tls else 0
    return struct.pack('>BBH', UNIFIED_PORT_MAGIC, flags, shard_id)


def build_pp2_header(src_addr='127.0.0.1', src_port=12345,
                     dst_addr='127.0.0.1', dst_port=9042):
    """Build a proxy protocol v2 PROXY header for IPv4/TCP."""
    signature = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a'
    ver_cmd = 0x21  # v2, PROXY
    fam_proto = 0x11  # AF_INET, STREAM

    src_ip = socket.inet_aton(src_addr)
    dst_ip = socket.inet_aton(dst_addr)
    addr_data = src_ip + dst_ip + struct.pack('>HH', src_port, dst_port)
    length = len(addr_data)

    header = signature + struct.pack('>BBH', ver_cmd, fam_proto, length) + addr_data
    return header


def cql_options_frame():
    """Build a CQL OPTIONS frame (protocol v4)."""
    version = 0x04
    flags = 0x00
    stream = 0x0001
    opcode = 0x05  # OPTIONS
    length = 0
    return struct.pack('>BBHBI', version, flags, stream, opcode, length)


def parse_cql_frame(data):
    """Parse a CQL response frame header. Return (version, flags, stream, opcode, length, body)."""
    if len(data) < 9:
        return None
    version, flags, stream, opcode, length = struct.unpack('>BBHBI', data[:9])
    body = data[9:9+length]
    return version, flags, stream, opcode, length, body


def parse_string_multimap(data):
    """Parse a CQL string multimap from SUPPORTED response body."""
    result = {}
    offset = 0
    if len(data) < 2:
        return result
    n_keys = struct.unpack('>H', data[offset:offset+2])[0]
    offset += 2
    for _ in range(n_keys):
        key_len = struct.unpack('>H', data[offset:offset+2])[0]
        offset += 2
        key = data[offset:offset+key_len].decode('utf-8')
        offset += key_len
        n_values = struct.unpack('>H', data[offset:offset+2])[0]
        offset += 2
        values = []
        for _ in range(n_values):
            val_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            val = data[offset:offset+val_len].decode('utf-8')
            offset += val_len
            values.append(val)
        result[key] = values
    return result


def send_options_and_get_supported(sock):
    """Send CQL OPTIONS and parse the SUPPORTED response."""
    sock.sendall(cql_options_frame())
    data = b''
    while len(data) < 9:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed before response")
        data += chunk
    frame = parse_cql_frame(data)
    if frame is None:
        raise ValueError("Invalid CQL frame")
    version, flags, stream, opcode, length, body = frame
    while len(body) < length:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed before full body")
        body += chunk
    if opcode != 0x06:  # SUPPORTED
        raise ValueError(f"Expected SUPPORTED (0x06), got 0x{opcode:02x}")
    return parse_string_multimap(body)


def test_legacy_plain_cql(host, unified_port):
    """Connect a legacy plain CQL client to the unified port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        supported = send_options_and_get_supported(sock)
        assert 'CQL_VERSION' in supported
        assert 'SCYLLA_SHARD' in supported
        assert 'SCYLLA_NR_SHARDS' in supported
    finally:
        sock.close()


def test_legacy_plain_cql_ipv6(host, unified_port):
    """Connect a legacy plain CQL client to the unified port over IPv6."""
    # Use the host fixture but attempt IPv6 connection
    ipv6_host = '::1' if host in ('127.0.0.1', 'localhost', '::1') else host
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        try:
            sock.connect((ipv6_host, unified_port))
        except OSError:
            pytest.skip("IPv6 not available or unified port not listening on IPv6")
        supported = send_options_and_get_supported(sock)
        assert 'CQL_VERSION' in supported
    finally:
        sock.close()


def test_supported_advertises_unified_port(host, unified_port):
    """Check that the SUPPORTED response includes SCYLLA_UNIFIED_PORT."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        supported = send_options_and_get_supported(sock)
        assert 'SCYLLA_UNIFIED_PORT' in supported, \
            f"SCYLLA_UNIFIED_PORT not in SUPPORTED response. Keys: {list(supported.keys())}"
    finally:
        sock.close()


def test_shard_selection_wraps_modulo(host, unified_port):
    """Shard ID >= nr_shards should wrap with modulo."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        supported = send_options_and_get_supported(sock)
        nr_shards = int(supported['SCYLLA_NR_SHARDS'][0])
    finally:
        sock.close()

    target = nr_shards + 1
    expected = target % nr_shards
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        header = build_shard_selection_header(target, tls=False)
        sock.sendall(header)
        supported = send_options_and_get_supported(sock)
        actual_shard = int(supported['SCYLLA_SHARD'][0])
        assert actual_shard == expected, \
            f"Expected shard {expected} (from {target} % {nr_shards}), got {actual_shard}"
    finally:
        sock.close()


def test_pp2_plus_legacy_cql(host, unified_port):
    """Consume a PPv2 header before a plain CQL OPTIONS request."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        pp_header = build_pp2_header(
            src_addr='10.0.0.1', src_port=54321,
            dst_addr='10.0.0.2', dst_port=unified_port
        )
        sock.sendall(pp_header)
        supported = send_options_and_get_supported(sock)
        assert 'CQL_VERSION' in supported
    finally:
        sock.close()

def test_pp2_plus_shard_selection(host, unified_port):
    """Send a PP v2 header followed by a shard selection header."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        supported = send_options_and_get_supported(sock)
        nr_shards = int(supported['SCYLLA_NR_SHARDS'][0])
    finally:
        sock.close()

    target_shard = nr_shards - 1  # last shard
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        pp_header = build_pp2_header(
            src_addr='10.0.0.1', src_port=54321,
            dst_addr='10.0.0.2', dst_port=unified_port
        )
        shard_header = build_shard_selection_header(target_shard, tls=False)
        sock.sendall(pp_header + shard_header)
        supported = send_options_and_get_supported(sock)
        actual_shard = int(supported['SCYLLA_SHARD'][0])
        assert actual_shard == target_shard, \
            f"Expected shard {target_shard}, got {actual_shard}"
    finally:
        sock.close()


def test_pp2_plus_shard_selection_plus_tls(host, unified_tls_port):
    """Send PP v2, then a shard selection header with the TLS flag, and complete TLS and CQL."""
    target_shard = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        sock.connect((host, unified_tls_port))
        pp_header = build_pp2_header(
            src_addr='10.0.0.1', src_port=54321,
            dst_addr='10.0.0.2', dst_port=unified_tls_port
        )
        shard_header = build_shard_selection_header(target_shard, tls=True)
        sock.sendall(pp_header + shard_header)
        with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
            supported = send_options_and_get_supported(tls_sock)
            assert 'CQL_VERSION' in supported
            actual_shard = int(supported['SCYLLA_SHARD'][0])
            assert actual_shard == target_shard, \
                f"Expected shard {target_shard}, got {actual_shard}"
    finally:
        sock.close()


def test_all_shards_reachable(host, unified_port):
    """Verify every shard 0..N-1 can be targeted via shard selection."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, unified_port))
        supported = send_options_and_get_supported(sock)
        nr_shards = int(supported['SCYLLA_NR_SHARDS'][0])
    finally:
        sock.close()

    reached_shards = set()
    for target_shard in range(nr_shards):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, unified_port))
            header = build_shard_selection_header(target_shard, tls=False)
            sock.sendall(header)
            supported = send_options_and_get_supported(sock)
            actual_shard = int(supported['SCYLLA_SHARD'][0])
            reached_shards.add(actual_shard)
            assert actual_shard == target_shard
        finally:
            sock.close()

    assert reached_shards == set(range(nr_shards)), \
        f"Not all shards reachable. Reached: {reached_shards}, expected: {set(range(nr_shards))}"


def test_legacy_tls(host, unified_tls_port):
    """Complete TLS and CQL on the explicitly configured TLS endpoint."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        sock.connect((host, unified_tls_port))
        with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
            supported = send_options_and_get_supported(tls_sock)
            assert 'CQL_VERSION' in supported
    finally:
        sock.close()


def test_shard_selection_with_tls(host, unified_tls_port):
    """Shard selection header with TLS flag -- TLS handshake on target shard.

    This test requires TLS to be configured on the server.
    """
    target_shard = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        sock.connect((host, unified_tls_port))
        header = build_shard_selection_header(target_shard, tls=True)
        sock.sendall(header)
        with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
            supported = send_options_and_get_supported(tls_sock)
            actual_shard = int(supported['SCYLLA_SHARD'][0])
            assert actual_shard == target_shard, \
                f"Expected shard {target_shard}, got {actual_shard}"
    finally:
        sock.close()


def test_driver_connection(host, unified_port):
    """Check that the standard cassandra-driver can connect to the unified port."""
    cluster = Cluster(
        contact_points=[host],
        port=unified_port,
        protocol_version=4,
        load_balancing_policy=RoundRobinPolicy(),
    )
    try:
        session = cluster.connect()
        result = session.execute("SELECT now() FROM system.local")
        assert result is not None
    finally:
        cluster.shutdown()


# --- Negative tests ---


def _assert_rejected_connection(host, port, payload):
    # Establish TCP before allowing a peer reset to count as protocol rejection.
    with socket.create_connection((host, port), timeout=5) as sock:
        try:
            sock.sendall(payload)
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError as error:
                # The server can reject the header before this half-close.
                if error.errno != errno.ENOTCONN:
                    raise
            assert sock.recv(4096) == b'', "Expected server to close the rejected connection"
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass


def test_malformed_shard_selection_header(host, unified_port):
    """Reject a shard header truncated after its flags byte."""
    _assert_rejected_connection(host, unified_port, struct.pack('BB', UNIFIED_PORT_MAGIC, 0x00))


def test_truncated_pp2_header(host, unified_port):
    """Reject a PPv2 fixed header truncated after its family byte."""
    truncated = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a\x21\x11'
    _assert_rejected_connection(host, unified_port, truncated)


def test_invalid_pp2_signature(host, unified_port):
    """Reject an invalid PPv2 signature before an otherwise valid CQL request."""
    header = bytearray(build_pp2_header())
    header[1] ^= 0xff
    _assert_rejected_connection(host, unified_port, header + cql_options_frame())


def test_pp2_unsupported_address_family(host, unified_port):
    """Reject a PPv2 header with the unsupported AF_UNIX address family."""
    signature = b'\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a'
    address_length = 216
    header = signature + struct.pack('>BBH', 0x21, 0x31, address_length)
    _assert_rejected_connection(host, unified_port, header + b'\x00' * address_length + cql_options_frame())


def test_tls_flag_rejected_by_plain_only_listener(host, unified_plain_only_port):
    """The explicitly configured plain-only endpoint must reject a TLS handshake."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, unified_plain_only_port), timeout=5) as sock:
        with pytest.raises((ssl.SSLError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            sock.sendall(build_shard_selection_header(0, tls=True))
            with ctx.wrap_socket(sock, server_hostname=host):
                pass
