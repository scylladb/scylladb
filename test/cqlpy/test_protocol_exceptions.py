# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from cassandra.cluster import NoHostAvailable
from contextlib import contextmanager
import pytest
import re
import requests
import socket
import struct
from test.cqlpy.util import cql_session

def get_protocol_error_metrics(host) -> int:
    result = 0
    metrics = requests.get(f"http://{host}:9180/metrics").text
    pattern = re.compile(r'^scylla_transport_cql_errors_total\{shard="\d+",type="protocol_error"\} (\d+)')

    for metric_line in metrics.split('\n'):
        match = pattern.match(metric_line)
        if match:
            count = int(match.group(1))
            result += count

    return result

def get_cpp_exceptions_metrics(host) -> int:
    result = 0
    metrics = requests.get(f"http://{host}:9180/metrics").text
    pattern = re.compile(r'^scylla_reactor_cpp_exceptions\{shard="\d+"\} (\d+)')

    for metric_line in metrics.split('\n'):
        match = pattern.match(metric_line)
        if match:
            count = int(match.group(1))
            result += count

    return result

@contextmanager
def cql_with_protocol(host_str, port, creds, protocol_version):
    try:
        with cql_session(
                host=host_str,
                port=port,
                is_ssl=creds["ssl"],
                username=creds["username"],
                password=creds["password"],
                protocol_version=protocol_version,
        ) as session:
            yield session
            session.shutdown()
    except NoHostAvailable:
        yield None

def try_connect(host, port, creds, protocol_version):
    with cql_with_protocol(host, port, creds, protocol_version) as session:
        return 1 if session else 0

# If there is a protocol version mismatch, the server should
# raise a protocol error, which is counted in the metrics.
def test_protocol_version_mismatch(scylla_only, request, host):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    port = request.config.getoption("--port")
    # Use the default superuser credentials, which work for both Scylla and Cassandra
    creds = {
        "ssl": request.config.getoption("--ssl"),
        "username": request.config.getoption("--auth_username") or "cassandra",
        "password": request.config.getoption("--auth_password") or "cassandra",
    }

    successful_session_count = try_connect(host, port, creds, protocol_version=4)
    assert successful_session_count == 1, "Expected to connect successfully with protocol version 4"

    for _ in range(run_count):
        successful_session_count = try_connect(host, port, creds, protocol_version=42)
        assert successful_session_count == 0, "Expected to fail connecting with protocol version 42"

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, "Expected protocol errors to increase after the test"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase after the test"

def _build_frame(*, opcode: int, stream: int, body: bytes) -> bytearray:
    frame = bytearray()
    frame += struct.pack("!B", 0x04)         # version 4
    frame += struct.pack("!B", 0x00)         # flags
    frame += struct.pack("!H", stream)       # stream
    frame += struct.pack("!B", opcode)       # opcode
    frame += struct.pack("!I", len(body))  # body length
    frame += body
    return frame

def _send_frame(sock: socket.socket, *, opcode: int, stream: int, body: bytes) -> None:
    sock.send(_build_frame(opcode=opcode, stream=stream, body=body))

def _recv_frame(sock: socket.socket) -> bytes:
    return sock.recv(4096)

# Many protocol errors are caused by sending malformed messages.
# It is not possible to reproduce them with the Python driver,
# so we use a low-level socket connection to send the messages.
# To avoid code duplication of this low-level code, we use a common
# implementation function with parameters. To trigger a specific
# protocol error, the appropriate trigger should be set to True.
def _protocol_error_impl(
        host, *,
        trigger_bad_batch=False,
        trigger_unexpected_auth=False,
        trigger_process_startup_invalid_string_map=False,
        trigger_unknown_compression=False,
        trigger_process_query_internal_malformed_query=False,
        trigger_process_query_internal_fail_read_options=False,
        trigger_process_prepare_malformed_query=False,
        trigger_process_execute_internal_malformed_cache_key=False,
        trigger_process_register_malformed_string_list=False):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, 9042))
    try:
        if trigger_process_startup_invalid_string_map:
            # STARTUP opcode = 0x01.
            # Body: map count = 1 (uint16), but no entries -> truncated
            body = b'\x00\x01'
            _send_frame(s, opcode=0x01, stream=1, body=body)
            _recv_frame(s)
            return

        if trigger_unknown_compression:
            # send STARTUP with an unknown COMPRESSION option
            # two entries in the string map: CQL_VERSION and COMPRESSION
            body = (
                b'\x00\x02'
                b'\x00\x0bCQL_VERSION\x00\x053.0.0'
                b'\x00\x0bCOMPRESSION\x00\x07invalid_compression_algorithm'
            )
            _send_frame(s, opcode=0x01, stream=1, body=body)
            _recv_frame(s)
            return

        # STARTUP
        body = b'\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0'
        _send_frame(s, opcode=0x01, stream=1, body=body)
        frame = _recv_frame(s)

        # READY or AUTHENTICATE?
        op = frame[4]
        assert op in (0x02, 0x03), f"expected READY(2) or AUTHENTICATE(3), got {hex(op)}"

        if op == 0x02:
            # READY path
            if trigger_unexpected_auth:
                pytest.skip("server not configured with authentication, skipping unexpected auth test")
        elif op == 0x03:
            # AUTHENTICATE path
            if trigger_unexpected_auth:
                # send OPTIONS to trigger auth‐state exception
                _send_frame(s, opcode=0x05, stream=2, body=b'')
                _recv_frame(s)
                return

            # send correct AUTH_RESPONSE
            body = b'\x00cassandra\x00cassandra'
            _send_frame(s, opcode=0x0F, stream=2, body=body)
            # wait for AUTH_SUCCESS (0x10)
            resp = _recv_frame(s)
            assert resp[4] == 0x10, f"expected AUTH_SUCCESS, got {hex(resp[4])}"

        if trigger_bad_batch:
            # BATCH opcode = 0x0D.
            # Body: batch type = LOGGED (0), 1 statement, but invalid kind = 255
            body = bytearray()
            body += struct.pack("!B", 0x00)      # batch type = LOGGED
            body += struct.pack("!H", 0x01)      # 1 statement
            body += struct.pack("!B", 0xFF)      # INVALID kind = 255
            # BATCH opcode 0x0D
            _send_frame(s, opcode=0x0D, stream=3, body=body)
            _recv_frame(s)
            return
        
        if trigger_process_query_internal_malformed_query:
            # QUERY opcode = 0x07.
            # Body: long-string length (uint32) = 100, but send only 2 bytes -> truncated
            long_string_len = 100
            body = struct.pack("!I", long_string_len) + b"AB"
            _send_frame(s, opcode=0x07, stream=2, body=body)
            resp = _recv_frame(s)
            # Expect ERROR frame (opcode 0x00)
            assert bool(resp) and resp[4] == 0x00
            return

        if trigger_process_query_internal_fail_read_options:
            # QUERY opcode = 0x07
            # Body: long-string query (uint32 len) + options; PAGE_SIZE flag set but page_size truncated (only 2 bytes provided instead of 4)
            query = b"SELECT 1"
            long_len = struct.pack("!I", len(query))
            # options: consistency (uint16) + flags (byte with PAGE_SIZE bit = 0x04) + truncated page_size (2 bytes only)
            options = struct.pack("!H", 0x0001) + struct.pack("!B", 0x04) + b'\x00\x10'  # only 2 bytes instead of 4
            body = long_len + query + options
            _send_frame(s, opcode=0x07, stream=2, body=body)
            resp = _recv_frame(s)
            # Expect ERROR frame (opcode 0x00)
            assert bool(resp) and resp[4] == 0x00
            return
        
        if trigger_process_prepare_malformed_query:
            # PREPARE opcode = 0x09.
            # Body: long-string length (uint32) = 100, but send only 2 bytes -> truncated
            long_string_len = 100
            body = struct.pack("!I", long_string_len) + b"AB" 
            _send_frame(s, opcode=0x09, stream=2, body=body)
            resp = _recv_frame(s)
            # Expect ERROR frame (opcode 0x00)
            assert bool(resp) and resp[4] == 0x00
            return
        
        if trigger_process_execute_internal_malformed_cache_key:
            # EXECUTE opcode = 0x0A.
            # Body: short-bytes length (uint16) = 5, but send only 3 bytes -> truncated id
            declared_len = 5
            body = struct.pack("!H", declared_len) + b'ABC'  # actual = 2 + 3 = 5 bytes
            _send_frame(s, opcode=0x0A, stream=2, body=body)
            resp = _recv_frame(s)
            # Expect ERROR frame (opcode 0x00)
            assert bool(resp) and resp[4] == 0x00
            return
        
        if trigger_process_register_malformed_string_list:
            # REGISTER opcode = 0x0B
            # Body: string list count = 1 (uint16) then a string with declared length = 5 but only 3 bytes provided -> truncated
            body = b'\x00\x01' + b'\x00\x05' + b'ABC'
            _send_frame(s, opcode=0x0B, stream=2, body=body)
            resp = _recv_frame(s)
            # Expect ERROR frame (opcode 0x00)
            assert bool(resp) and resp[4] == 0x00

    finally:
        s.close()

def _test_impl(host, flag):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        kwargs = {flag: True}
        _protocol_error_impl(host, **kwargs)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, f"Expected protocol errors to increase after running test with {flag}"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, f"Expected C++ protocol errors to not increase after running test with {flag}"

@pytest.fixture
def no_ssl(request):
    if request.config.getoption("--ssl"):
        pytest.skip("skipping non-SSL test on SSL-enabled run")
    yield

# Malformed BATCH with an invalid kind triggers a protocol error.
def test_invalid_kind_in_batch_message(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_bad_batch")

# Send OPTIONS during AUTHENTICATE to trigger auth-state error.
def test_unexpected_message_during_auth(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_unexpected_auth")

# STARTUP with an invalid/missing string-map entry should produce a protocol error.
def test_process_startup_invalid_string_map(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_startup_invalid_string_map")

# STARTUP with unknown COMPRESSION option should produce a protocol error.
def test_unknown_compression_algorithm(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_unknown_compression")

# QUERY long-string truncation: declared length > provided bytes triggers protocol error.
def test_process_query_internal_malformed_query(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_query_internal_malformed_query")

# QUERY options malformed: PAGE_SIZE flag set but page_size truncated triggers protocol error.
def test_process_query_internal_fail_read_options(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_query_internal_fail_read_options")

# PREPARE long-string truncation: declared length > provided bytes triggers protocol error.
def test_process_prepare_malformed_query(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_prepare_malformed_query")

# EXECUTE cache-key malformed: short-bytes length > provided bytes triggers protocol error.
def test_process_execute_internal_malformed_cache_key(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_execute_internal_malformed_cache_key")

# REGISTER malformed string list: declared string length > provided bytes triggers protocol error.
def test_process_register_malformed_string_list(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_register_malformed_string_list")

# Test if the protocol exceptions do not decrease after running the test happy path.
# This is to ensure that the protocol exceptions are not cleared or reset
# during the test execution.
def test_no_protocol_exceptions(scylla_only, no_ssl, host):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        _protocol_error_impl(host)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after == protocol_exception_metrics_before, "Expected protocol errors to not increase"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"
