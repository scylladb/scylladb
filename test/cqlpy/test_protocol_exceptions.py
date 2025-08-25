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

# Many protocol errors are caused by sending malformed messages.
# It is not possible to reproduce them with the Python driver,
# so we use a low-level socket connection to send the messages.
# To avoid code duplication of this low-level code, we use a common
# implementation function with parameters. To trigger a specific
# protocol error, the appropriate trigger should be set to True.
def _protocol_error_impl(host, *, trigger_bad_batch=False, trigger_unexpected_auth=False, trigger_unknown_compression=False):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, 9042))
    try:
        if trigger_unknown_compression:
            # send STARTUP with an unknown COMPRESSION option
            bad_startup = bytearray()
            bad_startup += struct.pack("!B", 0x04)  # version 4
            bad_startup += struct.pack("!B", 0x00)  # flags
            bad_startup += struct.pack("!H", 0x01)  # stream = 1
            bad_startup += struct.pack("!B", 0x01)  # opcode = STARTUP
            # two entries in the string map: CQL_VERSION and COMPRESSION
            body = (
                b'\x00\x02'
                b'\x00\x0bCQL_VERSION\x00\x053.0.0'
                b'\x00\x0bCOMPRESSION\x00\x07invalid_compression_algorithm'
            )
            bad_startup += struct.pack("!I", len(body))
            bad_startup += body
            s.send(bad_startup)
            s.recv(4096)
            return

        # STARTUP
        startup = bytearray()
        startup += struct.pack("!B", 0x04)         # version 4
        startup += struct.pack("!B", 0x00)         # flags
        startup += struct.pack("!H", 0x01)         # stream = 1
        startup += struct.pack("!B", 0x01)         # opcode = STARTUP
        body = b'\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0'
        startup += struct.pack("!I", len(body))
        startup += body
        s.send(startup)

        # READY or AUTHENTICATE?
        frame = s.recv(4096)
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
                bad = bytearray()
                bad += struct.pack("!B", 0x04)        # version 4
                bad += struct.pack("!B", 0x00)        # flags
                bad += struct.pack("!H", 0x02)        # stream = 2
                bad += struct.pack("!B", 0x05)        # opcode = OPTIONS
                bad += struct.pack("!I", 0)           # body length = 0
                s.send(bad)
                s.recv(4096)
                return

            # send correct AUTH_RESPONSE
            auth = bytearray()
            auth += struct.pack("!B", 0x04)
            auth += struct.pack("!B", 0x00)
            auth += struct.pack("!H", 0x02)       # stream = 2
            auth += struct.pack("!B", 0x0F)       # AUTH_RESPONSE
            payload = b'\x00cassandra\x00cassandra'
            auth += struct.pack("!I", len(payload))
            auth += payload
            s.send(auth)
            # wait for AUTH_SUCCESS (0x10)
            resp = s.recv(4096)
            assert resp[4] == 0x10, f"expected AUTH_SUCCESS, got {hex(resp[4])}"

        if trigger_bad_batch:
            bad = bytearray()
            bad += struct.pack("!B", 0x04)        # version 4
            bad += struct.pack("!B", 0x00)        # flags
            bad += struct.pack("!H", 0x03)        # stream = 3
            bad += struct.pack("!B", 0x0D)        # BATCH
            bbody = bytearray()
            bbody += struct.pack("!B", 0x00)      # batch type = LOGGED
            bbody += struct.pack("!H", 0x01)      # 1 statement
            bbody += struct.pack("!B", 0xFF)      # INVALID kind = 255
            bad += struct.pack("!I", len(bbody))
            bad += bbody
            s.send(bad)
            s.recv(4096)

    finally:
        s.close()

@pytest.fixture
def no_ssl(request):
    if request.config.getoption("--ssl"):
        pytest.skip("skipping non-SSL test on SSL-enabled run")
    yield

# Test if the error is raised when sending a malformed BATCH message
# containing an invalid BATCH kind.
def test_invalid_kind_in_batch_message(scylla_only, no_ssl, host):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        _protocol_error_impl(host, trigger_bad_batch=True)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, "Expected protocol errors to increase"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"

# Test if the error is raised when sending an unexpected AUTH_RESPONSE
# message during the authentication phase.
def test_unexpected_message_during_auth(scylla_only, no_ssl, host):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        _protocol_error_impl(host, trigger_unexpected_auth=True)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, "Expected protocol errors to increase"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"

# Test if the error is raised when sending an unknown compression algorithm.
def test_unknown_compression_algorithm(scylla_only, no_ssl, host):
    run_count = 100
    cpp_exception_threshold = 10

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        _protocol_error_impl(host, trigger_unknown_compression=True)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, "Expected protocol errors to increase"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"

# Test if the protocol exceptions do not decrease after running the test.
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
