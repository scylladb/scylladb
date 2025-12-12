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
from test.cqlpy import nodetool
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

@pytest.fixture
def debug_exceptions_logging(request, cql):
    def _read_level() -> str | None:
        try:
            level = nodetool.getlogginglevel(cql, "exception")
            if level:
                level = level.strip().strip('"').lower()
            return level
        except Exception as exc:
            print(f"Failed to read exception logger level: {exc}")
            return None

    def _set_and_verify(level: str) -> bool:
        try:
            nodetool.setlogginglevel(cql, "exception", level)
        except Exception as exc:
            print(f"Failed to set exception logger level to '{level}': {exc}")
            return False

        observed = _read_level()
        if observed == level:
            return True

        print(f"Exception logger level observed as '{observed}' while expecting '{level}'")
        return False

    def _restore_logging():
        if not enabled and previous_level is None:
            return

        target_level = previous_level or "info"
        _set_and_verify(target_level)

    previous_level = _read_level()
    enabled = _set_and_verify("debug")

    yield
    _restore_logging()

# If there is a protocol version mismatch, the server should
# raise a protocol error, which is counted in the metrics.
def test_protocol_version_mismatch(scylla_only, debug_exceptions_logging, request, host):
    run_count = 200
    cpp_exception_threshold = 20

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
def _protocol_error_impl(host, *, trigger_bad_batch=False, trigger_unexpected_auth=False):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, 9042))
    try:
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

<<<<<<< HEAD
||||||| parent of 807fc68dc5 (test: cqlpy: test_protocol_exceptions.py: increase cpp exceptions threshold)
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

=======
def _test_impl(host, flag):
    run_count = 200
    cpp_exception_threshold = 20

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        kwargs = {flag: True}
        _protocol_error_impl(host, **kwargs)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, f"Expected protocol errors to increase after running test with {flag}"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, f"Expected C++ protocol errors to not increase after running test with {flag}"

>>>>>>> 807fc68dc5 (test: cqlpy: test_protocol_exceptions.py: increase cpp exceptions threshold)
@pytest.fixture
def no_ssl(request):
    if request.config.getoption("--ssl"):
        pytest.skip("skipping non-SSL test on SSL-enabled run")
    yield

<<<<<<< HEAD
# Test if the error is raised when sending a malformed BATCH message
# containing an invalid BATCH kind.
def test_invalid_kind_in_batch_message(scylla_only, no_ssl, host):
    run_count = 100
    cpp_exception_threshold = 10
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# Malformed BATCH with an invalid kind triggers a protocol error.
def test_invalid_kind_in_batch_message(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_bad_batch")
=======
# Malformed BATCH with an invalid kind triggers a protocol error.
def test_invalid_kind_in_batch_message(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_bad_batch")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
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
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# Send OPTIONS during AUTHENTICATE to trigger auth-state error.
def test_unexpected_message_during_auth(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_unexpected_auth")
=======
# Send OPTIONS during AUTHENTICATE to trigger auth-state error.
def test_unexpected_message_during_auth(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_unexpected_auth")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# STARTUP with an invalid/missing string-map entry should produce a protocol error.
def test_process_startup_invalid_string_map(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_startup_invalid_string_map")
=======
# STARTUP with an invalid/missing string-map entry should produce a protocol error.
def test_process_startup_invalid_string_map(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_startup_invalid_string_map")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
    for _ in range(run_count):
        _protocol_error_impl(host, trigger_unexpected_auth=True)
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# STARTUP with unknown COMPRESSION option should produce a protocol error.
def test_unknown_compression_algorithm(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_unknown_compression")
=======
# STARTUP with unknown COMPRESSION option should produce a protocol error.
def test_unknown_compression_algorithm(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_unknown_compression")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after > protocol_exception_metrics_before, "Expected protocol errors to increase"
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# QUERY long-string truncation: declared length > provided bytes triggers protocol error.
def test_process_query_internal_malformed_query(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_query_internal_malformed_query")
=======
# QUERY long-string truncation: declared length > provided bytes triggers protocol error.
def test_process_query_internal_malformed_query(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_query_internal_malformed_query")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# QUERY options malformed: PAGE_SIZE flag set but page_size truncated triggers protocol error.
def test_process_query_internal_fail_read_options(scylla_only, no_ssl, host):
    _test_impl(host, "trigger_process_query_internal_fail_read_options")
=======
# QUERY options malformed: PAGE_SIZE flag set but page_size truncated triggers protocol error.
def test_process_query_internal_fail_read_options(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_query_internal_fail_read_options")
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)

<<<<<<< HEAD
# Test if the protocol exceptions do not decrease after running the test.
||||||| parent of c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
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
=======
# PREPARE long-string truncation: declared length > provided bytes triggers protocol error.
def test_process_prepare_malformed_query(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_prepare_malformed_query")

# EXECUTE cache-key malformed: short-bytes length > provided bytes triggers protocol error.
def test_process_execute_internal_malformed_cache_key(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_execute_internal_malformed_cache_key")

# REGISTER malformed string list: declared string length > provided bytes triggers protocol error.
def test_process_register_malformed_string_list(scylla_only, no_ssl, debug_exceptions_logging, host):
    _test_impl(host, "trigger_process_register_malformed_string_list")

# Test if the protocol exceptions do not decrease after running the test happy path.
>>>>>>> c30b326033 (test: cqlpy: test_protocol_exceptions.py: enable debug exception logging)
# This is to ensure that the protocol exceptions are not cleared or reset
# during the test execution.
def test_no_protocol_exceptions(scylla_only, no_ssl, debug_exceptions_logging, host):
    run_count = 200
    cpp_exception_threshold = 20

    cpp_exception_metrics_before = get_cpp_exceptions_metrics(host)
    protocol_exception_metrics_before = get_protocol_error_metrics(host)

    for _ in range(run_count):
        _protocol_error_impl(host)

    protocol_exception_metrics_after = get_protocol_error_metrics(host)
    assert protocol_exception_metrics_after == protocol_exception_metrics_before, "Expected protocol errors to not increase"

    cpp_exception_metrics_after = get_cpp_exceptions_metrics(host)
    assert cpp_exception_metrics_after - cpp_exception_metrics_before <= cpp_exception_threshold, "Expected C++ protocol errors to not increase"
