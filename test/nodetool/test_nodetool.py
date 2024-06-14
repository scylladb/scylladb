#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request, expected_requests_manager
import subprocess
from test.nodetool.utils import check_nodetool_fails_with_all, check_nodetool_fails_with_error_contains


def test_jmx_compatibility_args(nodetool, scylla_only):
    """Check that all JMX arguments inherited to nodetool are ignored.

    These arguments are unused in the scylla-native nodetool and should be
    silently ignored.
    """
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-u", "us3r", "-pw", "secr3t",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--username", "us3r", "--password", "secr3t",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "-u", "us3r", "-pwf", "/tmp/secr3t_file",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--username", "us3r", "--password-file", "/tmp/secr3t_file",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "-pp",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--print-port",
             expected_requests=dummy_request)


def test_nodetool_no_args(nodetool_path, scylla_only):
    res = subprocess.run([nodetool_path, "nodetool"], capture_output=True, text=True)

    assert res.stdout == ""
    assert res.stderr == """\
Usage: scylla nodetool OPERATION [OPTIONS] ...
Try `scylla nodetool --help` for more information.
"""


def test_nodetool_api_request_failed(nodetool, scylla_only, rest_api_mock_server):
    ip, port = rest_api_mock_server

    error_messages = [
            f"error executing POST request to http://{ip}:{port}/storage_service/compact with parameters {{}}:"
            " remote replied with status code 500 Internal Server Error:",
            "ERROR MESSAGE"]

    check_nodetool_fails_with_all(
        nodetool,
        ("compact",),
        {"expected_requests": [expected_request("POST",
                                                "/storage_service/compact",
                                                response={"message": "ERROR MESSAGE", "code": 500},
                                                response_status=500)]},
        error_messages)


def test_nodetool_nonexistent_command(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("non-existent-command",),
            {},
            ["error: unrecognized operation argument: expected one of"])


def test_global_options_order(nodetool_path, rest_api_mock_server, scylla_only):
    with expected_requests_manager(rest_api_mock_server, [
            expected_request("POST", "/storage_service/compact", multiple=expected_request.MULTIPLE)]):

        ip, port = rest_api_mock_server
        port = str(port)

        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, "-p", port], check=True)
        subprocess.run([nodetool_path, "nodetool", "-h", ip, "compact", "-p", port], check=True)
        subprocess.run([nodetool_path, "nodetool", "-h", ip, "-p", port, "compact"], check=True)

        # Also add some compatibility args to the mix
        subprocess.run([nodetool_path, "nodetool", "-h", ip, "-p", port, "-u", "us3r", "compact"], check=True)
        subprocess.run([nodetool_path, "nodetool", "-h", ip, "-p", port, "compact", "-u", "us3r"], check=True)


def test_jvm_options(nodetool_path, rest_api_mock_server, scylla_only):
    with expected_requests_manager(rest_api_mock_server, [
            expected_request("POST", "/storage_service/compact", multiple=expected_request.MULTIPLE)]):

        ip, port = rest_api_mock_server
        port = str(port)

        jvm_opt = "-Dcom.sun.jndi.rmiURLParsing=legacy"

        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, "-p", port, jvm_opt], check=True)
        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, jvm_opt, "-p", port], check=True)
        subprocess.run([nodetool_path, "nodetool", jvm_opt, "compact", "-h", ip, "-p", port], check=True)


def test_alternative_api_port(nodetool_path, rest_api_mock_server, scylla_only):
    with expected_requests_manager(rest_api_mock_server, [
            expected_request("POST", "/storage_service/compact", multiple=expected_request.MULTIPLE)]):

        ip, port = rest_api_mock_server
        port = str(port)

        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, "-p", "1", "--rest-api-port", port], check=True)
        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, "-p", "1", f"--rest-api-port={port}"],
                       check=True)
        subprocess.run([nodetool_path, "nodetool", "compact", "-h", ip, "-p", "1", f"-Dcom.scylladb.apiPort={port}"],
                       check=True)
