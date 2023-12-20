#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
import subprocess


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
