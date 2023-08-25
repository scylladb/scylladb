#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess

from rest_api_mock import expected_request


def test_all_keyspaces(nodetool):
    nodetool("compact", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
        expected_request("POST", "/storage_service/keyspace_compaction/system"),
        expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_keyspace(nodetool):
    nodetool("compact", "system_schema", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_nonexistent_keyspace(nodetool):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool("compact", "non_existent_ks", expected_requests=[
                expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system"]),
                expected_request("POST", "/storage_service/keyspace_compaction/non_existent_ks")])

    err_lines = e.value.stderr.rstrip().split('\n')
    out_lines = e.value.stdout.rstrip().split('\n')
    cassandra_error = "nodetool: Keyspace [non_existent_ks] does not exist."
    scylla_error = "error processing arguments: keyspace non_existent_ks does not exist"
    assert cassandra_error in err_lines or cassandra_error in out_lines or \
        scylla_error in err_lines or scylla_error in out_lines


def test_table(nodetool):
    nodetool("compact", "system_schema", "columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema", params={"cf": "columns"})])

    nodetool("compact", "system_schema", "columns", "computed_columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST",
                             "/storage_service/keyspace_compaction/system_schema",
                             params={"cf": "columns,computed_columns"})])


def test_split_output_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-s", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--split-output", expected_requests=dummy_request)


def test_token_range_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-st", "0", "-et", "1000", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--start-token", "0", "--end-token", "1000", expected_requests=dummy_request)


def test_partition_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "--partition", "0", expected_requests=dummy_request)


def test_user_defined(nodetool, scylla_only):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool("compact", "--user-defined", "/var/lib/scylla/data/system/local-7ad54392bcdd35a684174e047860b377/"
                 "me-3g8w_11cg_4317k2ppfb6d5vgp0w-big-Data.db")

    err_lines = e.value.stderr.rstrip().split('\n')
    "error processing arguments: --user-defined flag is unsupported" in err_lines
