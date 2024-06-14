#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with
import pytest


# `scylla nodetool compact` invokes the newly added global compact api
def test_all_keyspaces(nodetool, scylla_only):
    nodetool("compact", expected_requests=[
        expected_request("POST", "/storage_service/compact")])


# The java-based `nodetool compact` lists all keyspaces and invoke the keyspace_compaction api on each of them
def test_all_keyspaces_jmx(nodetool, cassandra_only):
    nodetool("compact", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["system", "system_schema"]),
        expected_request("POST", "/storage_service/keyspace_compaction/system"),
        expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_keyspace(nodetool):
    nodetool("compact", "system_schema", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_nonexistent_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("compact", "non_existent_ks"),
            {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                                 response=["system"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])


def test_table(nodetool):
    nodetool("compact", "system_schema", "columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema", params={"cf": "columns"})])

    nodetool("compact", "system_schema", "columns", "computed_columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST",
                             "/storage_service/keyspace_compaction/system_schema",
                             params={"cf": "columns,computed_columns"})])


def test_split_output_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-s", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--split-output", expected_requests=dummy_request)


def test_token_range_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-st", "0", "-et", "1000", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--start-token", "0", "--end-token", "1000", expected_requests=dummy_request)


def test_user_defined(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("compact", "--user-defined", "/var/lib/scylla/data/system/local-7ad54392bcdd35a684174e047860b377/"
             "me-3g8w_11cg_4317k2ppfb6d5vgp0w-big-Data.db"),
            {},
            ["error processing arguments: --user-defined flag is unsupported"])


@pytest.mark.parametrize("flush", ("true", "false"))
# The `--flush-memtables` option to `nodetool compact` is available only with `scylla_nodetool`
def test_keyspace_flush_memtables_option(nodetool, scylla_only, flush):
    params = {"flush_memtables": flush}
    nodetool("compact", "system_schema", "--flush-memtables", flush, expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                             response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema", params=params)])


@pytest.mark.parametrize("flush", ("true", "false"))
# The `--flush-memtables` option to `nodetool compact` is available only with `scylla_nodetool`
def test_all_keyspaces_flush_memtables_option(nodetool, scylla_only, flush):
    params = {"flush_memtables": flush}
    nodetool("compact", "--flush-memtables", flush, expected_requests=[
            expected_request("POST", "/storage_service/compact", params=params)])
