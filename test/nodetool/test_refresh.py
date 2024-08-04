#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest

from test.nodetool.utils import check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request


def test_refresh(nodetool):
    nodetool("refresh", "ks", "tbl", expected_requests=[
        expected_request("POST", "/storage_service/sstables/ks", params={"cf": "tbl"})])


@pytest.mark.parametrize("load_and_stream_opt", ["--load-and-stream", "-las"])
def test_refresh_load_and_stream(nodetool, load_and_stream_opt):
    nodetool("refresh", "ks", "tbl", load_and_stream_opt, expected_requests=[
        expected_request("POST", "/storage_service/sstables/ks", params={"cf": "tbl", "load_and_stream": "true"})])


@pytest.mark.parametrize("load_and_stream_opt", ["--load-and-stream", "-las"])
@pytest.mark.parametrize("primary_replica_only_opt", ["--primary-replica-only", "-pro"])
def test_refresh_load_and_stream_and_primary_replica_only(nodetool, load_and_stream_opt, primary_replica_only_opt):
    nodetool("refresh", "ks", "tbl", load_and_stream_opt, primary_replica_only_opt, expected_requests=[
        expected_request("POST", "/storage_service/sstables/ks",
                         params={"cf": "tbl", "load_and_stream": "true", "primary_replica_only": "true"})])


def test_refresh_no_table(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("refresh", "ks"),
            {"expected_requests": []},
            ["nodetool: refresh requires ks and cf args",
             "error processing arguments: required parameter is missing: table"])


def test_refresh_no_table_no_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("refresh",),
            {"expected_requests": []},
            ["nodetool: refresh requires ks and cf args",
             "error processing arguments: required parameters are missing: keyspace and table"])


def test_refresh_primary_replica_only(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("refresh", "ks", "tbl", "--primary-replica-only"),
            {"expected_requests": []},
            ["error processing arguments: --primary-replica-only|-pro takes no effect without --load-and-stream|-las"])
