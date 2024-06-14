#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.nodetool.utils import check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request


def check_compaction_type(nodetool, compaction_type):
    nodetool(f"stop", compaction_type, expected_requests=[
        expected_request("POST", "/compaction_manager/stop_compaction", params={"type": compaction_type})])


# Test compaction-types supported by both C* and Scylla
def test_stop_common(nodetool):
    for compaction_type in ("COMPACTION", "CLEANUP", "SCRUB", "RESHAPE"):
        check_compaction_type(nodetool, compaction_type)


# Even though our docs says it is supported, cassandra-nodetool doesn't know about RESHARD
def test_stop_reshard(nodetool, scylla_only):
    check_compaction_type(nodetool, "RESHARD")


# Cassandra calls UPGRADE, UPGRADE_SSTABLES, which the scylla-code doesn't recognize
def test_stop_upgrade(nodetool, scylla_only):
    check_compaction_type(nodetool, "UPGRADE")


# Recognized by scylla, but not supported
def test_stop_unsupported(nodetool):
    for compaction_type in ("VALIDATION", "INDEX_BUILD"):
        req = expected_request(
                "POST",
                "/compaction_manager/stop_compaction",
                params={"type": compaction_type},
                multiple=expected_request.ANY,
                response={"code": 500,
                          "message": f"std::runtime_error (Compaction type {compaction_type} is unsupported)"},
                response_status=500)
        check_nodetool_fails_with(
                nodetool,
                ("stop", compaction_type),
                {"expected_requests": [req]},
                ["nodetool: Scylla API server HTTP POST to URL 'compaction_manager/stop_compaction' failed:"
                 f" std::runtime_error (Compaction type {compaction_type} is unsupported)",
                 f"error processing arguments: invalid compaction type: {compaction_type}"])


def test_stop_unknown(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("stop", "FOO"),
            {},
            ["nodetool: compaction_type: can not convert \"FOO\" to a OperationType",
             "error processing arguments: invalid compaction type: FOO"])


def test_stop_no_type(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("stop",),
            {},
            ["error processing arguments: missing required parameter: compaction_type"])


# This is not implemented, nodetool logs a message and exits
def test_stop_by_id(nodetool, scylla_only):
    expected_error = "error processing arguments: stopping compactions by id is not implemented"

    check_nodetool_fails_with(nodetool, ("stop", "-id", "123"), {}, [expected_error])
    check_nodetool_fails_with(nodetool, ("stop", "-id=123"), {}, [expected_error])
    check_nodetool_fails_with(nodetool, ("stop", "--id", "123"), {}, [expected_error])
    check_nodetool_fails_with(nodetool, ("stop", "--id=123"), {}, [expected_error])
