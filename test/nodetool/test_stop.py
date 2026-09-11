#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

# Test Scylla-only compaction types
def test_stop_common_scylla(nodetool):
    for compaction_type in ("SPLIT", "REWRITE_COMPONENT"):
        check_compaction_type(nodetool, compaction_type)


# Even though our docs says it is supported, cassandra-nodetool doesn't know about RESHARD
# Although RESHARD is a valid compaction type, RESHARD compaction cannot be stopped
def test_stop_reshard(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("stop", "RESHARD"),
            {},
            [f"error processing arguments: Stopping compaction of type RESHARD is disallowed"])


# Cassandra calls UPGRADE, UPGRADE_SSTABLES, which the scylla-code doesn't recognize
def test_stop_upgrade(nodetool, scylla_only):
    check_compaction_type(nodetool, "UPGRADE")


# Scylla-specific compaction types, see scylladb/scylladb#SCYLLADB-3761.
# COMPACTION stops both regular and major compactions, REGULAR and MAJOR stop just one of them.
def test_stop_regular_and_major(nodetool, scylla_only):
    for compaction_type in ("REGULAR", "MAJOR"):
        check_compaction_type(nodetool, compaction_type)


# Recognized by scylla, but not supported
def test_stop_unsupported(nodetool):
    for compaction_type in ("VALIDATION", "INDEX_BUILD"):
        check_nodetool_fails_with(
                nodetool,
                ("stop", compaction_type),
                {},
                [f"error processing arguments: Compaction type {compaction_type} is unsupported"])


def test_stop_unknown(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("stop", "FOO"),
            {},
            ["error processing arguments: Invalid compaction type FOO, valid compaction types are: (COMPACTION, CLEANUP, SCRUB, UPGRADE, RESHAPE, SPLIT, MAJOR, REWRITE_COMPONENT and REGULAR)"])


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
