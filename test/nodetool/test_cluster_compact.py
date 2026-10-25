#
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.nodetool.utils import check_nodetool_fails_with, check_nodetool_fails_with_error_contains
from test.nodetool.rest_api_mock import expected_request


ID1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
ID2 = "ef1b7a61-66c8-494c-bb03-6f65724e6eef"
TABLE_ID = "5e1bd3a0-4a9e-11f0-9f7a-0123456789ab"
TABLE_ID2 = "5e1bd3a0-4a9e-11f0-9f7a-0123456789ac"


def _remove_log_timestamp(res):
    """Drops the [2023-12-22 09:18:06,615] timestamp of each log line, which can't be matched."""
    return "\n".join(line.split('] ', 1)[1] if line else '' for line in res.split('\n'))


def _compact_requests(task_id, params, state="done", compaction_type="major"):
    return [
        expected_request("POST", "/storage_service/tablets/compaction", params={"type": compaction_type, **params}, response=task_id),
        expected_request("GET", f"/task_manager/wait_task/{task_id}", response={"state": state, "error": ""}),
    ]


def test_cluster_compact_all_keyspaces(nodetool):
    """A single request, and so a single task, for all tablet keyspaces: the server resolves them."""
    res = nodetool("cluster", "compact", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "vnodes"}, response=[]),
        *_compact_requests(ID1, {}),
    ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting major compaction with task_id={ID1} all tablet keyspaces
major compaction with task_id={ID1} finished
"""


def test_cluster_compact_keyspace(nodetool):
    """A keyspace is passed on as is: the server resolves it to its tables, skipping co-located ones."""
    res = nodetool("cluster", "compact", "ks", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        *_compact_requests(ID1, {"ks": "ks"}),
    ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting major compaction with task_id={ID1} keyspace=ks
major compaction with task_id={ID1} finished
"""


def test_cluster_compact_tables_recursive(nodetool):
    """Several tables go in one request, as a comma-separated list."""
    res = nodetool("cluster", "compact", "--type", "major", "--recursive", "ks", "table1", "table2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        *_compact_requests(ID1, {"ks": "ks", "table": "table1,table2", "recursive": "true"}),
    ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting major compaction with task_id={ID1} keyspace=ks table=table1,table2
major compaction with task_id={ID1} finished
"""


def test_cluster_compact_force_flush(nodetool):
    nodetool("cluster", "compact", "--force-flush", "ks", "table1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        *_compact_requests(ID1, {"ks": "ks", "table": "table1", "force_flush": "true"}),
    ])


def test_cluster_compact_table_id(nodetool):
    res = nodetool("cluster", "compact", "--table-id", TABLE_ID, "--table-id", TABLE_ID2, expected_requests=[
        *_compact_requests(ID1, {"table_id": f"{TABLE_ID},{TABLE_ID2}"}),
    ])
    assert _remove_log_timestamp(res.stdout) == f"""\
Starting major compaction with task_id={ID1} table_id={TABLE_ID},{TABLE_ID2}
major compaction with task_id={ID1} finished
"""

    # The keyspace is passed on, for the API to check that the table belongs to it.
    nodetool("cluster", "compact", "--table-id", TABLE_ID, "ks", expected_requests=[
        *_compact_requests(ID1, {"ks": "ks", "table_id": TABLE_ID}),
    ])


def test_cluster_compact_table_id_with_table_name(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("cluster", "compact", "--table-id", TABLE_ID, "ks", "table1"),
        {},
        ["error processing arguments: nodetool cluster compact: --table-id can't be combined with table names"])


def test_cluster_compact_table_without_keyspace(nodetool):
    """The positional options can be given by name too; a table without its keyspace must not
    silently compact every table."""
    check_nodetool_fails_with(
        nodetool,
        ("cluster", "compact", "--table", "table1"),
        {},
        ["error processing arguments: nodetool cluster compact: a table name requires the keyspace"])


def test_cluster_compact_unsupported_type(nodetool):
    """The type is passed through to the server, which validates it, so supporting a new type
    doesn't need a change in nodetool."""
    error = "Unsupported tablet compaction type 'scrub': only 'major' is supported"
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "compact", "--type", "scrub", "ks", "table1"),
        {"expected_requests": [
            expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
            expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
            expected_request("POST", "/storage_service/tablets/compaction", params={"type": "scrub", "ks": "ks", "table": "table1"},
                             response={"message": error, "code": 400}, response_status=400),
        ]},
        [error])


def test_cluster_compact_vnode_keyspace(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("cluster", "compact", "ks"),
        {"expected_requests": [
            expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
            expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=[]),
        ]},
        ["error processing arguments: nodetool cluster compact compacts only tablet keyspaces. To compact vnode keyspaces use nodetool compact."])


def test_cluster_compact_failed(nodetool):
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "compact", "ks", "table1"),
        {"expected_requests": [
            expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
            expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
            expected_request("POST", "/storage_service/tablets/compaction", params={"type": "major", "ks": "ks", "table": "table1"}, response=ID1),
            expected_request("GET", f"/task_manager/wait_task/{ID1}",
                             response={"state": "failed", "error": "major compaction failed for 1 tablet(s)"}),
        ]},
        [f"major compaction with task_id={ID1} failed: major compaction failed for 1 tablet(s)"])
