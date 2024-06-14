#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.nodetool.utils import check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request, approximate_value
import pytest
import re
import time
from typing import NamedTuple


def test_clearnapshot(nodetool):
    nodetool("clearsnapshot", expected_requests=[
        expected_request("DELETE", "/storage_service/snapshots")
    ])


def test_clearnapshot_keyspace(nodetool):
    nodetool("clearsnapshot", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1"})
    ])


def test_clearnapshot_keyspaces(nodetool):
    nodetool("clearsnapshot", "ks1", "ks2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1,ks2"})
    ])


def test_clearnapshot_nonexistent_keyspaces(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("clearsnapshot", "non_existent_ks"),
            {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["error processing arguments: keyspace non_existent_ks does not exist"])


def test_clearnapshot_tag(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", expected_requests=[
        expected_request("DELETE", "/storage_service/snapshots", params={"tag": "snapshot_name"})
    ])


def test_clearnapshot_tag_and_keyspace(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1", "tag": "snapshot_name"})
    ])


def test_clearnapshot_tag_and_keyspaces(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", "ks1", "ks2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1,ks2", "tag": "snapshot_name"})
    ])


def test_listsnapshots(nodetool, request):
    res = nodetool("listsnapshots", expected_requests=[
        expected_request("GET", "/storage_service/snapshots", response=[
            {"key": "1698236289867", "value": [{"ks": "ks1", "cf": "tbl1", "total": 45056, "live": 0},
                                               {"ks": "ks1", "cf": "tbl2", "total": 40956, "live": 0}]},
            {"key": "1698236070745", "value": [{"ks": "ks1", "cf": "tbl1", "total": 35056, "live": 0},
                                               {"ks": "ks1", "cf": "tbl2", "total": 20956, "live": 0}]},
            ]),
        expected_request("GET", "/storage_service/snapshots/size/true", response=945235),
        ])

    expected_output =\
"""Snapshot Details: 
Snapshot name Keyspace name Column family name True size Size on disk
1698236289867 ks1           tbl1               0 bytes   44 KB       
1698236289867 ks1           tbl2               0 bytes   40 KB       
1698236070745 ks1           tbl1               0 bytes   34.23 KB    
1698236070745 ks1           tbl2               0 bytes   20.46 KB    

Total TrueDiskSpaceUsed: 923.08 KiB

"""
    assert res.stdout == expected_output


def test_listsnapshots_no_snapshots(nodetool, request):
    res = nodetool("listsnapshots", expected_requests=[
        expected_request("GET", "/storage_service/snapshots", response=[]),
        ])
    if request.config.getoption("nodetool") == "scylla":
        assert res.stdout == "There are no snapshots\n"
    else:
        assert res.stdout == "Snapshot Details: \nThere are no snapshots\n"


def check_snapshot_out(res, tag, ktlist, skip_flush):
    """Check that the output of nodetool snapshot contains the expected messages"""

    if len(ktlist) == 0:
        keyspaces = "all keyspaces"
    else:
        keyspaces = ", ?".join(ktlist)

    pattern = re.compile("Requested creating snapshot\\(s\\)"
                         f" for \\[{keyspaces}\\]"
                         f" with snapshot name \\[(.+)\\]"
                         f" and options \\{{skipFlush={str(skip_flush).lower()}\\}}")

    print(res)
    print(pattern)

    lines = res.split("\n")
    assert len(lines) == 3

    match = pattern.fullmatch(lines[0])
    assert match
    actual_tag = match.group(1)
    if type(tag) is int:
        # The tag is a timestamp in millis.
        # Allow for 30s worth of divergence between actual and expected tag value.
        assert abs(int(actual_tag) - tag) <= 30000
    else:
        # The tag was provided by the user, we expect an exact match
        assert actual_tag == tag

    assert lines[1] == f"Snapshot directory: {actual_tag}"
    assert lines[2] == ''


def test_snapshot_keyspace(nodetool):
    tag = "my_snapshot"

    res = nodetool("snapshot", "--tag", tag, "ks1", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1"], False)

    res = nodetool("snapshot", "--tag", tag, "ks1", "ks2", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1,ks2"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1", "ks2"], False)


@pytest.mark.parametrize("option_name", ("-cf", "--column-family", "--table"))
def test_snapshot_keyspace_with_table(nodetool, option_name):
    tag = "my_snapshot"

    res = nodetool("snapshot", "--tag", tag, "ks1", option_name, "tbl", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1", "cf": "tbl"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1"], False)

    res = nodetool("snapshot", "--tag", tag, "ks1", option_name, "tbl1,tbl2", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1", "cf": "tbl1,tbl2"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1"], False)


class kn_param(NamedTuple):
    args: tuple[str]
    kn: str
    cf: str
    snapshot_keyspaces: list[str]


@pytest.mark.parametrize("param", (
    kn_param(("ks1.tbl1",), "ks1", "tbl1", ["ks1.tbl1"]),
    kn_param(("ks1.tbl1", "ks1.tbl2"), "ks1.tbl1,ks1.tbl2", "", ["ks1.tbl1", "ks1.tbl2"]),
    kn_param(("ks1.tbl1", "ks2.tbl2"), "ks1.tbl1,ks2.tbl2", "", ["ks1.tbl1", "ks2.tbl2"]),
    kn_param(("ks1.1/tbl1",), "ks1.1", "tbl1", ["ks1.1/tbl1"]),
    kn_param(("ks1.1/tbl1", "ks1.2/tbl2"), "ks1.1/tbl1,ks1.2/tbl2", "", ["ks1.1/tbl1", "ks1.2/tbl2"]),
    kn_param(("--kt-list", "ks1.1/tbl1",), "ks1.1", "tbl1", ["ks1.1/tbl1"]),
    kn_param(("--kt-list", "ks1.1/tbl1,ks1.2/tbl2"), "ks1.1/tbl1,ks1.2/tbl2", "",
             ["ks1.1/tbl1", "ks1.2/tbl2"]),
))
def test_snapshot_keyspace_table_single_arg(nodetool, param, scylla_only):
    tag = "my_snapshot"

    req_params = {"tag": tag, "sf": "false", "kn": param.kn}
    if param.cf:
        req_params["cf"] = param.cf

    res = nodetool("snapshot", "--tag", tag, *param.args, expected_requests=[
        expected_request("POST", "/storage_service/snapshots", params=req_params)
    ])
    check_snapshot_out(res.stdout, tag, param.snapshot_keyspaces, False)


@pytest.mark.parametrize("option_name", ("-kt", "--kt-list", "-kc", "--kc.list"))
def test_snapshot_ktlist(nodetool, option_name):
    tag = "my_snapshot"

    res = nodetool("snapshot", "--tag", tag, option_name, "ks1.tbl1", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1", "cf": "tbl1"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1.tbl1"], False)

    res = nodetool("snapshot", "--tag", tag, option_name, "ks1.tbl1,ks2.tbl2", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1.tbl1,ks2.tbl2"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1.tbl1", "ks2.tbl2"], False)

    res = nodetool("snapshot", "--tag", tag, option_name, "ks1,ks2", expected_requests=[
        expected_request("POST", "/storage_service/snapshots",
                         params={"tag": tag, "sf": "false", "kn": "ks1,ks2"})
    ])
    check_snapshot_out(res.stdout, tag, ["ks1" ,"ks2"], False)


@pytest.mark.parametrize("tag", [None, "my_snapshot_tag"])
@pytest.mark.parametrize("ktlist", [
    {},
    {"ks": ["ks.tbl1"]},
    {"ks": ["ks1.tbl1", "ks2.tbl2"]},
    {"ks": ["ks1"], "tbl": ["tbl1"]},
    {"ks": ["ks1"], "tbl": ["tbl1", "tbl2"]},
    {"ks": ["ks1", "ks2"], "tbl": []},
])
@pytest.mark.parametrize("skip_flush", [False, True])
def test_snapshot_options_matrix(nodetool, tag, ktlist, skip_flush):
    cmd = ["snapshot"]
    params = {}

    if tag is None:
        tag = int(time.time() * 1000)
        params["tag"] = approximate_value(value=tag, delta=99999).to_json()
    else:
        cmd += ["--tag", tag]
        params["tag"] = tag

    if skip_flush:
        cmd.append("--skip-flush")

    params["sf"] = str(skip_flush).lower()

    if ktlist:
        if "tbl" in ktlist:
            if len(ktlist["tbl"]) > 0:
                keyspaces = ktlist["ks"]
                cmd += ["--table", ",".join(ktlist["tbl"])]
                cmd += keyspaces
                params["kn"] = keyspaces[0]
                params["cf"] = ",".join(ktlist["tbl"])
            else:
                keyspaces = ktlist["ks"]
                cmd += keyspaces
                params["kn"] = ",".join(keyspaces)
        else:
            keyspaces = ktlist["ks"]
            cmd += ["-kt", ",".join(keyspaces)]
            if len(keyspaces) == 1:
                ks, tbl = keyspaces[0].split(".")
                params["kn"] = ks
                params["cf"] = tbl
            elif len(keyspaces) > 1:
                params["kn"] = ",".join(keyspaces)
    else:
        keyspaces = []

    res = nodetool(*cmd, expected_requests=[
        expected_request("POST", "/storage_service/snapshots", params=params)
    ])

    check_snapshot_out(res.stdout, tag, keyspaces, skip_flush)


def test_snapshot_multiple_keyspace_with_table(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("snapshot", "--table", "tbl1", "ks1", "ks2"),
            {"expected_requests": []},
            ["error: When specifying the table for a snapshot, you must specify one and only one keyspace",
             "error processing arguments: when specifying the table for the snapshot,"
             " you must specify one and only one keyspace"])


def test_snapshot_table_with_ktlist(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("snapshot", "--table", "tbl1", "-kt", "ks1.tbl1"),
            {"expected_requests": []},
            ["error: When specifying the Keyspace columfamily list for a snapshot,"
             " you should not specify columnfamily",
             "error processing arguments: when specifying the keyspace-table list for a snapshot,"
             " you should not specify table(s)"])


def test_snapshot_keyspace_with_ktlist(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("snapshot", "-kt", "ks1.tbl1", "ks1"),
            {"expected_requests": []},
            ["error: When specifying the Keyspace columfamily list for a snapshot,"
             " you should not specify columnfamily",
             "error processing arguments: when specifying the keyspace-table list for a snapshot,"
             " you should not specify keyspace(s)"])


def test_snapshot_keyspace_with_tables(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("snapshot", "--table", "tbl1", "-cf", "tbl2", "ks1"),
            {"expected_requests": []},
            ["error: option '--table' cannot be specified more than once"])
