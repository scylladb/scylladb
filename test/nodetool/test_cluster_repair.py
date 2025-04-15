#
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest

from test.nodetool.utils import check_nodetool_fails_with_error_contains, check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request


def _remove_log_timestamp(res):
    """ Log timestamp[1] is impossible to match accurately, so just remove it

    E.g. for:

        [2023-12-22 09:18:06,615] Repair session 1

    We drop the [2023-12-22 09:18:06,615] part
    """
    rebuilt_res = []
    for line in res.split('\n'):
        if not line:
            rebuilt_res.append('')
            continue
        print(line)
        rebuilt_res.append(line.split('] ')[1])
    return "\n".join(rebuilt_res)

def test_repair_all_single_keyspace_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    id2 = "ef1b7a61-66c8-494c-bb03-6f65724e6eef"
    res = nodetool("cluster", "repair", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "vnodes"}, response=[]),
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "tablets"}, response=["ks1"]),
        expected_request("GET", "/column_family", response=[{"ks": "ks1", "cf": "table1"}, {"ks": "ks1", "cf": "table2"}]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks1",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"}),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks1",
                "table": "table2",
                "tokens": "all"},
            response={"tablet_task_id": id2}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id2}",
            response={"state": "done"})])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks1 table=table1
Repair with task_id={id1} finished
Starting repair with task_id={id2} keyspace=ks1 table=table2
Repair with task_id={id2} finished
"""

def test_repair_all_two_keyspaces_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    id2 = "ef1b7a61-66c8-494c-bb03-6f65724e6eef"
    res = nodetool("cluster", "repair", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "vnodes"}, response=[]),
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "tablets"}, response=["ks1", "ks2"]),
        expected_request("GET", "/column_family", response=[{"ks": "ks1", "cf": "table1"}, {"ks": "ks2", "cf": "table2"}]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks1",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"}),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks2",
                "table": "table2",
                "tokens": "all"},
            response={"tablet_task_id": id2}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id2}",
            response={"state": "done"})])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks1 table=table1
Repair with task_id={id1} finished
Starting repair with task_id={id2} keyspace=ks2 table=table2
Repair with task_id={id2} finished
"""

def test_repair_keyspace_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    id2 = "ef1b7a61-66c8-494c-bb03-6f65724e6eef"
    res = nodetool("cluster", "repair", "ks", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        expected_request("GET", "/column_family", response=[{"ks": "ks", "cf": "table1"}, {"ks": "ks2", "cf": "table3"}, {"ks": "ks", "cf": "table2"}]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
         expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"}),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks",
                "table": "table2",
                "tokens": "all"},
            response={"tablet_task_id": id2}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id2}",
            response={"state": "done"}),
        ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks table=table1
Repair with task_id={id1} finished
Starting repair with task_id={id2} keyspace=ks table=table2
Repair with task_id={id2} finished
"""

def test_repair_one_table_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    res = nodetool("cluster", "repair", "ks", "table1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
         expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"}),
        ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks table=table1
Repair with task_id={id1} finished
"""

def test_repair_two_tables_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    id2 = "ef1b7a61-66c8-494c-bb03-6f65724e6eef"
    res = nodetool("cluster", "repair", "ks", "table1", "table2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
         expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"}),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks",
                "table": "table2",
                "tokens": "all"},
            response={"tablet_task_id": id2}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id2}",
            response={"state": "done"}),
        ])

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks table=table1
Repair with task_id={id1} finished
Starting repair with task_id={id2} keyspace=ks table=table2
Repair with task_id={id2} finished
"""

def test_repair_failed_tablets(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "repair", "ks"),
        {"expected_requests": [
            expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
            expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
            expected_request("GET", "/column_family", response=[{"ks": "ks", "cf": "table1"}]),
            expected_request(
                "POST",
                "/storage_service/tablets/repair",
                params={
                    "ks": "ks",
                    "table": "table1",
                    "tokens": "all"},
                response={"tablet_task_id": id1}),
            expected_request(
                "GET",
                f"/task_manager/wait_task/{id1}",
                response={"state": "failed"})]
        },
        [f"Repair with task_id={id1} failed"])

def _do_test_repair_options_tablets(
        nodetool,
        datacenter=None,
        hosts=None,
        tokens=None
        ):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"

    args = ["cluster", "repair", "ks"]
    expected_params = {
        "ks": "ks",
        "table": "table",
        "tokens": "all"
    }

    expected_requests = [
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=["ks"]),
        expected_request("GET", "/column_family", response=[{"ks": "ks", "cf": "table"}])
    ]

    if hosts is not None:
        host_params = []
        for arg, host in hosts:
            host_params.append(host)
            args += [arg, host]
        expected_params["hosts_filter"] = ",".join(host_params)

    if datacenter is not None:
        dcs = []
        for dc in datacenter:
            if len(dc) == 2:
                dcs.append(dc[1])

            args += list(dc)

            if dc[0] == "-local" or dc[0] == "--in-local-dc":
                # Looks like JMX caches the response to this, so we have to make it optional
                expected_requests += [
                    expected_request("GET", "/snitch/datacenter", response="DC_local", multiple=expected_request.ANY),
                ]
                dcs.append("DC_local")

        expected_params["dcs_filter"] = ",".join(dcs)

    if tokens is not None:
        tokens_params = []
        for arg, t in tokens:
            tokens_params.append(t)
            args += [arg, t]
        expected_params["tokens"] = ",".join(tokens_params)

    expected_requests += [
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            expected_params,
            response={"tablet_task_id": id1}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"})
    ]

    res = nodetool(*args, expected_requests=expected_requests)

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks table=table
Repair with task_id={id1} finished
"""

@pytest.mark.parametrize("datacenter", [None,
                                        [("-dc", "DC1")],
                                        [("--in-dc", "DC1")],
                                        [("-dc", "DC1"), ("--in-dc", "DC2")],
                                        [("-dc", "DC1,DC2")]])
def test_repair_options_datacenter_tablets(nodetool, datacenter):
    _do_test_repair_options_tablets(nodetool, datacenter=datacenter)

@pytest.mark.parametrize("hosts", [None,
                                   [("-hosts", "HOST1")],
                                   [("-hosts", "HOST1,HOST2")],
                                   [("--in-hosts", "HOST1"), ("--in-hosts", "HOST2")]])
def test_repair_options_hosts_tablets(nodetool, hosts):
    _do_test_repair_options_tablets(nodetool, hosts=hosts)

@pytest.mark.parametrize("datacenter", [None,
                                        [("-dc", "DC1")]])
@pytest.mark.parametrize("hosts", [None,
                                   [("-hosts", "HOST1")]])
def test_repair_options_hosts_and_dcs_tablets(nodetool, datacenter, hosts):
    _do_test_repair_options_tablets(nodetool, datacenter=datacenter, hosts=hosts)

@pytest.mark.parametrize("tokens", [None,
                                   [("--tablet-tokens", "1")],
                                   [("--tablet-tokens", "-1,2")],
                                   [("--tablet-tokens", "-1"), ("--tablet-tokens", "2")]])
def test_repair_options_hosts_tablets(nodetool, tokens):
    _do_test_repair_options_tablets(nodetool, tokens=tokens)

def test_repair_all_with_vnode_keyspace(nodetool):
    id1 = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
    res = nodetool("cluster", "repair", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "tablets"}, response=["ks1"]),
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy", "replication": "vnodes"}, response=["ks2"]),
        expected_request("GET", "/column_family", response=[{"ks": "ks1", "cf": "table1"}, {"ks": "ks2", "cf": "table2"}]),
        expected_request(
            "POST",
            "/storage_service/tablets/repair",
            params={
                "ks": "ks1",
                "table": "table1",
                "tokens": "all"},
            response={"tablet_task_id": id1}),
        expected_request(
            "GET",
            f"/task_manager/wait_task/{id1}",
            response={"state": "done"})])

    assert "Warning: only tablet keyspaces will be repaired." in res.stdout

    assert _remove_log_timestamp(res.stdout) == f"""\
Starting repair with task_id={id1} keyspace=ks1 table=table1
Repair with task_id={id1} finished
"""

def test_repair_keyspace(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("cluster", "repair", "ks"),
        {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
                expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"}, response=[]),
        ]},
        ["error processing arguments: nodetool cluster repair repairs only tablet keyspaces. To repair vnode keyspaces use nodetool repair."])
