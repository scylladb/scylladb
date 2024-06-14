#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest

from test.nodetool.utils import check_nodetool_fails_with_error_contains, check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request


JMX_COLUMN_FAMILIES_REQUEST = expected_request(
        "GET",
        "/column_family/",
        multiple=expected_request.ANY,
        response=[{"ks": "system_schema",
                   "cf": "columns",
                   "type": "ColumnFamilies"},
                  {"ks": "system_schema",
                   "cf": "computed_columns",
                   "type": "ColumnFamilies"}])

JMX_STREAM_MANAGER_REQUEST = expected_request(
        "GET",
        "/stream_manager/",
        multiple=expected_request.ANY,
        response=[])


def _remove_log_timestamp(res):
    """ Log timestamp[1] is impossible to match accurately, so just remove it

    E.g. for:

        [2023-12-22 09:18:06,615] Repair session 1

    We drop the [2023-12-22 09:18:06,615] part

    Also strip trailing the whitespace, that nodetool loves to add randomly.
    """
    rebuilt_res = []
    for line in res.split('\n'):
        if not line:
            rebuilt_res.append('')
            continue
        print(line)
        rebuilt_res.append(line.split('] ')[1].strip())
    return "\n".join(rebuilt_res)


def test_repair_all_single_keyspace(nodetool):
    res = nodetool("repair", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy"}, response=["ks1"]),
        expected_request("GET", "/storage_service/keyspaces", response=["ks1"], multiple=expected_request.ANY),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks1",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks1", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks1 (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_all_two_keyspaces(nodetool):
    res = nodetool("repair", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy"},
                         response=["ks1", "ks2"]),
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY, response=["ks1", "ks2"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks1",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=3),
        expected_request("GET", "/storage_service/repair_async/ks1", params={"id": "3"}, response="RUNNING"),
        expected_request("GET", "/storage_service/repair_async/ks1", params={"id": "3"}, response="SUCCESSFUL"),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks2",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=4),
        expected_request("GET", "/storage_service/repair_async/ks2", params={"id": "4"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #3, repairing 1 ranges for keyspace ks1 (parallelism=SEQUENTIAL, full=true)
Repair session 3
Repair session 3 finished
Starting repair command #4, repairing 1 ranges for keyspace ks2 (parallelism=SEQUENTIAL, full=true)
Repair session 4
Repair session 4 finished
"""


def test_repair_keyspace(nodetool):
    res = nodetool("repair", "ks", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_one_table(nodetool):
    res = nodetool("repair", "ks", "table1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "columnFamilies": "table1",
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_two_tables(nodetool):
    res = nodetool("repair", "ks", "table1", "table2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "columnFamilies": "table1,table2",
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_long_progress(nodetool):
    res = nodetool("repair", "ks", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="RUNNING"),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="RUNNING"),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_failed(nodetool):
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("repair", "ks"),
        {"expected_requests": [
            expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
            JMX_COLUMN_FAMILIES_REQUEST,
            JMX_STREAM_MANAGER_REQUEST,
            expected_request(
                "POST",
                "/storage_service/repair_async/ks",
                params={
                    "trace": "false",
                    "ignoreUnreplicatedKeyspaces": "false",
                    "parallelism": "parallel",
                    "incremental": "false",
                    "pullRepair": "false",
                    "primaryRange": "false",
                    "jobThreads": "1"},
                response=1),
            expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="FAILED")]
         },
        ["error: Repair job has failed with the error message: ",
         "Repair session 1 failed"])


def test_repair_all_three_keyspaces_failed(nodetool):
    """Check that given three keyspaces to repair, if the second one fails, the
    third one isn't even started."""
    expected_requests = [
        expected_request("GET", "/storage_service/keyspaces", params={"type": "non_local_strategy"},
                         response=["ks1", "ks2", "ks3"]),
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2", "ks3"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks1",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=10),
        expected_request("GET", "/storage_service/repair_async/ks1", params={"id": "10"}, response="RUNNING"),
        expected_request("GET", "/storage_service/repair_async/ks1", params={"id": "10"}, response="SUCCESSFUL"),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks2",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=11),
        expected_request("GET", "/storage_service/repair_async/ks2", params={"id": "11"}, response="FAILED")]

    # Check that repair of ks3 is not even started, after repair of ks2 failed
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("repair",),
        {"expected_requests": expected_requests},
        ["error: Repair job has failed with the error message: ",
         "Repair session 11 failed"])


def _do_test_repair_options(
        nodetool,
        table=[],
        trace=None,
        datacenter=None,
        token=None,
        ignore_unreplicated_ks=None,
        parallelism=None,
        pull=None,
        primary_range=None,
        hosts=None):

    args = ["repair", "ks"] + table

    # We add params in the same order that java nodetool does, to make param comparison easier to read
    expected_params = {}

    expected_requests = [
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
    ]

    if trace is None:
        expected_params["trace"] = "false"
    else:
        args.append(trace)
        expected_params["trace"] = "true"

    if token is not None:
        st, et = token
        if st != '':
            args += ['-st', st]
        if et != '':
            args += ['-et', et]
        expected_params["ranges"] = f"{st}:{et}"

    if ignore_unreplicated_ks is None:
        expected_params["ignoreUnreplicatedKeyspaces"] = "false"
    else:
        args.append(ignore_unreplicated_ks)
        expected_params["ignoreUnreplicatedKeyspaces"] = "true"

    if hosts is not None:
        host_params = []
        for arg, host in hosts:
            host_params.append(host)
            args += [arg, host]
        expected_params["hosts"] = ",".join(host_params)

    if parallelism is None:
        expected_params["parallelism"] = "parallel"
    elif parallelism == "-seq" or parallelism == "--sequential":
        args.append(parallelism)
        expected_params["parallelism"] = "sequential"
    elif parallelism == "-dcpar" or parallelism == "--dc-parallel":
        args.append(parallelism)
        expected_params["parallelism"] = "dc_parallel"

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

        expected_params["dataCenters"] = ",".join(dcs)

    expected_params["incremental"] = "false"

    if pull is None:
        expected_params["pullRepair"] = "false"
    else:
        args.append(pull)
        expected_params["pullRepair"] = "true"

    if primary_range is None:
        expected_params["primaryRange"] = "false"
    else:
        args.append(primary_range)
        expected_params["primaryRange"] = "true"

    expected_params["jobThreads"] = "1"

    expected_requests += [
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params=expected_params,
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")
    ]

    res = nodetool(*args, expected_requests=expected_requests)

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


@pytest.mark.parametrize("trace", [None, "-tr", "--trace"])
def test_repair_options_trace(nodetool, trace):
    _do_test_repair_options(nodetool, trace=trace)


@pytest.mark.parametrize("token", [None, ('10', ''), ('', '20'), ('10', '20')])
def test_repair_options_token(nodetool, token):
    _do_test_repair_options(nodetool, token=token)


@pytest.mark.parametrize("ignore_unreplicated_ks", [None, "-iuk", "--ignore-unreplicated-keyspaces"])
def test_repair_options_ignore_unreplicated_ks(nodetool, ignore_unreplicated_ks):
    _do_test_repair_options(nodetool, ignore_unreplicated_ks=ignore_unreplicated_ks)


@pytest.mark.parametrize("parallelism", [None, "-seq", "--sequential", "-dcpar", "--dc-parallel"])
def test_repair_options_parallelism(nodetool, parallelism):
    _do_test_repair_options(nodetool, parallelism=parallelism)


@pytest.mark.parametrize("datacenter", [None,
                                        [("-dc", "DC1")],
                                        [("--in-dc", "DC1")],
                                        [("-dc", "DC1"), ("--in-dc", "DC2")],
                                        [("-local",)],
                                        [("--in-local-dc",)]])
def test_repair_options_datacenter(nodetool, datacenter):
    _do_test_repair_options(nodetool, datacenter=datacenter)


@pytest.mark.parametrize("pull", [None, "-pl", "--pull"])
def test_repair_options_pull(nodetool, pull):
    _do_test_repair_options(nodetool, pull=pull)


@pytest.mark.parametrize("primary_range", [None, "-pr", "--partitioner-range"])
def test_repair_options_primary_range(nodetool, primary_range):
    _do_test_repair_options(nodetool, primary_range=primary_range)


@pytest.mark.parametrize("hosts", [None,
                                   [("-hosts", "HOST1")],
                                   [("--in-hosts", "HOST1"), ("--in-hosts", "HOST2")]])
def test_repair_options_hosts(nodetool, hosts):
    _do_test_repair_options(nodetool, hosts=hosts)


def test_repair_parallelism_precedence(nodetool):
    res = nodetool("repair", "ks", "--dc-parallel", "--sequential", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "sequential",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_dc_precedence(nodetool):
    res = nodetool("repair", "ks", "--in-dc", "DC1", "-dc", "DC2", "--in-local-dc", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request("GET", "/snitch/datacenter", response="DC_local", multiple=expected_request.ANY),
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "dataCenters": "DC_local",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": "1"},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


@pytest.mark.parametrize("jobs", [None, ("-j", "2"), ("--job-threads", "2")])
@pytest.mark.parametrize("full", [None, "-full", "--full"])
def test_repair_unused_options(request, nodetool, jobs, full):
    args = ["repair", "ks"]

    if jobs:
        args += list(jobs)

    if full:
        args.append(full)

    if jobs:
        job_threads = jobs[1]
    else:
        job_threads = "1"

    res = nodetool(*args, expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        JMX_COLUMN_FAMILIES_REQUEST,
        JMX_STREAM_MANAGER_REQUEST,
        expected_request(
            "POST",
            "/storage_service/repair_async/ks",
            params={
                "trace": "false",
                "ignoreUnreplicatedKeyspaces": "false",
                "parallelism": "parallel",
                "incremental": "false",
                "pullRepair": "false",
                "primaryRange": "false",
                "jobThreads": job_threads},
            response=1),
        expected_request("GET", "/storage_service/repair_async/ks", params={"id": "1"}, response="SUCCESSFUL")])

    assert _remove_log_timestamp(res.stdout) == """\
Starting repair command #1, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
Repair session 1
Repair session 1 finished
"""


def test_repair_pr_and_dcs(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("repair", "ks", "-pr", "-dc", "DC1"),
        {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        ]},
        ["error: Primary range repair should be performed on all nodes in the cluster.",
         "error processing arguments: primary range repair should be performed on all nodes in the cluster"])


def test_repair_pr_and_hosts(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("repair", "ks", "-pr", "-hosts", "127.0.0.2"),
        {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        ]},
        ["error: Primary range repair should be performed on all nodes in the cluster.",
         "error processing arguments: primary range repair should be performed on all nodes in the cluster"])
