#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest

from test.nodetool.rest_api_mock import expected_request

@pytest.mark.parametrize("table",
                         ["cf",
                          pytest.param("",
                                       marks=pytest.mark.xfail(
                                           reason="full keyspace restore not implemented yet"))])
@pytest.mark.parametrize("nowait", [False, True])
def test_restore(nodetool, scylla_only, table, nowait):
    endpoint = "s3.us-east-2.amazonaws.com"
    bucket = "bucket-foo"
    keyspace = "ks"

    snapshot = "ss"
    params = {"endpoint": endpoint,
              "bucket": bucket,
              "snapshot": snapshot,
              "keyspace": keyspace}
    if table:
        params["table"] = table

    task_id = "2c4a3e5f"
    start_time = "2024-08-08T14:29:25Z"
    end_time = "2024-08-08T14:30:42Z"
    state = "done"
    task_status = {
        "id": task_id,
        "type": "download_sstables",
        "kind": "node",
        "scope": "node",
        "state": state,
        "is_abortable": False,
        "start_time": start_time,
        "end_time": end_time,
        "error": "",
        "sequence_number": 0,
        "shard": 0,
        "progress_total": 1.0,
        "progress_completed": 1.0,
        "children_ids": []
    }
    expected_requests = [
        expected_request(
            "POST",
            "/storage_service/restore",
            params,
            response=task_id)
    ]
    args = ["restore",
            "--endpoint", endpoint,
            "--bucket", bucket,
            "--snapshot", snapshot,
            "--keyspace", keyspace]
    if table:
        args.extend(["--table", table])
    if nowait:
        args.append("--nowait")
        expected_output = ""
    else:
        # wait for the completion of backup task
        expected_requests.append(
            expected_request(
                "GET",
                f"/task_manager/wait_task/{task_id}",
                response=task_status))
        expected_output = f"""{state}
start: {start_time}
end: {end_time}
"""
    res = nodetool(*args, expected_requests=expected_requests)
    assert res.stdout == expected_output
