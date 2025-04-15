#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest

from test.nodetool.rest_api_mock import expected_request

@pytest.mark.parametrize("nowait,task_state,task_error", [(False, "failed", "error"),
                                                          (False, "done", ""),
                                                          (True, "", "")])
def test_restore(nodetool, scylla_only, nowait, task_state, task_error):
    endpoint = "s3.us-east-2.amazonaws.com"
    bucket = "bucket-foo"
    keyspace = "ks"
    table = "cf"
    prefix = "foo/bar"

    params = {"endpoint": endpoint,
              "bucket": bucket,
              "prefix": prefix,
              "keyspace": keyspace,
              "table": table}

    task_id = "2c4a3e5f"
    start_time = "2024-08-08T14:29:25Z"
    end_time = "2024-08-08T14:30:42Z"
    task_status = {
        "id": task_id,
        "type": "download_sstables",
        "kind": "node",
        "scope": "node",
        "state": task_state,
        "is_abortable": False,
        "start_time": start_time,
        "end_time": end_time,
        "error": task_error,
        "sequence_number": 0,
        "shard": 0,
        "progress_total": 1.0,
        "progress_completed": 1.0,
        "children_ids": []
    }
    # just generate filenames of TOC components of random sstables
    sstables = [f"me-{id}-big-TOC.txt" for id in range(12)]
    expected_requests = [
        expected_request(
            "POST",
            "/storage_service/restore",
            params,
            sstables,
            response=task_id)
    ]
    args = ["restore",
            "--endpoint", endpoint,
            "--bucket", bucket,
            "--prefix", prefix,
            "--keyspace", keyspace,
            "--table", table]
    if nowait:
        args.append("--nowait")
        args.extend(sstables)
        res = nodetool(*args, expected_requests=expected_requests)
        assert task_id in res.stdout
    else:
        args.extend(sstables)
        # wait for the completion of backup task
        expected_requests.append(
            expected_request(
                "GET",
                f"/task_manager/wait_task/{task_id}",
                response=task_status))
        res = nodetool(*args, expected_requests=expected_requests, check_return_code=False)
        if task_state == "done":
            expected_returncode = 0
            expected_output = f"""{task_state}
start: {start_time}
end: {end_time}
"""
        else:
            expected_returncode = 1
            expected_output = f"""{task_state}: {task_error}
start: {start_time}
end: {end_time}
"""
        assert res.returncode == expected_returncode
        assert res.stdout == expected_output
