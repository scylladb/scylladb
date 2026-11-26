#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with

@pytest.mark.parametrize("nowait,task_state,task_error", [(False, "failed", "error"),
                                                          (False, "done", ""),
                                                          (True, "", "")])
def test_restore(nodetool, nowait, task_state, task_error):
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


# The --sstables-file-list file is split on newlines and every piece is sent
# to the server verbatim, where an empty name fails the whole restore with
# malformed_sstable_exception. Text files usually end with a newline, so make
# sure it doesn't turn into such an empty entry in the request body.
@pytest.mark.parametrize("trailing_newline", [False, True])
def test_restore_sstables_file_list(nodetool, tmp_path, trailing_newline):
    endpoint = "s3.us-east-2.amazonaws.com"
    bucket = "bucket-foo"
    keyspace = "ks"
    table = "cf"
    prefix = "foo/bar"

    sstables = [f"me-{id}-big-TOC.txt" for id in range(3)]
    list_file = tmp_path / "sstables.list"
    content = "\n".join(sstables)
    if trailing_newline:
        content += "\n"
    list_file.write_text(content)

    params = {"endpoint": endpoint,
              "bucket": bucket,
              "prefix": prefix,
              "keyspace": keyspace,
              "table": table}

    task_id = "2c4a3e5f"
    res = nodetool("restore",
                   "--endpoint", endpoint,
                   "--bucket", bucket,
                   "--prefix", prefix,
                   "--keyspace", keyspace,
                   "--table", table,
                   "--nowait",
                   "--sstables-file-list", str(list_file),
                   expected_requests=[
                       expected_request(
                           "POST",
                           "/storage_service/restore",
                           params,
                           sstables,
                           response=task_id)
                   ])
    assert task_id in res.stdout


@pytest.mark.parametrize("scope_val", ["all", "dc", "rack"])
@pytest.mark.parametrize("pro_val", ["--primary-replica-only", "-pro"])
def test_restore_scope_primary_replica(nodetool, scope_val, pro_val):
    nodetool("restore", "--endpoint", "s3.us-east-2.amazonaws.com", "--bucket", "test_bucket", "--prefix",
             "test_prefix", "--keyspace", "ks", "--table", "tbl", f"--scope={scope_val}", pro_val,
             "me-1-big-TOC.txt",
             expected_requests=[
                 expected_request("POST", "/storage_service/restore",
                                  params={"endpoint": "s3.us-east-2.amazonaws.com", "bucket": "test_bucket",
                                          "table": "tbl", "prefix": "test_prefix", "keyspace": "ks",
                                          "scope": f"{scope_val}", "primary_replica_only": "true"}, body=["me-1-big-TOC.txt"])],
             check_return_code=False)


@pytest.mark.parametrize("nowait,task_state,task_error", [(False, "failed", "error"),
                                                          (False, "done", ""),
                                                          (True, "", "")])
def test_cluster_restore(nodetool, nowait, task_state, task_error):
    task_id = "2c4a3e5f"
    start_time = "2024-08-08T14:29:25Z"
    end_time = "2024-08-08T14:30:42Z"
    task_status = {
        "id": task_id,
        "type": "restore_tablets",
        "kind": "cluster",
        "scope": "table",
        "state": task_state,
        "is_abortable": True,
        "start_time": start_time,
        "end_time": end_time,
        "error": task_error,
        "sequence_number": 0,
        "shard": 0,
        "progress_total": 1.0,
        "progress_completed": 1.0,
        "children_ids": []
    }
    expected_requests = [
        expected_request(
            "POST",
            "/storage_service/tablets/restore",
            {"keyspace": "ks", "table": "cf", "snapshot": "ss"},
            [{"datacenter": "dc1", "endpoint": "s3.us-east-2.amazonaws.com", "bucket": "bucket-foo", "prefix": "foo/bar"},
             {"datacenter": "dc2", "endpoint": "s3.us-west-2.amazonaws.com", "bucket": "bucket-bar", "prefix": ""}],
            response=task_id)
    ]
    args = ["cluster", "restore", "ks",
            "--table", "cf",
            "--snapshot", "ss",
            "--location", "dc1,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar",
            "--location", "dc2,s3.us-west-2.amazonaws.com,bucket-bar"]
    if nowait:
        args.append("--nowait")
        res = nodetool(*args, expected_requests=expected_requests)
        assert task_id in res.stdout
    else:
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


def test_cluster_restore_source_names(nodetool):
    task_id = "2c4a3e5f"
    res = nodetool("cluster", "restore", "ks",
                   "--table", "cf",
                   "--snapshot", "ss",
                   "--location", "dc1,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar",
                   "--source-keyspace", "old_ks",
                   "--source-table", "old_cf",
                   "--nowait",
                   expected_requests=[
                       expected_request(
                           "POST",
                           "/storage_service/tablets/restore",
                           {"keyspace": "ks", "table": "cf", "snapshot": "ss", "source_keyspace": "old_ks", "source_table": "old_cf"},
                           [{"datacenter": "dc1", "endpoint": "s3.us-east-2.amazonaws.com", "bucket": "bucket-foo", "prefix": "foo/bar"}],
                           response=task_id)
                   ])
    assert task_id in res.stdout


@pytest.mark.parametrize("repair", ["full", "disabled"])
def test_cluster_restore_repair(nodetool, repair):
    task_id = "2c4a3e5f"
    res = nodetool("cluster", "restore", "ks",
                   "--table", "cf",
                   "--snapshot", "ss",
                   "--location", "dc1,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar",
                   "--repair", repair,
                   "--nowait",
                   expected_requests=[
                       expected_request(
                           "POST",
                           "/storage_service/tablets/restore",
                           {"keyspace": "ks", "table": "cf", "snapshot": "ss", "repair": repair},
                           [{"datacenter": "dc1", "endpoint": "s3.us-east-2.amazonaws.com", "bucket": "bucket-foo", "prefix": "foo/bar"}],
                           response=task_id)
                   ])
    assert task_id in res.stdout


@pytest.mark.parametrize("args,error", [
    (["--table", "cf", "--snapshot", "ss", "--location", "dc1,ep,bucket"], "missing required parameter: keyspace"),
    (["ks", "--snapshot", "ss", "--location", "dc1,ep,bucket"], "missing required parameter: table"),
    (["ks", "--table", "cf", "--location", "dc1,ep,bucket"], "missing required parameter: snapshot"),
    (["ks", "--table", "cf", "--snapshot", "ss"], "missing required parameter: location"),
    (["ks", "--table", "cf", "--snapshot", "ss", "--location", "dc1,ep"], "Invalid 'location' argument: dc1,ep"),
    (["ks", "--table", "cf", "--snapshot", "ss", "--location", "dc1,ep,bucket,prefix,extra"], "Invalid 'location' argument: dc1,ep,bucket,prefix,extra"),
])
def test_cluster_restore_invalid_arguments(nodetool, args, error):
    check_nodetool_fails_with(nodetool, ("cluster", "restore", *args), {"expected_requests": []}, [f"error processing arguments: {error}"])
