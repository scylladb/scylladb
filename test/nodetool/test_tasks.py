#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with_error_contains

def test_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks"),
            {"expected_requests": []},
            ["unrecognized operation argument"])

def test_abort(nodetool, scylla_only):
    nodetool("tasks", "abort", "675ed9f4-6564-6dbd-can8-43fddce952gy", expected_requests=[
        expected_request("POST", "/task_manager/abort_task/675ed9f4-6564-6dbd-can8-43fddce952gy")])

    res = nodetool("tasks", "abort", "675ed9f4-6564-6dbd-can8-43fddce952gy", expected_requests=[
        expected_request("POST", "/task_manager/abort_task/675ed9f4-6564-6dbd-can8-43fddce952gy", response=[], response_status=403)])
    assert "Task with id 675ed9f4-6564-6dbd-can8-43fddce952gy is not abortable" in res.stdout

def test_abort_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "abort"),
            {"expected_requests": []},
            ["required parameter is missing"])

def test_list(nodetool, scylla_only):
    nodetool("tasks", "list", "repair", expected_requests=[
        expected_request("GET", "/task_manager/list_module_tasks/repair", response=[])])

    params = {
        "keyspace": "ks",
        "table": "t"
    }
    nodetool("tasks", "list", "compaction", "--keyspace", params["keyspace"], "--table", params["table"],
        expected_requests=[expected_request("GET", "/task_manager/list_module_tasks/compaction", params, response=[])])

    params = {
        "internal": "true"
    }
    nodetool("tasks", "list", "repair", "--internal", expected_requests=[
        expected_request("GET", "/task_manager/list_module_tasks/repair", params, response=[])])

    iterations = 2
    nodetool("tasks", "list", "repair", "--interval", "1", "--iterations", str(iterations), expected_requests=[
        expected_request("GET", "/task_manager/list_module_tasks/repair", response=[]) for _ in range(0, iterations + 1)])

def test_list_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "list"),
            {"expected_requests": []},
            ["required parameter is missing"])

    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "list", "repair", "--x"),
            {"expected_requests": []},
            ["unrecognised option"])

def test_modules(nodetool, scylla_only):
    nodetool("tasks", "modules", expected_requests=[
        expected_request("GET", "/task_manager/list_modules", response=[])])

def test_status(nodetool, scylla_only):
    nodetool("tasks", "status", "675ed9f4-6564-6dbd-can8-43fddce952gy", expected_requests=[
        expected_request("GET", "/task_manager/task_status/675ed9f4-6564-6dbd-can8-43fddce952gy")])

def test_status_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "status"),
            {"expected_requests": []},
            ["required parameter is missing"])

def test_tree(nodetool, scylla_only):
    nodetool("tasks", "tree", "675ed9f4-6564-6dbd-can8-43fddce952gy", expected_requests=[
        expected_request("GET", "/task_manager/task_status_recursive/675ed9f4-6564-6dbd-can8-43fddce952gy", response=[])])

    nodetool("tasks", "tree", expected_requests=[
        expected_request("GET", "/task_manager/list_modules", response=["repair"]),
        expected_request("GET", "/task_manager/list_module_tasks/repair", response=[{ "task_id": "675ed9f4-6564-6dbd-can8-43fddce952gy" }]),
        expected_request("GET", "/task_manager/task_status_recursive/675ed9f4-6564-6dbd-can8-43fddce952gy", response=[])])

def test_tree_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "tree", "foo", "bar"),
            {"expected_requests": []},
            ["cannot be specified more than once"])

def test_ttl(nodetool, scylla_only):
    nodetool("tasks", "ttl", expected_requests=[
        expected_request("GET", "/task_manager/ttl")])

    params = { "ttl": "10" }
    nodetool("tasks", "ttl", "--set", params["ttl"], expected_requests=[
        expected_request("POST", "/task_manager/ttl", params)])

def test_wait(nodetool, scylla_only):
    nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy")])

    res = nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", "--timeout", "10", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy", params={ "timeout": "10" }, response=[], response_status=408)], check_return_code=False)

def test_wait_quiet(nodetool, scylla_only):
    nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", "--quiet", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy", response={ "state": "done" })])

    res = nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", "--quiet", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy", response={ "state": "failed" })], check_return_code=False)
    assert res.returncode == 123

    res = nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", "--quiet", "--timeout", "10", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy", params={ "timeout": "10" }, response=[], response_status=408)], check_return_code=False)
    assert res.returncode == 124

    res = nodetool("tasks", "wait", "675ed9f4-6564-6dbd-can8-43fddce952gy", "--quiet", expected_requests=[
        expected_request("GET", "/task_manager/wait_task/675ed9f4-6564-6dbd-can8-43fddce952gy", response=[], response_status=400)], check_return_code=False)
    assert res.returncode == 125

def test_wait_failure(nodetool, scylla_only):
    check_nodetool_fails_with_error_contains(
            nodetool,
            ("tasks", "wait"),
            {"expected_requests": []},
            ["required parameter is missing"])
