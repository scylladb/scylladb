import requests
import time
import asyncio

from rest_util import new_test_module, new_test_task, set_tmp_task_ttl, long_time

def check_field_correctness(field_name, status, expected_status):
    if field_name in expected_status:
        assert status[field_name] == expected_status[field_name], f"Incorrect task {field_name}"

def check_status_correctness(status, expected_status):
    check_field_correctness("id", status, expected_status)
    check_field_correctness("state", status, expected_status)
    check_field_correctness("sequence_number", status, expected_status)
    check_field_correctness("keyspace", status, expected_status)
    check_field_correctness("table", status, expected_status)
    if "error" in expected_status:
        assert expected_status["error"] in status["error"], "Incorrect error message"
    else:
        assert status["error"] == "", "Error message was set"

async def wait_for_task(rest_api, task_id):
    rest_api.send("GET", f"task_manager/wait_task/{task_id}")

def assert_task_does_not_exist(rest_api, task_id):
    resp = rest_api.send("GET", f"task_manager/task_status/{task_id}")
    assert resp.status_code == requests.codes.internal_server_error, f"Task {task_id} is kept in memory"

def check_sequence_number(rest_api, task_id, expected):
    resp = rest_api.send("GET", f"task_manager/task_status/{task_id}")
    resp.raise_for_status()
    status = resp.json()
    check_field_correctness("sequence_number", status, { "sequence_number": expected })

def test_task_manager_modules(rest_api):
    with new_test_module(rest_api):
        resp = rest_api.send("GET", "task_manager/list_modules")
        resp.raise_for_status()
        modules = resp.json()
        assert "test" in modules, "test module was not listed"

def test_task_manager_tasks(rest_api):
    with new_test_module(rest_api):
        args0 = { "shard": 0, "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")
            args1 = { "shard": 1, "keyspace": "keyspace0", "table": "table1"}
            with new_test_task(rest_api, args1) as task1:
                print(f"created test task {task1}")
                resp = rest_api.send("GET", "task_manager/list_module_tasks/test")
                resp.raise_for_status()
                tasks = [task0, task1]
                for task in resp.json():
                    task_id = task["task_id"]
                    assert task_id in tasks, f"Unrecognized task_id={task_id}"
                    tasks.remove(task_id)
                assert not tasks, f"list_module_tasks did not return all tasks. remaining={tasks}"

def test_task_manager_status_running(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")
            resp = rest_api.send("GET", f"task_manager/task_status/{task0}")
            resp.raise_for_status()

            status = resp.json()
            check_status_correctness(status, { "id": task0, "state": "running", "sequence_number": 1, "keyspace": "keyspace0", "table": "table0" })

            resp = rest_api.send("GET", "task_manager/list_module_tasks/test")
            tasks = resp.json()
            assert tasks, "task_status unregistered task that did not finish"

def test_task_manager_status_done(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            with set_tmp_task_ttl(rest_api, long_time):
                resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
                resp.raise_for_status()
                resp = rest_api.send("GET", f"task_manager/task_status/{task0}")
                resp.raise_for_status()

            status = resp.json()
            check_status_correctness(status, { "id": task0, "state": "done", "sequence_number": 1, "keyspace": "keyspace0", "table": "table0" })
            assert_task_does_not_exist(rest_api, task0)

def test_task_manager_status_failed(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            with set_tmp_task_ttl(rest_api, long_time):
                resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}", { "error": "Test task failed" })
                resp.raise_for_status()
                resp = rest_api.send("GET", f"task_manager/task_status/{task0}")
                resp.raise_for_status()

            status = resp.json()
            check_status_correctness(status, { "id": task0, "state": "failed", "error": "Test task failed", "sequence_number": 1, "keyspace": "keyspace0", "table": "table0" })
            assert_task_does_not_exist(rest_api, task0)

def test_task_manager_not_abortable(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            resp = rest_api.send("POST", f"task_manager/abort_task/{task0}")
            assert resp.status_code == requests.codes.internal_server_error, "Aborted unabortable task"

async def test_task_manager_wait(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            waiting_task = asyncio.create_task(wait_for_task(rest_api, task0))
            done, pending = await asyncio.wait({waiting_task})

            assert waiting_task in pending, "wait_task finished while the task was still running"

            resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
            resp.raise_for_status()
            status = resp.json()
            check_status_correctness(status, { "id": task0, "state": "done", "sequence_number": 1, "keyspace": "keyspace0", "table": "table0" })

            assert waiting_task in done, "wait_task did not returned even though the task finished"
            assert_task_does_not_exist(rest_api, task0)

def test_task_manager_ttl(rest_api):
    with new_test_module(rest_api):
        args0 = {"keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")
            ttl = 2
            with set_tmp_task_ttl(rest_api, ttl):
                resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
                resp.raise_for_status()

                time.sleep(ttl + 1)
                assert_task_does_not_exist(rest_api, task0)

def test_task_manager_sequence_number(rest_api):
    with new_test_module(rest_api):
        args0 = { "shard": 0 }                              # sequence_number == 1
        args1 = { "shard": 0 }                              # sequence_number == 2
        with new_test_task(rest_api, args0) as task0:
            with new_test_task(rest_api, args1) as task1:
                args2 = { "shard": 0, "parent_id": task0 }  # sequence_number == 1
                args3 = { "shard": 1, "parent_id": task1 }  # sequence_number == 2
                args4 = { "shard": 1 }                      # sequence_number == 1
                with new_test_task(rest_api, args2) as task2:
                    with new_test_task(rest_api, args3) as task3:
                        with new_test_task(rest_api, args4) as task4:
                            check_sequence_number(rest_api, task0, 1)
                            check_sequence_number(rest_api, task1, 2)
                            check_sequence_number(rest_api, task2, 1)
                            check_sequence_number(rest_api, task3, 2)
                            check_sequence_number(rest_api, task4, 1)
