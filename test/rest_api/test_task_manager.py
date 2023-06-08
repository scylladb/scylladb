import requests
import time
import asyncio

from rest_util import new_test_module, new_test_task, set_tmp_task_ttl
from task_manager_utils import check_field_correctness, check_status_correctness, assert_task_does_not_exist, list_modules, get_task_status, list_tasks, get_task_status_recursively

long_time = 1000000000

def check_sequence_number(rest_api, task_id, expected):
    status = get_task_status(rest_api, task_id)
    check_field_correctness("sequence_number", status, { "sequence_number": expected })

def test_task_manager_modules(rest_api):
    with new_test_module(rest_api):
        modules = list_modules(rest_api)
        assert "test" in modules, "test module was not listed"

def test_task_manager_tasks(rest_api):
    with new_test_module(rest_api):
        args0 = { "shard": 0, "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")
            args1 = { "shard": 1, "keyspace": "keyspace0", "table": "table1"}
            with new_test_task(rest_api, args1) as task1:
                print(f"created test task {task1}")
                tasks = [task0, task1]
                for task in list_tasks(rest_api, "test"):
                    task_id = task["task_id"]
                    assert task_id in tasks, f"Unrecognized task_id={task_id}"
                    tasks.remove(task_id)
                assert not tasks, f"list_module_tasks did not return all tasks. remaining={tasks}"

def test_task_manager_status_running(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            status = get_task_status(rest_api, task0)
            check_status_correctness(status, { "id": task0, "state": "running", "sequence_number": 1, "keyspace": "keyspace0", "table": "table0" })

            tasks = list_tasks(rest_api, "test")
            assert tasks, "task_status unregistered task that did not finish"

def test_task_manager_status_done(rest_api):
    with new_test_module(rest_api):
        args0 = { "keyspace": "keyspace0", "table": "table0"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            with set_tmp_task_ttl(rest_api, long_time):
                resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
                resp.raise_for_status()

                status = get_task_status(rest_api, task0)
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

                status = get_task_status(rest_api, task0)
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

            waiting_task = asyncio.create_task(wait_for_task_async(rest_api, task0))
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
        args1 = {"keyspace": "keyspace0", "table": "table0", "shard": "1"}
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")
            with new_test_task(rest_api, args1) as task1:
                print(f"created test task {task1}")
                ttl = 2
                with set_tmp_task_ttl(rest_api, ttl):
                    resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
                    resp.raise_for_status()
                    resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task1}")
                    resp.raise_for_status()

                    time.sleep(ttl + 1)
                    assert_task_does_not_exist(rest_api, task0)
                    assert_task_does_not_exist(rest_api, task1)

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

def test_task_manager_recursive_status(rest_api):
    with new_test_module(rest_api):
        args0 = {"keyspace": "keyspace0"}
        with new_test_task(rest_api, args0) as task0:                           # parent
            print(f"created test task {task0}")

            args1 = {"keyspace": "keyspace0", "parent_id": f"{task0}"}
            with new_test_task(rest_api, args1) as task1:                       # child1
                print(f"created test task {task1}")

                args2 = {"keyspace": "keyspace0", "parent_id": f"{task1}"}
                with new_test_task(rest_api, args2) as task2:                   # child1 of child1
                    print(f"created test task {task2}")

                    with new_test_task(rest_api, args1) as task3:               # child2
                        print(f"created test task {task3}")

                    tasks = get_task_status_recursively(rest_api, task0)
                    check_field_correctness("id", tasks[0], { "id" : f"{task0}" })
                    check_field_correctness("id", tasks[1], { "id" : f"{task1}" })
                    check_field_correctness("id", tasks[2], { "id" : f"{task3}" })
                    check_field_correctness("id", tasks[3], { "id" : f"{task2}" })

def test_module_not_exists(rest_api):
    module_name = "module_that_does_not_exist"
    resp = rest_api.send("GET", f"task_manager/list_module_tasks/{module_name}", )
    assert resp.status_code == requests.codes.bad_request, f"Invalid response status code: {resp.status_code}"
