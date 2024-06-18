import requests
import sys
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import new_test_module, new_test_task, set_tmp_task_ttl, ThreadWrapper, scylla_inject_error
from task_manager_utils import check_field_correctness, check_status_correctness, assert_task_does_not_exist, list_modules, get_task_status, list_tasks, get_task_status_recursively, wait_for_task, drain_module_tasks, abort_task

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

def wait_and_check_status(rest_api, id, sequence_number, keyspace, table):
    status = wait_for_task(rest_api, id)
    check_status_correctness(status, { "id": id, "state": "done", "sequence_number": sequence_number, "keyspace": keyspace, "table": table })

def test_task_manager_wait(rest_api):
    with new_test_module(rest_api):
        keyspace = "keyspace0"
        table = "table0"
        args0 = { "keyspace": keyspace, "table": table }
        with new_test_task(rest_api, args0) as task0:
            print(f"created test task {task0}")

            x = ThreadWrapper(target=wait_and_check_status, args=(rest_api, task0, 1, keyspace, table,))
            x.start()

            time.sleep(2)   # Thread x should wait until finish_test_task.

            assert x.is_alive, "task_manager/wait_task does not wait for task to be complete"

            resp = rest_api.send("POST", f"task_manager_test/finish_test_task/{task0}")
            resp.raise_for_status()

            x.join()

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

def test_abort_on_unregistered_task(cql, this_dc, rest_api):
    module_name = "repair"
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                repair_injection = "repair_shard_repair_task_impl_do_repair_ranges"
                abort_injection = "tasks_abort_children"
                with scylla_inject_error(rest_api, repair_injection, True): # Stops running compaction.
                    with scylla_inject_error(rest_api, abort_injection, True):  # Stops task abort.
                        # Start repair.
                        resp = rest_api.send("POST", f"storage_service/repair_async/{keyspace}")
                        resp.raise_for_status()
                        sequence_number = resp.json()

                        # Get repair's id.
                        resp = rest_api.send("GET", f"task_manager/list_module_tasks/{module_name}")
                        resp.raise_for_status()
                        ids = [s["task_id"] for s in resp.json() if s["sequence_number"] == sequence_number]
                        assert len(ids) == 1, "Wrong task number"
                        task_id = ids[0]

                        # Abort repair.
                        abort_task(rest_api, task_id)

                        # Resume repair.
                        resp = rest_api.send("POST", f"v2/error_injection/injection/{repair_injection}/message")
                        resp.raise_for_status()

                        # Wait until repair is done and unregister the task.
                        wait_for_task(rest_api, task_id)

                        # Resume abort.
                        resp = rest_api.send("POST", f"v2/error_injection/injection/{abort_injection}/message")
                        resp.raise_for_status()
    drain_module_tasks(rest_api, module_name)
