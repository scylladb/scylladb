from enum import Enum
import requests
import sys
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import new_test_module, new_test_task, set_tmp_task_ttl, ThreadWrapper, scylla_inject_error
from test.rest_api.task_manager_utils import check_field_correctness, check_status_correctness, assert_task_does_not_exist, list_modules, get_task_status, list_tasks, get_task_status_recursively, wait_for_task, drain_module_tasks, abort_task

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
            assert resp.status_code == requests.codes.forbidden, "Aborted unabortable task"

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

class State(Enum):
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    NONE = "none"

# A class for testing task tree folding.
#
# The tasks are formed into a complete binary tree. The tree is kept in a list, such that i-th element
# of the list corresponds to i-th element in BFS order from root. All methods which get list of values
# for each tree node are expected to be in the same format.
#
# Example of indices for height = 4:
#
#                     0
#             1               2
#         3       4       5       6
#        7 8     9 10   11 12   13 14
#
class TaskBinaryTree():
    def __init__(self, rest_api, height: int):
        self.rest_api = rest_api
        self.tree = [self._new_task()]
        for i in range(1, pow(2, height) - 1):
            self.tree.append(self._new_task({ "parent_id": self.tree[(i - 1) // 2]}))

    def _new_task(self, args={}):
        resp = self.rest_api.send("POST", "task_manager_test/test_task", args)
        resp.raise_for_status()
        return resp.json()

    def get_nodes_number(self):
        return len(self.tree)

    def finish_all_tasks(self, failure_pattern: list[bool]):
        assert len(self.tree) == len(failure_pattern), "Incorrect pattern"
        for i in range(len(self.tree) - 1, -1, -1):
            if failure_pattern[i]:
                resp = self.rest_api.send("POST", f"task_manager_test/finish_test_task/{self.tree[i]}", { "error": "x" })
                resp.raise_for_status()
            else:
                resp = self.rest_api.send("POST", f"task_manager_test/finish_test_task/{self.tree[i]}")
                resp.raise_for_status()

    def get_status_tree(self):
        return get_task_status_recursively(self.rest_api, self.tree[0])

    def check_status_tree(self, status_tree, expected_states: list[State]):
        assert len(self.tree) == len(expected_states), "Incorrect tree size"
        assert len(status_tree) == len([s for s in expected_states if s != State.NONE]), "Incorrect tree nodes number"
        for i in range(len(self.tree)):
            if expected_states[i] != State.NONE:
                statuses = [s for s in status_tree if s["id"] == self.tree[i]]
                assert len(statuses) == 1
                status = statuses[0]
                assert expected_states[i].value == status["state"]

    def __del__(self):
        for task_id in self.tree:
            self.rest_api.send("DELETE", "task_manager_test/test_task", { "task_id": task_id })


def make_expected_states(failures_indexes, successes_indexes, nodes_num):
    expected_states = [State.NONE for _ in range(nodes_num)]
    for i in failures_indexes:
        assert i < nodes_num
        expected_states[i] = State.FAILED
    for i in successes_indexes:
        assert i < nodes_num
        assert expected_states[i] == State.NONE, "Index marked as both failed and succeed"
        expected_states[i] = State.DONE
    return expected_states

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     o                                       o
#             o               o          ->           o               o
#         o       o       o       o
#        o o     o o     o o     o o
#
def task_folding1(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    status_tree_running = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_running, [State.RUNNING for _ in range(task_tree.get_nodes_number())])

    success_pattern = [False for _ in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(success_pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[], successes_indexes=[0, 1, 2], nodes_num=task_tree.get_nodes_number()))

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     o                                       o
#             o               o          ->           o               o
#         o       o       o       o               o
#        x o     o o     o o     o o             x
#
def task_folding2(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    pattern = [i == 7 for i in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[7], successes_indexes=[0, 1, 2, 3], nodes_num=task_tree.get_nodes_number()))

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     o                                       o
#             o               o          ->           o               o
#         x       o       o       o               x
#        o o     o o     o o     o o
#
def task_folding3(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    pattern = [i == 3 for i in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[3], successes_indexes=[0, 1, 2], nodes_num=task_tree.get_nodes_number()))

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     o                                       o
#             o               o          ->           o               o
#         x       o       o       o               x
#        x o     o o     o o     o o             x
#
def task_folding4(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    pattern = [i == 3 or i == 7 for i in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[3, 7], successes_indexes=[0, 1, 2], nodes_num=task_tree.get_nodes_number()))

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     x                                       x
#             x               o          ->           x               o
#         x       o       o       o               x
#        x o     o o     o o     o o             x
#
def task_folding5(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    pattern = [i in [0, 1, 3, 7] for i in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[0, 1, 3, 7], successes_indexes=[2], nodes_num=task_tree.get_nodes_number()))

# The actual tree and tree after folding. o means that a task finished successfully, x - that it failed.
#
#                     o                                       o
#             x               o          ->           x               o
#         o       o       x       o                               x
#        o o     o o     o x     o o                               x
#
def task_folding6(rest_api):
    tree_height = 4
    task_tree = TaskBinaryTree(rest_api, tree_height)

    pattern = [i in [1, 5, 12] for i in range(task_tree.get_nodes_number())]
    task_tree.finish_all_tasks(pattern)

    status_tree_done = task_tree.get_status_tree()
    task_tree.check_status_tree(status_tree_done, make_expected_states(failures_indexes=[1, 5, 12], successes_indexes=[0, 2], nodes_num=task_tree.get_nodes_number()))

# Checks whether finished children fold into parents as expected.
def test_task_folding(rest_api):
    with new_test_module(rest_api):
        with set_tmp_task_ttl(rest_api, long_time):
            task_folding1(rest_api)
            task_folding2(rest_api)
            task_folding3(rest_api)
            task_folding4(rest_api)
            task_folding5(rest_api)
            task_folding6(rest_api)

def test_abort_on_unregistered_task(cql, this_dc, rest_api):
    module_name = "compaction"
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                compaction_injection = "compaction_major_keyspace_compaction_task_impl_run"
                abort_injection = "tasks_abort_children"
                with scylla_inject_error(rest_api, compaction_injection, True): # Stops running compaction.
                    with scylla_inject_error(rest_api, abort_injection, True):  # Stops task abort.
                        # Start compaction.
                        resp = rest_api.send("POST", f"tasks/compaction/keyspace_compaction/{keyspace}")
                        resp.raise_for_status()
                        task_id = resp.json()

                        # Abort compaction.
                        abort_task(rest_api, task_id)

                        # Resume compaction.
                        resp = rest_api.send("POST", f"v2/error_injection/injection/{compaction_injection}/message")
                        resp.raise_for_status()

                        # Wait until compaction is done and unregister the task.
                        wait_for_task(rest_api, task_id)
                        get_task_status(rest_api, task_id)

                        # Resume abort.
                        resp = rest_api.send("POST", f"v2/error_injection/injection/{abort_injection}/message")
                        resp.raise_for_status()
    drain_module_tasks(rest_api, module_name)
