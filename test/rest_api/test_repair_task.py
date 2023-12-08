import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import set_tmp_task_ttl, scylla_inject_error
from test.rest_api.task_manager_utils import list_tasks, drain_module_tasks, get_task_status, get_task_status_recursively, wait_for_task, check_child_parent_relationship, get_children

empty_id = "00000000-0000-0000-0000-000000000000"
module_name = "repair"
long_time = 1000000000

def run_repair_and_wait(rest_api, keyspace):
    resp = rest_api.send("POST", f"storage_service/repair_async/{keyspace}")
    resp.raise_for_status()
    sequence_number = resp.json()
    resp = rest_api.send("GET", f"storage_service/repair_status", { "id": sequence_number })
    resp.raise_for_status()
    return sequence_number

def test_repair_task_progress_finished_task(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        # Insert some data.
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])
                with new_test_table(cql, keyspace, schema) as t1:
                    stmt = cql.prepare(f"INSERT INTO {t1} (p, v) VALUES (?, ?)")
                    cql.execute(stmt, [2, 'hello'])
                    cql.execute(stmt, [3, 'world'])

                    sequence_number = run_repair_and_wait(rest_api, keyspace)

                    # Get all repairs.
                    ids = [task["task_id"] for task in list_tasks(rest_api, "repair") if task["sequence_number"] == sequence_number]
                    assert len(ids) == 1, "Wrong number of internal repair tasks"
                    status_tree = get_task_status_recursively(rest_api, ids[0])
                    status = status_tree[0]
                    assert status["progress_completed"] == status["progress_total"], "Incorrect task progress"

                    assert "children_ids" in status, "Shard tasks weren't created"
                    children = [s for s in status_tree if s["parent_id"] == status["id"]]
                    assert all([child["progress_completed"] == child["progress_total"] for child in children]), "Some shard tasks have incorrect progress"

                    assert sum([child["progress_total"] for child in children]) == status["progress_total"], "Total progress of parent is not equal to children total progress sum"
                    assert sum([child["progress_completed"] for child in children]) == status["progress_completed"], "Completed progress of parent is not equal to children completed progress sum"
    drain_module_tasks(rest_api, module_name)

def test_repair_task_tree(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])
                with new_test_table(cql, keyspace, schema) as t1:
                    stmt = cql.prepare(f"INSERT INTO {t1} (p, v) VALUES (?, ?)")
                    cql.execute(stmt, [2, 'hello'])
                    cql.execute(stmt, [3, 'world'])

                    run_repair_and_wait(rest_api, keyspace)

                    ids = [task["task_id"] for task in list_tasks(rest_api, module_name)]
                    assert ids, "repair task was not created"
                    assert len(ids) == 1, "incorrect number of non-internal tasks"

                    status_tree = get_task_status_recursively(rest_api, ids[0])
                    status = status_tree[0]
                    assert status["state"] == "done", f"tasks with id {status['id']} failed"

                    repair_tree_depth = 1
                    check_child_parent_relationship(rest_api, status_tree, status, False)
    drain_module_tasks(rest_api, module_name)

def test_repair_task_progress(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        # Insert some data.
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])
                with new_test_table(cql, keyspace, schema) as t1:
                    stmt = cql.prepare(f"INSERT INTO {t1} (p, v) VALUES (?, ?)")
                    cql.execute(stmt, [2, 'hello'])
                    cql.execute(stmt, [3, 'world'])

                    injection = "repair_shard_repair_task_impl_do_repair_ranges"
                    with scylla_inject_error(rest_api, injection, True):
                        resp = rest_api.send("POST", f"storage_service/repair_async/{keyspace}")
                        resp.raise_for_status()
                        sequence_number = resp.json()

                        # Get all repairs.
                        statuses = [get_task_status(rest_api, task["task_id"]) for task in list_tasks(rest_api, "repair") if task["sequence_number"] == sequence_number]
                        assert len(statuses) == 1, "Wrong number of internal repair tasks"
                        status = statuses[0]
                        assert "children_ids" in status, "No child tasks created"

                        for child_ident in status["children_ids"]:
                            # Check if task state is correct.
                            child_status = get_task_status(rest_api, child_ident["task_id"])
                            assert child_status["state"] == "running", "Incorrect task progress"
                            assert child_status["progress_completed"] * 2 <= child_status["progress_total"], "Incorrect task progress"

                        resp = rest_api.send("POST", f"v2/error_injection/injection/{injection}/message")
                        resp.raise_for_status()

                        child_status = wait_for_task(rest_api, status["id"])
                        status_tree = get_task_status_recursively(rest_api, status["id"])
                        for child_status in get_children(status_tree, status["id"]):
                            assert child_status["progress_completed"] == child_status["progress_total"], "Incorrect task progress"
    drain_module_tasks(rest_api, module_name)
