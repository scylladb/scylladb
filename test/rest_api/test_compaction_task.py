import sys
import threading

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import set_tmp_task_ttl, scylla_inject_error
from task_manager_utils import wait_for_task, list_tasks, check_child_parent_relationship, drain_module_tasks, abort_task

module_name = "compaction"
long_time = 1000000000

# depth parameter means the number of edges in the longest path from root to leaves in task tree.
def check_compaction_task(cql, this_dc, rest_api, run_compaction, compaction_type, depth, allow_no_children=False):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                # Run major compaction.
                [_, table] = t0.split(".")
                resp = run_compaction(keyspace, table)
                resp.raise_for_status()

                # Get list of compaction tasks.
                tasks = [task for task in list_tasks(rest_api, module_name) if task["type"] == compaction_type]
                assert tasks, f"{compaction_type} task was not created"

                # Check if all tasks finished successfully.
                statuses = [wait_for_task(rest_api, task["task_id"]) for task in tasks]
                failed = [task["task_id"] for task in statuses if task["state"] != "done"]
                assert not failed, f"tasks with ids {failed} failed"

                for top_level_task in statuses:
                    check_child_parent_relationship(rest_api, top_level_task, depth, allow_no_children)
    drain_module_tasks(rest_api, module_name)

def checkout_async_task(cql, this_dc, rest_api, run_compaction_async, compaction_type):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                resp = run_compaction_async(keyspace)
                resp.raise_for_status()
                task_id = resp.json()

                # Use task_id in task manager api.
                status = wait_for_task(rest_api, task_id)
                assert status["type"] == compaction_type, "Task has an invalid type"
                assert status["state"] == "done", "Compaction failed"
    drain_module_tasks(rest_api, module_name)

def test_global_major_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 4
    # global major compaction
    check_compaction_task(cql, this_dc, rest_api, lambda _, __: rest_api.send("POST", f"storage_service/compact"), "major compaction", task_tree_depth, allow_no_children=True)

    # global major compaction, flush option
    check_compaction_task(cql, this_dc, rest_api, lambda _, __: rest_api.send("POST", f"storage_service/compact?flush_memtables=true"), "major compaction", task_tree_depth, allow_no_children=True)
    check_compaction_task(cql, this_dc, rest_api, lambda _, __: rest_api.send("POST", f"storage_service/compact?flush_memtables=false"), "major compaction", task_tree_depth, allow_no_children=True)

def test_major_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 3
    # keyspace major compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}"), "major compaction", task_tree_depth)
    # column family major compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}"), "major compaction", task_tree_depth)

    # keyspace major compaction, flush option
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}?flush_memtables=true"), "major compaction", task_tree_depth)
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}?flush_memtables=false"), "major compaction", task_tree_depth)
    # column family major compaction, flush option
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}?flush_memtables=true"), "major compaction", task_tree_depth)
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}?flush_memtables=false"), "major compaction", task_tree_depth)

def test_cleanup_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 3
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}"), "cleanup compaction", task_tree_depth, True)

def test_offstrategy_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 3
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}"), "offstrategy compaction", task_tree_depth, True)

def test_rewrite_sstables_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 3
    # upgrade sstables compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}"), "upgrade sstables compaction", task_tree_depth)
    # scrub sstables compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}"), "scrub sstables compaction", task_tree_depth)

def test_reshaping_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 2
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "reshaping compaction", task_tree_depth, True)

def test_resharding_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 2
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "resharding compaction", task_tree_depth, True)

def test_regular_compaction_task(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                [_, table] = t0.split(".")
                resp = rest_api.send("POST", f"column_family/autocompaction/{keyspace}:{table}")
                resp.raise_for_status()

                statuses = [wait_for_task(rest_api, task["task_id"]) for task in list_tasks(rest_api, "compaction") if task["type"] == "regular compaction" and task["keyspace"] == keyspace and task["table"] == table]
                assert statuses, f"regular compaction task for {t0} was not created"

                failed = [status["task_id"] for status in statuses if status["state"] != "done"]
                assert not failed, f"Regular compaction tasks with ids = {failed} failed"
    drain_module_tasks(rest_api, module_name)

def test_running_compaction_task_abort(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                injection = "compaction_regular_compaction_task_executor_do_run"
                with scylla_inject_error(rest_api, injection, True):
                    [_, table] = t0.split(".")
                    resp = rest_api.send("POST", f"column_family/autocompaction/{keyspace}:{table}")
                    resp.raise_for_status()

                    tasks = [task for task in list_tasks(rest_api, module_name, False, keyspace, table) if task["type"] == "regular compaction"]
                    assert tasks, "Compaction task was not created"

                    for task in tasks:
                        abort_task(rest_api, task["task_id"])

                    resp = rest_api.send("POST", f"v2/error_injection/injection/{injection}/message")
                    resp.raise_for_status()

                    statuses = [wait_for_task(rest_api, task["task_id"]) for task in tasks]
                    aborted = [status for status in statuses if "abort requested" in status["error"]]
                    assert aborted, "Task wasn't aborted by user"
                    assert all([status["state"] == "failed" for status in aborted]), "Task finished successfully"
    drain_module_tasks(rest_api, module_name)

def run_major_compaction(rest_api, keyspace):
    resp = rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}")
    resp.raise_for_status()

def test_not_created_compaction_task_abort(cql, this_dc, rest_api):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                injection = "compaction_major_keyspace_compaction_task_impl_run"
                with scylla_inject_error(rest_api, injection, True):
                    [_, table] = t0.split(".")
                    # FIXME: Replace with asynchronous compaction api call as soon as it is available.
                    x = threading.Thread(target=run_major_compaction, args=(rest_api, keyspace,))
                    x.start()

                    tasks = []
                    while not tasks:
                        tasks = [task for task in list_tasks(rest_api, module_name) if task["type"] == "major compaction"]
                    assert len(tasks) == 1, "More than one top level task was created"
                    task = tasks[0]

                    abort_task(rest_api, task["task_id"])

                    resp = rest_api.send("POST", f"v2/error_injection/injection/{injection}/message")
                    resp.raise_for_status()

                    status = wait_for_task(rest_api, task["task_id"])
                    assert status["state"] == "failed", "Task finished successfully despite abort"
                    assert "abort" in status["error"], "Task wasn't aborted by user"

                    if "children_ids" in status:
                        children = [wait_for_task(rest_api, child_id) for child_id in status["children_ids"]]
                        assert all(child["state"] == "failed" for child in children), "Some child tasks finished successfully despite abort"
                        assert all("abort requested" in child["error"] for child in children), "Some child tasks weren't aborted by user"
                        assert all("children" not in child for child in children), "Some child tasks spawned new tasks even though they were aborted"
                    x.join()
    drain_module_tasks(rest_api, module_name)

def test_major_keyspace_compaction_task_async(cql, this_dc, rest_api):
    checkout_async_task(cql, this_dc, rest_api, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_compaction/{keyspace}"), "major compaction")

def test_cleanup_keyspace_compaction_task_async(cql, this_dc, rest_api):
    checkout_async_task(cql, this_dc, rest_api, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_cleanup/{keyspace}"), "cleanup compaction")

def test_offstrategy_keyspace_compaction_task_async(cql, this_dc, rest_api):
    checkout_async_task(cql, this_dc, rest_api, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_offstrategy_compaction/{keyspace}"), "offstrategy compaction")

def test_rewrite_sstables_keyspace_compaction_task_async(cql, this_dc, rest_api):
    # upgrade sstables compaction
    checkout_async_task(cql, this_dc, rest_api, lambda keyspace: rest_api.send("GET", f"tasks/compaction/keyspace_upgrade_sstables/{keyspace}"), "upgrade sstables compaction")
    # scrub sstables compaction
    checkout_async_task(cql, this_dc, rest_api, lambda keyspace: rest_api.send("GET", f"tasks/compaction/keyspace_scrub/{keyspace}"), "scrub sstables compaction")
