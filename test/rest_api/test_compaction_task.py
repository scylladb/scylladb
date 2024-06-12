import requests
import sys
import threading

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import set_tmp_task_ttl, scylla_inject_error
from test.rest_api.task_manager_utils import wait_for_task, list_tasks, check_child_parent_relationship, drain_module_tasks, abort_task, get_task_status, get_task_status_recursively, get_children

module_name = "compaction"
long_time = 1000000000

def get_status_if_exists(rest_api, task_id):
    resp = rest_api.send("GET", f"task_manager/task_status/{task_id}")
    if resp.status_code == requests.codes.ok:
        return resp.json()

# depth parameter means the number of edges in the longest path from root to leaves in task tree.
def check_compaction_task(cql, rest_api, keyspace, run_compaction, compaction_type, depth, allow_no_children=False):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
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

            for root_id in [s["id"] for s in statuses]:
                status_tree = get_task_status_recursively(rest_api, root_id)
                check_child_parent_relationship(rest_api, status_tree, status_tree[0], allow_no_children)
    drain_module_tasks(rest_api, module_name)

def checkout_async_task(cql, rest_api, keyspace, run_compaction_async, compaction_type):
    drain_module_tasks(rest_api, module_name)
    with set_tmp_task_ttl(rest_api, long_time):
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

def test_global_major_keyspace_compaction_task(cql, rest_api, test_keyspace):
    task_tree_depth = 4
    # global major compaction
    check_compaction_task(cql, rest_api, test_keyspace, lambda _, __: rest_api.send("POST", f"storage_service/compact"), "major compaction", task_tree_depth, allow_no_children=True)

    # global major compaction, flush option
    check_compaction_task(cql, rest_api, test_keyspace, lambda _, __: rest_api.send("POST", f"storage_service/compact?flush_memtables=true"), "major compaction", task_tree_depth, allow_no_children=True)
    check_compaction_task(cql, rest_api, test_keyspace, lambda _, __: rest_api.send("POST", f"storage_service/compact?flush_memtables=false"), "major compaction", task_tree_depth, allow_no_children=True)

def test_major_keyspace_compaction_task(cql, rest_api, test_keyspace):
    task_tree_depth = 3
    # keyspace major compaction
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}"), "major compaction", task_tree_depth)
    # column family major compaction
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}"), "major compaction", task_tree_depth)

    # keyspace major compaction, flush option
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}?flush_memtables=true"), "major compaction", task_tree_depth)
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}?flush_memtables=false"), "major compaction", task_tree_depth)
    # column family major compaction, flush option
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}?flush_memtables=true"), "major compaction", task_tree_depth)
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}?flush_memtables=false"), "major compaction", task_tree_depth)

def test_cleanup_keyspace_compaction_task(cql, rest_api, test_keyspace_vnodes):
    task_tree_depth = 3
    check_compaction_task(cql, rest_api, test_keyspace_vnodes, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}"), "cleanup compaction", task_tree_depth, True)

def test_offstrategy_keyspace_compaction_task(cql, rest_api, test_keyspace):
    task_tree_depth = 3
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}"), "offstrategy compaction", task_tree_depth, True)

def test_rewrite_sstables_keyspace_compaction_task(cql, rest_api, test_keyspace):
    task_tree_depth = 2
    # upgrade sstables compaction
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}"), "upgrade sstables compaction", task_tree_depth)
    # scrub sstables compaction
    check_compaction_task(cql, rest_api, test_keyspace, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}"), "scrub sstables compaction", task_tree_depth)

def test_reshaping_compaction_task(cql, rest_api, test_keyspace_vnodes):
    task_tree_depth = 2
    check_compaction_task(cql, rest_api, test_keyspace_vnodes, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "reshaping compaction", task_tree_depth, True)

def test_resharding_compaction_task(cql, rest_api, test_keyspace_vnodes):
    task_tree_depth = 2
    check_compaction_task(cql, rest_api, test_keyspace_vnodes, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "resharding compaction", task_tree_depth, True)

def test_regular_compaction_task(cql, this_dc, rest_api):
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

                    statuses = [get_status_if_exists(rest_api, task["task_id"]) for task in list_tasks(rest_api, "compaction", internal=True) if task["type"] == "regular compaction" and task["keyspace"] == keyspace and task["table"] == table]
                    statuses = [s for s in statuses if s is not None]
                    assert statuses, f"regular compaction task for {t0} was not created"
                    assert all([s["state"] != "done" and s["state"] != "failed" for s in statuses]), "Regular compaction task isn't unregiatered after it completes"

                    resp = rest_api.send("POST", f"v2/error_injection/injection/{injection}/message")
                    resp.raise_for_status()
    drain_module_tasks(rest_api, module_name)

def test_compaction_task_abort(cql, this_dc, rest_api):
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
                    resp = rest_api.send("POST", f"tasks/compaction/keyspace_compaction/{keyspace}")
                    resp.raise_for_status()
                    task_id = resp.json()

                    abort_task(rest_api, task_id)

                    resp = rest_api.send("POST", f"v2/error_injection/injection/{injection}/message")
                    resp.raise_for_status()

                    status = wait_for_task(rest_api, task_id)
                    assert status["state"] == "failed", "Task finished successfully despite abort"
                    assert "abort" in status["error"], "Task wasn't aborted by user"

                    status_tree = get_task_status_recursively(rest_api, status["id"])
                    if "children_ids" in status:
                        children = get_children(status_tree, status["id"])
                        assert all(child["state"] == "failed" for child in children), "Some child tasks finished successfully despite abort"
                        assert all("abort requested" in child["error"] for child in children), "Some child tasks weren't aborted by user"
                        assert all("children" not in child for child in children), "Some child tasks spawned new tasks even though they were aborted"
    drain_module_tasks(rest_api, module_name)

def test_major_keyspace_compaction_task_async(cql, rest_api, test_keyspace):
    checkout_async_task(cql, rest_api, test_keyspace, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_compaction/{keyspace}"), "major compaction")

def test_cleanup_keyspace_compaction_task_async(cql, rest_api, test_keyspace_vnodes):
    checkout_async_task(cql, rest_api, test_keyspace_vnodes, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_cleanup/{keyspace}"), "cleanup compaction")

def test_offstrategy_keyspace_compaction_task_async(cql, rest_api, test_keyspace):
    checkout_async_task(cql, rest_api, test_keyspace, lambda keyspace: rest_api.send("POST", f"tasks/compaction/keyspace_offstrategy_compaction/{keyspace}"), "offstrategy compaction")

def test_rewrite_sstables_keyspace_compaction_task_async(cql, rest_api, test_keyspace):
    # upgrade sstables compaction
    checkout_async_task(cql, rest_api, test_keyspace, lambda keyspace: rest_api.send("GET", f"tasks/compaction/keyspace_upgrade_sstables/{keyspace}"), "upgrade sstables compaction")
    # scrub sstables compaction
    checkout_async_task(cql, rest_api, test_keyspace, lambda keyspace: rest_api.send("GET", f"tasks/compaction/keyspace_scrub/{keyspace}"), "scrub sstables compaction")
