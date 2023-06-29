import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import set_tmp_task_ttl
from task_manager_utils import wait_for_task, list_tasks, check_child_parent_relationship

# depth parameter means the number of edges in the longest path from root to leaves in task tree.
def check_compaction_task(cql, this_dc, rest_api, run_compaction, compaction_type, depth):
    long_time = 1000000000
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
                tasks = [task for task in list_tasks(rest_api, "compaction") if task["type"] == compaction_type]
                assert tasks, "compaction task was not created"

                # Check if all tasks finished successfully.
                statuses = [wait_for_task(rest_api, task["task_id"]) for task in tasks]
                failed = [task["task_id"] for task in statuses if task["state"] != "done"]
                assert not failed, f"tasks with ids {failed} failed"

                for top_level_task in statuses:
                    if top_level_task["type"] != "resharding compaction":
                        check_child_parent_relationship(rest_api, top_level_task, depth)

def test_major_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 3
    # keyspace major compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}"), "major compaction", task_tree_depth)
    # column family major compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}"), "major compaction", task_tree_depth)

def test_cleanup_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 2
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}"), "cleanup compaction", task_tree_depth)

def test_offstrategy_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 2
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}"), "offstrategy compaction", task_tree_depth)

def test_rewrite_sstables_keyspace_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 2
    # upgrade sstables compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}"), "rewrite sstables compaction", task_tree_depth)
    # scrub sstables compaction
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}"), "rewrite sstables compaction", task_tree_depth)

def test_reshaping_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 1
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "reshaping compaction", task_tree_depth)

def test_resharding_compaction_task(cql, this_dc, rest_api):
    task_tree_depth = 1
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"storage_service/sstables/{keyspace}", {'cf': table, 'load_and_stream': False}), "resharding compaction", task_tree_depth)
