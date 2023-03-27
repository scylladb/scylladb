import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import set_tmp_task_ttl
from task_manager_utils import wait_for_task, list_tasks, check_child_parent_relationship

def check_compaction_task(cql, this_dc, rest_api, run_compaction):
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
                tasks = list_tasks(rest_api, "compaction")
                assert tasks, "compaction task was not created"

                # Check if all tasks finished successfully.
                statuses = [wait_for_task(rest_api, task["task_id"]) for task in tasks]
                failed = [task["task_id"] for task in statuses if task["state"] != "done"]
                assert not failed, f"tasks with ids {failed} failed"

                for top_level_task in statuses:
                    check_child_parent_relationship(rest_api, top_level_task)

def test_major_keyspace_compaction_task(cql, this_dc, rest_api):
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}"))

def test_major_column_family_compaction_task(cql, this_dc, rest_api):
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, table: rest_api.send("POST", f"column_family/major_compaction/{keyspace}:{table}"))

def test_cleanup_keyspace_compaction_task(cql, this_dc, rest_api):
    check_compaction_task(cql, this_dc, rest_api, lambda keyspace, _: rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}"))
