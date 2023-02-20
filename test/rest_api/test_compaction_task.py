import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import set_tmp_task_ttl
from task_manager_utils import wait_for_task, list_tasks, check_child_parent_relationship

def test_major_keyspace_compaction_task(cql, this_dc, rest_api):
    long_time = 1000000000
    with set_tmp_task_ttl(rest_api, long_time):
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])
                cql.execute(stmt, [1, 'world'])

                # Run major compaction.
                resp = rest_api.send("POST", f"storage_service/keyspace_compaction/{keyspace}")
                resp.raise_for_status()

                # Get list of compaction tasks.
                tasks = list_tasks(rest_api, "compaction")
                assert tasks, "compaction task was not created"

                # Check if all tasks finished successfully.
                statuses = [wait_for_task(rest_api, task["task_id"]) for task in tasks]
                failed = [task["task_id"] for task in statuses if task["state"] != "done"]
                assert not failed, f"tasks with ids {failed} failed"

                for top_level_task in statuses:
                    check_child_parent_relationship(rest_api, top_level_task, 3, 0)
