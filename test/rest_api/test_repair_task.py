import functools
import requests
import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import set_tmp_task_ttl
from task_manager_utils import list_tasks

empty_id = "00000000-0000-0000-0000-000000000000"

def get_status(rest_api, task, sequence_number):
    task_id = task["task_id"]
    resp = rest_api.send("GET", f"task_manager/task_status/{task_id}")
    if resp.status_code == requests.codes.ok:
        status = resp.json()
        if status["sequence_number"] == sequence_number:
            assert status["progress_completed"] == status["progress_total"], "invalid progress"
            return status
    return None

def test_repair_task_progress(cql, this_dc, rest_api):
    long_time = 1000000000
    with set_tmp_task_ttl(rest_api, long_time):
        # Insert some data.
        with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
            schema = 'p int, v text, primary key (p)'
            with new_test_table(cql, keyspace, schema) as t0:
                stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [0, 'hello'])

                # Run repair and wait until it is finished.
                resp = rest_api.send("POST", f"storage_service/repair_async/{keyspace}")
                resp.raise_for_status()
                sequence_number = resp.json()
                resp = rest_api.send("GET", f"storage_service/repair_status", { "id": sequence_number })
                resp.raise_for_status()

                # Get all repairs.
                tasks = list_tasks(rest_api, "repair", internal=True)

                statuses = list(filter(lambda s : s is not None, map(lambda s: get_status(rest_api, s, sequence_number), tasks)))

                roots = list(filter(lambda s : s["parent_id"] == empty_id, statuses))
                assert len(roots) == 1, "incorrect roots number"
                root = roots[0]

                descendants = list(filter(lambda s : s["parent_id"] == root["parent_id"], statuses))
                progress_total = functools.reduce(lambda a, b: a + b, map(lambda s : s["progress_total"], descendants))
                progress_completed = functools.reduce(lambda a, b: a + b, map(lambda s : s["progress_completed"], descendants))
                assert progress_total == root["progress_total"], "incorrect total progress value"
                assert progress_completed == root["progress_completed"], "incorrect completed progress value"
