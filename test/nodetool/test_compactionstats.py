#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
from datetime import timedelta
from uuid import uuid1
from test.nodetool.rest_api_mock import expected_request


def create_task(i, num_pending_tasks):
    return {"ks": "ks",
            "cf": f"cf{i}",
            "task": num_pending_tasks}


def create_compaction_stats(ks, cf):
    return {"id": str(uuid1()),
            "ks": ks,
            "cf": cf,
            "unit": "keys",
            "completed": 42,
            "total": 10240,
            "task_type": "COMPACTION"}


def format_task(task):
    ks = task["ks"]
    cf = task["cf"]
    num = task["task"]
    return f"- {ks}.{cf}: {num}\n"


def format_compaction(compaction, progress=None):
    task_id = compaction['id']
    task_type = compaction['task_type']
    keyspace = compaction['ks']
    table = compaction['cf']
    completed = compaction['completed']
    total = compaction['total']
    unit = compaction['unit']
    if progress is None:
        percent = completed * 100 / total
        progress = f"{percent:.2f}%"
    return f'{task_id:<36} {task_type:<15} {keyspace:<8} {table:<5} {completed:<9} {total:<5} {unit:<4} {progress:<8}\n'


@pytest.mark.parametrize("num_compactions", [0, 1, 2])
@pytest.mark.parametrize("throughput", [0, 1024])
def test_compactionstats(nodetool, request, num_compactions, throughput):
    pending_tasks = [create_task(i, 1) for i in range(num_compactions)]
    compactions = [create_compaction_stats(task["ks"], task["cf"])
                   for task in pending_tasks]
    expected_requests = [
        expected_request("GET", "/compaction_manager/metrics/pending_tasks_by_table",
                         response=pending_tasks),
        expected_request("GET", "/compaction_manager/compactions",
                         response=compactions),
    ]
    if request.config.getoption("nodetool") == "cassandra" or len(compactions) > 0:
        # scylla nodetool does not bother reading throughput if there is no
        # pending compaction. but cassandra's nodetool always does.
        expected_requests.append(
            expected_request("GET", "/storage_service/compaction_throughput",
                             response=throughput))
    res = nodetool("compactionstats", expected_requests=expected_requests)
    actual_output = res.stdout
    expected_output = f"pending tasks: {num_compactions}\n"
    for task in pending_tasks:
        expected_output += format_task(task)
    expected_output += "\n"

    if num_compactions > 0:
        header = {"id": "id",
                  "task_type": "compaction type",
                  "ks": "keyspace",
                  "cf": "table",
                  "completed": "completed",
                  "total": "total",
                  "unit": "unit"}
        expected_output += format_compaction(header, progress="progress")

        remaining_bytes = 0
        for compaction in compactions:
            expected_output += format_compaction(compaction)
            remaining_bytes += compaction['total'] - compaction['completed']

        remaining_time = 'n/a'
        if throughput > 0:
            remaining_secs = remaining_bytes // (throughput * 1024 * 1024)
            hours, remainder = divmod(remaining_secs, timedelta(hours=1).seconds)
            minutes, seconds = divmod(remainder, timedelta(minutes=1).seconds)
            remaining_time = f'{hours}h{minutes:0>2}m{seconds:0>2}s'

        expected_output += f'Active compaction remaining time : {remaining_time:>10}\n'

    assert actual_output == expected_output
