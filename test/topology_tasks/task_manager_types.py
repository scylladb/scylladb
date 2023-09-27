#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from enum import StrEnum
from typing import NewType, NamedTuple


TaskID = NewType('TaskID', str)
SequenceNum = NewType('SequenceNum', str)


class State(StrEnum):
    created = "created"
    running = "running"
    done = "done"
    failed = "failed"


class TaskStats(NamedTuple):
    """Basic stats of Task Manager's tasks"""
    task_id: TaskID
    state: State
    type: str
    scope: str
    keyspace: str
    table: str
    entity: str
    sequence_number: SequenceNum


class TaskStatus(NamedTuple):
    """Full status of Task Manager's tasks"""
    id: TaskID
    state: State
    type: str
    scope: str
    keyspace: str
    table: str
    entity: str
    sequence_number: SequenceNum
    is_abortable: bool
    start_time: str
    end_time: str
    error: str
    parent_id: TaskID
    shard: int
    progress_units: str
    progress_total: float
    progress_completed: float
    children_ids: list[TaskID] = []
