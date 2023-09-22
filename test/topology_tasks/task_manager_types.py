#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from enum import StrEnum
from typing import Any, NewType, NamedTuple

from test.pylib.internal_types import IPAddress


TaskID = NewType('TaskID', str)
SequenceNum = NewType('SequenceNum', str)


class TaskIdentity(NamedTuple):
    task_id: TaskID
    node: IPAddress

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
    kind: str
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
    kind: str
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
    children_ids: list[Any] = []

    def __eq__(self, other):
        def compare_field(name, value):
            return [TaskIdentity(**tid) for tid in value].sort() == [TaskIdentity(**tid) for tid in getattr(other, name)].sort() if name == "children_ids" else value == getattr(other, name)

        return type(other) is type(self) and all(compare_field(name, value) for name, value in self._asdict().items())
