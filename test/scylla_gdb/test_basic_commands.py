# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Basic tests for commands that does not require additional options.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest
import re


@pytest.mark.parametrize(
    "command",
    [
        "features",
        "compaction-tasks",
        "databases",
        "commitlog",
        "tables",
        "table system.local",
        "tablet-metadata",
        "keyspaces",
        "active-sstables",
        "sstables",
        "memtables",
        "repairs",
        "gms",
        "heapprof",
        "io-queues",
        "cache",
        "mem-range",
        "mem-ranges",
        "memory",
        "segment-descs",
        "small-object -o 32 --random-page",
        "small-object -o 64 --summarize",
        "large-objects -o 131072 --random-page",
        "large-objects -o 32768 --summarize",
        "lsa",
        "netw",
        "smp-queues",
        "task-queues",
        "task_histogram",
        "tasks",
        "threads",
        "timers",
        "get-config-value compaction_static_shares",
        "read-stats",
        "prepared-statements",
    ],
)
def test_scylla_commands(gdb_execute, command):
    gdb_execute(command)


def test_task_histogram_coro(gdb_execute):
    result = gdb_execute("task_histogram -a")
    assert re.search(
        r"\) \[clone \.\w+]", result.stdout
    ), "no coroutine entries are found in task_histogram"


def test_nonexistent_scylla_command(gdb_execute):
    """Verifies that running unknown command will produce correct error message"""
    with pytest.raises(
        Exception, match=r'Undefined scylla command: "nonexistent_command"'
    ):
        gdb_execute("nonexistent_command")
