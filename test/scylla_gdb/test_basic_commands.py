# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Basic tests for commands that does not require additional options.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest

from test.scylla_gdb.conftest import execute_gdb_command

pytestmark = [
    pytest.mark.skip_mode(
        mode=["dev", "debug"],
        reason="Scylla was built without debug symbols; use release mode",
    ),
    pytest.mark.skip_mode(
        mode=["dev", "debug", "release"],
        platform_key="aarch64",
        reason="GDB is broken on aarch64: https://sourceware.org/bugzilla/show_bug.cgi?id=27886",
    ),
]


@pytest.mark.parametrize(
    "command",
    [
        "timers",
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
        "task_histogram -a",
        "tasks",
        "threads",
        "get-config-value compaction_static_shares",
        "read-stats",
        "prepared-statements",
    ],
)
def test_scylla_commands(gdb_cmd, command):
    result = execute_gdb_command(gdb_cmd, command)
    assert result.returncode == 0, (
        f"GDB command {command} failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_nonexistent_scylla_command(gdb_cmd):
    """Verifies that running unknown command will produce correct error message"""
    result = execute_gdb_command(gdb_cmd, "nonexistent_command")
    assert result.returncode == 1
    assert  "Undefined scylla command: \"nonexistent_command\"" in result.stderr
