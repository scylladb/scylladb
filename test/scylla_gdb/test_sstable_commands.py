# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a sstable to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest
import re


a = """python
db = sharded(gdb.parse_and_eval("::debug::the_database")).local()
sst = next(find_sstables())
print(f"sst=(sstables::sstable *)", sst)
"""


@pytest.fixture(scope="module")
def sstable(gdb_execute):
    """Finds sstable"""
    args = ["-ex", a]
    result = gdb_execute(args=args)

    match = re.search(r"sst=\s*(.*)", result.stdout)
    assert match is not None, "No sstable was present in result.stdout"
    sstable_pointer = match.group(1) if match else None

    return sstable_pointer


@pytest.mark.parametrize(
    "command",
    [
        "sstable-summary",
        "sstable-index-cache",
    ],
)
def test_sstable_summary(gdb_execute, command, sstable):
    gdb_execute(f"{command} {sstable}")
