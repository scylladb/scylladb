# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a sstable to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""

import pytest
import re


@pytest.fixture(scope="module")
def sstable(gdb_execute):
    """Finds sstable"""
    result = gdb_execute(full_command="python sstables_config()")

    match = re.search(r"(\(sstables::sstable \*\) 0x)([0-9a-f]+)", result)
    assert match is not None, "No sstable was present in result.stdout"
    sstable_pointer = match.group(0).strip() if match else None

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
