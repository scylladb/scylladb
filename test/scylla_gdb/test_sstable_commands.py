# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Tests for commands that need an sstable to work on.
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
        "sstable-summary",
        "sstable-index-cache",
    ],
)
def test_sstable(gdb_cmd, command):
    result = execute_gdb_command(gdb_cmd, f"{command} $get_sstable()")
    assert result.returncode == 0, (
        f"GDB command {command} failed. stdout: {result.stdout} stderr: {result.stderr}"
    )


def test_sstable_summary_output(gdb_cmd):
    """Verify sstable-summary produces expected output fields.

    For ms-format sstables with the partitions db footer loaded, the output
    should contain first_key, last_key, partition_count, and trie_root_position.
    For ms-format sstables where the footer has not been lazily loaded yet,
    expects an informational message indicating the footer is not loaded.
    For legacy formats, it should contain header, first_key, and last_key.
    """

    result = execute_gdb_command(gdb_cmd, f"sstable-summary $get_sstable()")
    assert result.returncode == 0, (
        f"sstable-summary failed. stdout: {result.stdout} stderr: {result.stderr}"
    )
    output = result.stdout

    if 'ms format (trie-based index)' in output:
        # ms-format sstable with footer loaded: data comes from _partitions_db_footer
        print('test branch: ms-format with footer loaded')
        assert 'first_key:' in output, (
            f"Missing first_key in ms-format output: {output}"
        )
        assert 'last_key:' in output, (
            f"Missing last_key in ms-format output: {output}"
        )
        assert 'partition_count:' in output, (
            f"Missing partition_count in ms-format output: {output}"
        )
        assert 'trie_root_position:' in output, (
            f"Missing trie_root_position in ms-format output: {output}"
        )
    elif 'ms format' in output:
        # ms-format sstable but partitions db footer is not loaded yet
        print('test branch: ms-format with footer NOT loaded')
        assert (
            'sstable uses ms format but partitions db footer is not loaded' in output
        ), f"Unexpected ms-format output: {output}"
    else:
        # Legacy format: data comes from summary
        print('test branch: legacy format')
        assert 'header:' in output, f"Missing header in legacy output: {output}"
        assert 'first_key:' in output, f"Missing first_key in legacy output: {output}"
        assert 'last_key:' in output, f"Missing last_key in legacy output: {output}"
