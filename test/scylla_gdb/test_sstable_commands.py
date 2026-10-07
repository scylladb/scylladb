# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Tests for commands that need an sstable to work on.
"""

import re

import pytest

from test.cqlpy.util import cql_session
from test.pylib.rest_client import ScyllaRESTAPIClient
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


@pytest.fixture
async def index_cached_table(scylla_server):
    """A flushed `me` table with loaded partition index pages; returns {pk: token}.

    `ms`/`mt` use a trie index instead, and the row cache is off so reads reach the index.
    """
    host = str(scylla_server.ip_addr)
    with cql_session(host, 9042, False, "cassandra", "cassandra") as cql:
        old_format = cql.execute("SELECT value FROM system.config WHERE name = 'sstable_format'").one().value.strip('"')
        cql.execute("UPDATE system.config SET value = 'me' WHERE name = 'sstable_format'")
        cql.execute("CREATE KEYSPACE ks_index_cache WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}")
        try:
            cql.execute("CREATE TABLE ks_index_cache.t (pk text PRIMARY KEY, v int) WITH caching = {'enabled': false}")
            for i in range(50):
                cql.execute(f"INSERT INTO ks_index_cache.t (pk, v) VALUES ('key{i}', {i})")
            await ScyllaRESTAPIClient().keyspace_flush(host, "ks_index_cache", "t")
            tokens = {}
            for i in range(50):
                row = cql.execute(f"SELECT pk, token(pk) AS t FROM ks_index_cache.t WHERE pk = 'key{i}'").one()
                tokens[row.pk] = row.t
            yield tokens
        finally:
            cql.execute("DROP KEYSPACE ks_index_cache")
            cql.execute(f"UPDATE system.config SET value = '{old_format}' WHERE name = 'sstable_format'")


def test_sstable_index_cache_output(gdb_cmd, index_cached_table):
    """Verify sstable-index-cache prints the keys and tokens of a loaded index page."""

    result = execute_gdb_command(gdb_cmd, full_command="python gdb.execute('scylla sstable-index-cache $get_table_sstable(\"ks_index_cache\", \"t\")')")
    assert result.returncode == 0, f"stdout: {result.stdout} stderr: {result.stderr}"
    assert re.search(r"\([1-9]\d* loaded", result.stdout), result.stdout

    entries = re.findall(r"\{ key: (\w*), token: (\S+), position: \d+ \}", result.stdout)
    assert entries, result.stdout
    for key, token in entries:
        pk = bytes.fromhex(key).decode()
        assert pk in index_cached_table, result.stdout
        # Tokens are computed lazily, so some may not be there yet.
        assert token == "null" or int(token) == index_cached_table[pk], result.stdout
