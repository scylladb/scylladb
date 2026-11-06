# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Tests for commands, that need a sstable to work on.
Each only checks that the command does not fail - but not what it does or returns.
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
