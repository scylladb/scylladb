# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Tests for printing rows (the cells of a clustering row) in memory.
"""

import pytest

from test.cqlpy.util import cql_session, unique_name
from test.pylib.rest_client import ScyllaRESTAPIClient
from test.scylla_gdb.conftest import execute_gdb_command

nr_columns = 20


@pytest.fixture(scope="module")
async def cached_table(scylla_server):
    """A table whose rows are in the row cache, as (keyspace, table)."""
    ks, ip = unique_name(), str(scylla_server.ip_addr)
    with cql_session(ip, 9042, False, "cassandra", "cassandra") as cql:
        # vnodes rather than tablets, to spread the partitions over all shards.
        cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                    " AND tablets = {'enabled': false}")
        columns = ", ".join(f"v{i:02} text" for i in range(nr_columns))
        cql.execute(f"CREATE TABLE {ks}.t (pk int, ck int, {columns}, PRIMARY KEY (pk, ck))")
        names = ", ".join(f"v{i:02}" for i in range(nr_columns))
        values = ", ".join(f"'gdb-cell-{i:02}'" for i in range(nr_columns))
        for pk in range(100):
            cql.execute(f"INSERT INTO {ks}.t (pk, ck, {names}) VALUES ({pk}, 0, {values})")
        await ScyllaRESTAPIClient().keyspace_flush(ip, ks, "t")
        # Flushing doesn't populate the cache, reading does.
        for pk in range(100):
            cql.execute(f"SELECT * FROM {ks}.t WHERE pk = {pk}")
        yield ks, "t"
        cql.execute(f"DROP KEYSPACE {ks}")


def test_row_printer(gdb_cmd, cached_table):
    ks, cf = cached_table
    result = execute_gdb_command(gdb_cmd, full_command=f'print $get_row("{ks}", "{cf}")')
    assert result.returncode == 0, f"stdout: {result.stdout} stderr: {result.stderr}"
    # A failing pretty printer doesn't fail the command, it reports an exception.
    assert "Python Exception" not in result.stderr, f"stdout: {result.stdout} stderr: {result.stderr}"
    assert "cells=" in result.stdout, f"stdout: {result.stdout} stderr: {result.stderr}"
