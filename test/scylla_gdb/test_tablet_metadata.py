# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Test for `scylla tablet-metadata` against a non-empty tablet map.

test_basic_commands.py already runs `tablet-metadata`, but the suite's cluster has no
user keyspaces, so `tablet_metadata::_tablets` is empty, the per-table loop never runs
and everything inside it is untested. Create one tablet table so the traversal happens,
and check what the command prints against system.tablets rather than against the exit
status alone: an exit-status-only assertion passes just as happily when the loop body
is skipped, which is how a broken field access inside the loop stayed hidden.
"""

import re

import pytest

from cassandra.auth import PlainTextAuthProvider          # type: ignore
from cassandra.cluster import Cluster                     # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore

from test.scylla_gdb.conftest import execute_gdb_command

pytestmark = [
    pytest.mark.skip_mode(
        mode=["dev", "debug", "coverage"],
        reason="Scylla was built without debug symbols; use release mode",
    ),
]

KEYSPACE = "gdb_tablet_metadata"
TABLE = "t"


@pytest.fixture(scope="module")
def tablet_table(scylla_server):
    """Create a tablet table and return its (tablet_count, last_tokens) from system.tablets."""
    addr = str(scylla_server.ip_addr)
    cluster = Cluster(
        contact_points=[addr],
        load_balancing_policy=WhiteListRoundRobinPolicy([addr]),
        auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
        protocol_version=4,
    )
    session = cluster.connect()
    try:
        session.execute(
            f"CREATE KEYSPACE {KEYSPACE} WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'enabled': true}"
        )
        session.execute(
            f"CREATE TABLE {KEYSPACE}.{TABLE} (pk int PRIMARY KEY) "
            "WITH tablets = {'min_tablet_count': 3}"
        )
        rows = list(session.execute(
            "SELECT tablet_count, last_token FROM system.tablets "
            f"WHERE keyspace_name = '{KEYSPACE}' AND table_name = '{TABLE}' ALLOW FILTERING"))
        assert rows, "the table has no rows in system.tablets"
        # last_token is a clustering key, so the rows already come back in tablet order.
        yield rows[0].tablet_count, [r.last_token for r in rows]
    finally:
        try:
            session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
        finally:
            cluster.shutdown()


def test_tablet_metadata_matches_system_tablets(gdb_cmd, tablet_table):
    tablet_count, last_tokens = tablet_table

    result = execute_gdb_command(gdb_cmd, "tablet-metadata")
    assert result.returncode == 0, (
        f"GDB command `tablet-metadata` failed. stdout: {result.stdout} stderr: {result.stderr}"
    )

    # The loop body has to have run for this table.
    header = re.search(rf"^table {KEYSPACE}\.{TABLE}: .*?tablets: (\d+)", result.stdout, re.MULTILINE)
    assert header, (
        f"`tablet-metadata` printed no line for {KEYSPACE}.{TABLE}, so the tablet map was "
        f"never walked. stdout: {result.stdout} stderr: {result.stderr}"
    )
    assert int(header.group(1)) == tablet_count, (
        f"`tablet-metadata` reported {header.group(1)} tablets for {KEYSPACE}.{TABLE}, "
        f"system.tablets says {tablet_count}. stdout: {result.stdout}"
    )

    # And every boundary it printed has to be the one recorded for that tablet.
    body = result.stdout.split(f"table {KEYSPACE}.{TABLE}:")[1].split("\ntable ")[0]
    printed = [int(t) for t in re.findall(r"^\s+tablet#\d+: last token: (-?\d+)", body, re.MULTILINE)]
    assert printed == last_tokens, (
        f"`tablet-metadata` printed last tokens {printed} for {KEYSPACE}.{TABLE}, "
        f"system.tablets has {last_tokens}"
    )
