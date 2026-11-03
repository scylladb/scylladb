# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import time

import pytest

from . import nodetool
from .util import new_test_table



def has_snapshot(snapshots, tag):
    for s in snapshots:
        if s['key'] == tag:
            return s['value']
    return None

def test_snapshot_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        write = cql.prepare(f"INSERT INTO {table} (k) VALUES (?)")
        for i in range(10):
            cql.execute(write, [i])
        nodetool.flush(cql, table)

        tag = "test_snapshot"
        ttl = 60

        # Take a snapshot of the table
        before = time.time()
        nodetool.take_snapshot(cql, table, tag, ttl=f"{ttl}s")

        # Verify that the snapshot exists
        snapshots = nodetool.list_snapshots(cql)
        print(f"Snapshots for {table}: {snapshots}")
        infos = has_snapshot(snapshots, tag)
        assert len(infos) == 1, f"Expected to find {tag} in {snapshots=}, found {len(infos)} infos"
        info = infos[0]

        # The creation time is truncated to whole seconds, so it may precede
        # the time measured just before the snapshot was taken.
        created_at = float(info["created_at"])
        assert before - 1 <= created_at <= time.time()
        # Cassandra reports the times with a sub-second resolution
        assert float(info["expires_at"]) - created_at == pytest.approx(ttl)


def test_snapshot_without_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        cql.execute(f"INSERT INTO {table} (k) VALUES (0)")
        nodetool.flush(cql, table)

        tag = "test_snapshot_without_ttl"
        before = time.time()
        nodetool.take_snapshot(cql, table, tag)
        try:
            infos = has_snapshot(nodetool.list_snapshots(cql), tag)
            assert len(infos) == 1, f"Expected to find exactly one {tag} snapshot, found {len(infos)}"
            info = infos[0]

            created_at = float(info["created_at"])
            assert before - 1 <= created_at <= time.time()
            # A snapshot taken with no ttl never expires
            assert not info.get("expires_at")
        finally:
            nodetool.del_snapshot(cql, tag)
