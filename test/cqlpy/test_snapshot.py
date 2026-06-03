# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import time

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
        created_at = time.time()
        nodetool.take_snapshot(cql, table, tag, ttl=f"{ttl}s")
        expires_at = time.time() + ttl + 1

        # Verify that the snapshot exists
        snapshots = nodetool.list_snapshots(cql)
        print(f"Snapshots for {table}: {snapshots}")
        infos = has_snapshot(snapshots, tag)
        assert len(infos) == 1, f"Expected to find {tag} in {snapshots=}, found {len(infos)} infos"
        # Currently, Cassandra returns the snapshot creation and expiration times in nodetool listsnapshots.
        # Scylla does not support that yet
        info = infos[0]
        if info.get("created_at"):
            created_at = float(info["created_at"])
            assert info.get("expires_at")
            expires_at = float(info["expires_at"])
            assert expires_at - created_at == ttl
