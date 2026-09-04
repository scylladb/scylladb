# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Test that a PREPARE racing with a USE on the same connection prepares the
# statement under one keyspace on all shards. PREPARE used to hand the other
# shards a reference to the connection's client_state, which the owning shard
# could modify at the same time (SCYLLADB-3951).
#############################################################################

from concurrent.futures import ThreadPoolExecutor

from .util import new_test_keyspace, new_cql, unique_name
from .rest_api import scylla_inject_error, wait_for_injection_enter, message_injection

INJECTION = "cql_server_process_prepare_before_local_prepare"


def test_prepare_concurrent_with_use(cql, scylla_only):
    ks_opts = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }"
    with new_test_keyspace(cql, ks_opts) as ks1, new_test_keyspace(cql, ks_opts) as ks2:
        # The same table name in both keyspaces, so the unqualified name in the
        # prepared statement resolves to a different table under each of them.
        table = unique_name()
        for ks in (ks1, ks2):
            cql.execute(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v int)")
        try:
            # Rows exist only in ks1's table.
            for pk in range(32):
                cql.execute(f"INSERT INTO {ks1}.{table} (pk, v) VALUES ({pk}, {pk + 100})")
            with new_cql(cql) as ncql:
                ncql.execute(f"USE {ks1}")
                with scylla_inject_error(ncql, INJECTION, one_shot=True), \
                     ThreadPoolExecutor(max_workers=1) as executor:
                    prepare = executor.submit(ncql.prepare, f"SELECT v FROM {table} WHERE pk = ?")
                    # PREPARE is now paused after the other shards prepared the
                    # statement, before the local shard does.
                    wait_for_injection_enter(ncql, INJECTION)
                    assert not prepare.done()
                    # Change the keyspace while PREPARE is in flight. This is
                    # what used to race with the other shards' reads.
                    ncql.execute(f"USE {ks2}")
                    message_injection(ncql, INJECTION)
                    stmt = prepare.result()
                # The statement was prepared while ks1 was the keyspace, so the
                # bind metadata the server returned must name ks1, and the
                # statement must read ks1's table on every shard.
                assert stmt.column_metadata[0].keyspace_name == ks1
                for pk in range(32):
                    assert [(pk + 100,)] == [tuple(r) for r in ncql.execute(stmt, [pk])]
        finally:
            for ks in (ks1, ks2):
                cql.execute(f"DROP TABLE {ks}.{table}")
