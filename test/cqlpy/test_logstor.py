# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the logstor storage engine. See additional tests, perhaps
# involving multiple nodes, reboots, etc., in test/cluster/test_logstor.py.
#############################################################################

import pytest
from .util import new_test_table

# All tests in this file are Scylla-only (logstor is not available on Cassandra).
@pytest.fixture(scope="module", autouse=True)
def logstor_scylla_only(scylla_only):
    pass

# Test basic logstor key-value table, where the value is a single column.
# We check write, overwrite, delete and single-row read.
def test_logstor_single_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, 200)")

        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v == 100

        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 2").one()
        assert row.pk == 2 and row.v == 200

        # Missing row
        assert cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 99").one() is None

        # Overwrite
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 999)")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.v == 999

        # Delete
        cql.execute(f"DELETE FROM {table} WHERE pk = 2")
        assert cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 2").one() is None

# Test a whole-table scan of a simple single-column logstor table.
def test_logstor_scan(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 10)")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, 20)")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (3, 30)")

        rows = cql.execute(f"SELECT pk, v FROM {table}")
        assert {(r.pk, r.v) for r in rows} == {(1, 10), (2, 20), (3, 30)}

# Test a logstor key-value table where the value has *two* columns. This test
# checks the simpler case of INSERT that writes both columns.
def test_logstor_two_columns_full_insert(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v1 int, v2 text",
                        " WITH storage_engine = 'logstor'") as table:
        # Write a row with two value columns - v1 and v2 - and verify it is
        # written.
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (1, 10, 'hello')")
        row = cql.execute(f"SELECT pk, v1, v2 FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v1 == 10 and row.v2 == 'hello'

        # Overwriting an entire row (both columns) works and the new value is
        # read back correctly.
        cql.execute(f"INSERT INTO {table} (pk, v1, v2) VALUES (1, 11, 'bye')")
        row = cql.execute(f"SELECT pk, v1, v2 FROM {table} WHERE pk = 1").one()
        assert row.v1 == 11 and row.v2 == 'bye'

# Test what happens when an INSERT inserts a partial row (setting only one of
# two regular columns). In normal CQL, such a partial INSERT keeps the other
# column unchanged from its previous value (if any). In logstor, the behavior
# is different: Logstor treats every INSERT as a full replacement of the entire
# row. This is surprising because it differs from normal CQL behavior, but it
# is deliberate, and this test verifies that it works as intended.
def test_logstor_two_columns_partial_insert(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v1 int, v2 text",
                        " WITH storage_engine = 'logstor'") as table:
        # Insert only v1; v2 should be NULL.
        cql.execute(f"INSERT INTO {table} (pk, v1) VALUES (1, 42)")
        row = cql.execute(f"SELECT pk, v1, v2 FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v1 == 42 and row.v2 is None

        # Insert only v2; The old item is replaced entirely - the old v1 is
        # not kept, and v1 becomes NULL.
        cql.execute(f"INSERT INTO {table} (pk, v2) VALUES (1, 'only_text')")
        row = cql.execute(f"SELECT pk, v1, v2 FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v1 is None and row.v2 == 'only_text'

        # Overwrite pk=1 with only v2; v1 should now be NULL (full replacement).
        cql.execute(f"INSERT INTO {table} (pk, v2) VALUES (1, 'replaced')")
        row = cql.execute(f"SELECT pk, v1, v2 FROM {table} WHERE pk = 1").one()
        assert row.v2 == 'replaced' and row.v1 is None

# Test if we can do exactly the same as we did above with INSERT but using the
# UPDATE statement instead of INSERT. Currently, this doesn't work, we get a
# cassandra.WriteFailure. We mark this test xfail because we should decide
# either this request should be not supported (and fail cleanly with an
# InvalidRequest) or it should be supported and work the same as an INSERT.
@pytest.mark.xfail(reason="Currently UPDATE is not supported on logstor tables, but we should decide whether to support it or reject it cleanly")
def test_logstor_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"UPDATE {table} SET v = 42 WHERE pk = 1")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v == 42
