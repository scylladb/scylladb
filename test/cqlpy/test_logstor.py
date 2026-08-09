# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the logstor storage engine. See additional tests, perhaps
# involving multiple nodes, reboots, etc., in test/cluster/test_logstor.py.
#############################################################################

import json

import pytest
from cassandra.protocol import ConfigurationException
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

# Test token range queries for logstor table.
def test_logstor_range_read(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        # Insert 10 rows
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i*10})")

        # Test full table scan with tokens
        rows = list(cql.execute(f"SELECT pk, v, token(pk) AS tok FROM {table}"))
        assert len(rows) == 10
        assert sorted([r.pk for r in rows]) == list(range(10))
        for row in rows:
            assert row.v == row.pk * 10

        # Verify rows are sorted by token
        tokens = [row.tok for row in rows]
        assert tokens == sorted(tokens)

        # Test token range query (middle range)
        rows_in_range = list(cql.execute(
            f"SELECT pk, v, token(pk) AS tok FROM {table} "
            f"WHERE token(pk) >= {tokens[2]} AND token(pk) < {tokens[5]}"))
        assert len(rows_in_range) == 3
        assert [row.tok for row in rows_in_range] == tokens[2:5]

        # Test token range query (first half)
        rows_first_half = list(cql.execute(
            f"SELECT pk, v, token(pk) AS tok FROM {table} "
            f"WHERE token(pk) >= {tokens[0]} AND token(pk) < {tokens[5]}"))
        assert len(rows_first_half) == 5
        assert [row.tok for row in rows_first_half] == tokens[0:5]

        # Test token range query (second half)
        rows_second_half = list(cql.execute(
            f"SELECT pk, v, token(pk) AS tok FROM {table} "
            f"WHERE token(pk) >= {tokens[5]}"))
        assert len(rows_second_half) == 5
        assert [row.tok for row in rows_second_half] == tokens[5:10]

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

# Test that clustering columns are not supported in logstor tables.
# Attempting to create a logstor table with clustering columns should fail.
def test_logstor_clustering_columns_disabled(cql, test_keyspace):
    with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with tables that have clustering columns"):
        with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            " WITH storage_engine = 'logstor'") as table:
            pass

# Test frozen map column with logstor storage engine.
def test_logstor_frozen_map(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v frozen<map<text, text>>",
                        " WITH storage_engine = 'logstor'") as table:
        # Insert initial map
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana'}})")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.pk == 1 and row.v == {'a': 'apple', 'b': 'banana'}

        # Overwrite with larger map
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, {{'a': 'apple', 'b': 'banana', 'c': 'cherry'}})")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.v == {'a': 'apple', 'b': 'banana', 'c': 'cherry'}

        # Delete
        cql.execute(f"DELETE FROM {table} WHERE pk = 1")
        assert cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one() is None

# Test timestamp-based conflict resolution in logstor.
def test_logstor_timestamp_conflict_resolution(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        # Insert with earlier timestamp
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100) USING TIMESTAMP 1000")
        # Insert the same key with later timestamp - should win
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 200) USING TIMESTAMP 2000")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.v == 200

        # Insert with earlier timestamp - should not overwrite
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 300) USING TIMESTAMP 1500")
        row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1").one()
        assert row.v == 200

# Test that DESCRIBE TABLE shows storage_engine property for logstor tables.
def test_logstor_describe(cql, test_keyspace):
    # Test logstor table has the property
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        result = cql.execute(f"DESCRIBE TABLE {table}")
        create_statement = result.one().create_statement
        assert "storage_engine = 'logstor'" in create_statement

    # Test normal table doesn't have the property
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int", "") as table:
        result = cql.execute(f"DESCRIBE TABLE {table}")
        create_statement = result.one().create_statement
        assert "storage_engine = 'logstor'" not in create_statement

# Test SELECT * FROM MUTATION_FRAGMENTS() on a logstor table. Freshly written
# keys live only in the on-disk log, so each partition should consist of a
# partition start, a single clustering row and a partition end, all coming
# from a single "logstor-log:" mutation source naming the log file, segment
# and offset holding the record.
def test_logstor_mutation_fragments(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        for i in range(3):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i*10})")

        by_pk = {}
        for row in cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table})"):
            by_pk.setdefault(row.pk, []).append(row)
        assert set(by_pk.keys()) == {0, 1, 2}

        for pk, frags in by_pk.items():
            kinds = [f.mutation_fragment_kind for f in frags]
            assert kinds == ['partition start', 'clustering row', 'partition end']
            # Nothing was read yet, so the data is not cached and all fragments
            # come from a single on-disk log record.
            assert len({f.mutation_source for f in frags}) == 1
            assert frags[0].mutation_source.startswith('logstor-log:')
            # A freshly written partition has no tombstone.
            assert json.loads(frags[0].metadata)['tombstone'] == {}
            assert json.loads(frags[1].value) == {'v': str(pk*10)}

        # Single-partition query returns only that partition's fragments.
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        assert [r.pk for r in rows] == [1, 1, 1]
        assert [r.mutation_fragment_kind for r in rows] == ['partition start', 'clustering row', 'partition end']

# Test that after a key is read (and thus cached), MUTATION_FRAGMENTS() shows
# it in both the logstor cache and the on-disk log, with the same value.
def test_logstor_mutation_fragments_cache(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, 200)")

        # Read pk=1 to populate the logstor cache with it.
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100

        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        sources = {r.mutation_source for r in rows}
        assert 'logstor-cache' in sources
        log_sources = {s for s in sources if s.startswith('logstor-log:')}
        assert len(log_sources) == 1
        # Both sources hold the same row value.
        for r in rows:
            if r.mutation_fragment_kind == 'clustering row':
                assert json.loads(r.value) == {'v': '100'}
        assert len([r for r in rows if r.mutation_fragment_kind == 'clustering row']) == 2

        # The unread pk=2 is not cached, so only the log source shows up.
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 2"))
        assert all(r.mutation_source.startswith('logstor-log:') for r in rows)

        # Restricting the mutation_source clustering column works.
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1 AND mutation_source = 'logstor-cache'"))
        assert len(rows) == 3
        assert all(r.mutation_source == 'logstor-cache' for r in rows)

# Test that a deleted key shows up in MUTATION_FRAGMENTS() as a partition
# tombstone (with no rows), as logstor stores deletions as tombstone records.
def test_logstor_mutation_fragments_tombstone(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        cql.execute(f"DELETE FROM {table} WHERE pk = 1")

        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        kinds = [r.mutation_fragment_kind for r in rows]
        assert kinds == ['partition start', 'partition end']
        assert rows[0].mutation_source.startswith('logstor-log:')
        tombstone = json.loads(rows[0].metadata)['tombstone']
        assert tombstone != {}
        assert 'timestamp' in tombstone

# Test that overwriting a key leaves a single log record visible in
# MUTATION_FRAGMENTS(), holding the latest value (logstor replaces the old
# record on overwrite rather than accumulating versions).
def test_logstor_mutation_fragments_overwrite(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 200)")

        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        assert len({r.mutation_source for r in rows}) == 1
        row_frags = [r for r in rows if r.mutation_fragment_kind == 'clustering row']
        assert len(row_frags) == 1
        assert json.loads(row_frags[0].value) == {'v': '200'}

# Test large text values to verify segment switching in logstor.
def test_logstor_large_values(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v text",
                        " WITH storage_engine = 'logstor'") as table:
        # Create large values approximately 100KB
        large_value = 'x' * (100 * 1024)

        # Insert multiple large values
        for i in range(4):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, '{i}-{large_value}')")

        # Verify all values are correctly stored and retrieved
        for i in range(4):
            row = cql.execute(f"SELECT pk, v FROM {table} WHERE pk = {i}").one()
            assert row.pk == i
            assert row.v == f"{i}-{large_value}"
