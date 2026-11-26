# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the logstor storage engine. See additional tests, perhaps
# involving multiple nodes, reboots, etc., in test/cluster/test_logstor.py.
#############################################################################

import json

import pytest
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import SimpleStatement, UNSET_VALUE
from .util import new_test_table, new_type, unique_key_int
from . import nodetool

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

# A logstor table with two regular columns, shared by the tests below of
# writes that do not write the entire row. Each test uses its own keys.
@pytest.fixture(scope='module')
def table_logstor2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v1 int, v2 text',
                        " WITH storage_engine = 'logstor'") as table:
        yield table

# Logstor stores a row as one value that a write replaces entirely, so it
# can't support a partial write that modifies some of the columns and keeps
# the others. Instead of silently treating a partial write as a replacement
# of the entire row (deleting the columns the write did not mention), such
# writes are rejected. This test checks that an INSERT that does not name
# every column of the table is rejected, and does not write anything.
def test_logstor_insert_missing_column(cql, table_logstor2):
    p = unique_key_int()
    # Check that an INSERT naming only v1, or only v2, or none of the regular
    # columns, is rejected.
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(f'INSERT INTO {table_logstor2} (pk, v1) VALUES ({p}, 42)')
    with pytest.raises(InvalidRequest, match='logstor.*v1'):
        cql.execute(f"INSERT INTO {table_logstor2} (pk, v2) VALUES ({p}, 'hi')")
    with pytest.raises(InvalidRequest, match='logstor'):
        cql.execute(f'INSERT INTO {table_logstor2} (pk) VALUES ({p})')
    # Check that the rejected INSERTs did not write anything.
    assert cql.execute(f'SELECT * FROM {table_logstor2} WHERE pk = {p}').one() is None
    # Check that the same INSERT is rejected also when the row exists.
    cql.execute(f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 1, 'one')")
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(f'INSERT INTO {table_logstor2} (pk, v1) VALUES ({p}, 42)')
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 1, 'one')]

# A write that should leave a column without a value can still be done by
# an INSERT that sets that column to null. Unlike a column the INSERT does not
# name, a null column is part of the full new value of the row: it deletes the
# column's old value.
def test_logstor_insert_null_column(cql, table_logstor2):
    p = unique_key_int()
    # Check that an INSERT with v2 set to null writes a row without v2.
    cql.execute(f'INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 42, null)')
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, None)]
    # Check that an INSERT with v1 set to null replaces the row, deleting
    # the old v1.
    cql.execute(f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, null, 'hi')")
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, None, 'hi')]
    # Check that an INSERT with all regular columns set to null still writes
    # the row, which has only the key.
    cql.execute(f'INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, null, null)')
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, None, None)]
    # Check that a null given as a bound variable works like a null in the
    # CQL text: it replaces the row, deleting the old v2.
    cql.execute(f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 1, 'one')")
    stmt = cql.prepare(f'INSERT INTO {table_logstor2} (pk, v1, v2) VALUES (?, ?, ?)')
    cql.execute(stmt, [p, 2, None])
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 2, None)]

# When a logstor table has no regular columns at all, an INSERT that names
# just the key does write the entire row, so it should be allowed.
def test_logstor_insert_key_only_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY',
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f'INSERT INTO {table} (pk) VALUES (1)')
        assert list(cql.execute(f'SELECT pk FROM {table} WHERE pk = 1')) == [(1,)]

# A prepared INSERT that binds UNSET_VALUE to a column does not write that
# column, so it is a partial write just like an INSERT that does not name the
# column, and must be rejected too. Because the bound values are only known
# when the statement is executed, this check happens at execution time.
def test_logstor_insert_unset_column(cql, table_logstor2):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table_logstor2} (pk, v1, v2) VALUES (?, ?, ?)')
    # Check that binding UNSET_VALUE to v2 is rejected, and writes nothing.
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(stmt, [p, 42, UNSET_VALUE])
    assert cql.execute(f'SELECT * FROM {table_logstor2} WHERE pk = {p}').one() is None
    # Check that the same prepared statement works when every column is
    # bound.
    cql.execute(stmt, [p, 42, 'hi'])
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, 'hi')]
    # Check that a conditional INSERT with an unset column is rejected too,
    # also when its condition fails - the error doesn't depend on whether
    # the row exists.
    stmt_lwt = cql.prepare(f'INSERT INTO {table_logstor2} (pk, v1, v2) VALUES (?, ?, ?) IF NOT EXISTS')
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(stmt_lwt, [p, 43, UNSET_VALUE])
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, 'hi')]

# INSERT JSON sets every column missing from the JSON object to null, so it
# writes the entire row and should be allowed. But with DEFAULT UNSET, the
# missing columns are not written, so it is a partial write and must be
# rejected - unless the JSON object has every column.
def test_logstor_insert_json(cql, table_logstor2):
    p = unique_key_int()
    # Check that INSERT JSON missing v2 sets v2 to null.
    cql.execute(f"INSERT INTO {table_logstor2} JSON '{{\"pk\": {p}, \"v1\": 42}}'")
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, None)]
    # Check that INSERT JSON ... DEFAULT UNSET missing v2 is rejected, and
    # does not modify the row.
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(f"INSERT INTO {table_logstor2} JSON '{{\"pk\": {p}, \"v1\": 43}}' DEFAULT UNSET")
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, None)]
    # Check that the same INSERT JSON is rejected also in a conditional batch
    # whose condition fails (the row exists) - the error doesn't depend on
    # whether the condition holds.
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(f"BEGIN BATCH INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 0, 'x') IF NOT EXISTS; "
                    f"INSERT INTO {table_logstor2} JSON '{{\"pk\": {p}, \"v1\": 43}}' DEFAULT UNSET; APPLY BATCH")
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 42, None)]
    # Check that INSERT JSON ... DEFAULT UNSET with every column works.
    cql.execute(f"INSERT INTO {table_logstor2} JSON '{{\"pk\": {p}, \"v1\": 44, \"v2\": \"hi\"}}' DEFAULT UNSET")
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 44, 'hi')]

# UPDATE is a partial write by definition: unlike INSERT, it doesn't write
# the CQL row marker, and it modifies only the columns it names. So UPDATE is
# rejected on logstor tables - even if it names every column.
def test_logstor_update(cql, table_logstor2):
    p = unique_key_int()
    # Check that UPDATE is rejected - whether it names one column or every
    # column, and whether the row exists or not - and writes nothing.
    with pytest.raises(InvalidRequest, match='UPDATE.*logstor'):
        cql.execute(f'UPDATE {table_logstor2} SET v1 = 42 WHERE pk = {p}')
    with pytest.raises(InvalidRequest, match='UPDATE.*logstor'):
        cql.execute(f"UPDATE {table_logstor2} SET v1 = 42, v2 = 'hi' WHERE pk = {p}")
    assert cql.execute(f'SELECT * FROM {table_logstor2} WHERE pk = {p}').one() is None
    cql.execute(f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 1, 'one')")
    with pytest.raises(InvalidRequest, match='UPDATE.*logstor'):
        cql.execute(f'UPDATE {table_logstor2} SET v1 = 42 WHERE pk = {p}')
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 1, 'one')]

# Check that the partial writes are rejected also inside a BATCH. Since the
# whole batch is rejected, its other statements are not executed either.
def test_logstor_partial_write_in_batch(cql, table_logstor2):
    p = unique_key_int()
    full_insert = f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 1, 'one')"
    # Check that a batch with an UPDATE is rejected.
    with pytest.raises(InvalidRequest, match='UPDATE.*logstor'):
        cql.execute(f'BEGIN BATCH {full_insert}; UPDATE {table_logstor2} SET v1 = 42 WHERE pk = {p}; APPLY BATCH')
    # Check that a batch with a partial INSERT is rejected.
    with pytest.raises(InvalidRequest, match='logstor.*v2'):
        cql.execute(f'BEGIN BATCH {full_insert}; INSERT INTO {table_logstor2} (pk, v1) VALUES ({p}, 42); APPLY BATCH')
    # Check that a batch deleting a column is rejected.
    with pytest.raises(InvalidRequest, match='logstor'):
        cql.execute(f'BEGIN BATCH {full_insert}; DELETE v1 FROM {table_logstor2} WHERE pk = {p}; APPLY BATCH')
    # Check that the rejected batches wrote nothing, not even the full INSERT.
    assert cql.execute(f'SELECT * FROM {table_logstor2} WHERE pk = {p}').one() is None

# A DELETE that names columns deletes only these columns and keeps the rest
# of the row, so it is a partial write and is rejected. Deleting the entire
# row still works.
def test_logstor_delete_column(cql, table_logstor2):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table_logstor2} (pk, v1, v2) VALUES ({p}, 1, 'one')")
    # Check that deleting one column, or every column, is rejected and does
    # not modify the row.
    with pytest.raises(InvalidRequest, match='logstor'):
        cql.execute(f'DELETE v1 FROM {table_logstor2} WHERE pk = {p}')
    with pytest.raises(InvalidRequest, match='logstor'):
        cql.execute(f'DELETE v1, v2 FROM {table_logstor2} WHERE pk = {p}')
    assert list(cql.execute(f'SELECT pk, v1, v2 FROM {table_logstor2} WHERE pk = {p}')) == [(p, 1, 'one')]
    # Check that deleting the entire row works.
    cql.execute(f'DELETE FROM {table_logstor2} WHERE pk = {p}')
    assert cql.execute(f'SELECT * FROM {table_logstor2} WHERE pk = {p}').one() is None

# Test that clustering columns are not supported in logstor tables.
# Attempting to create a logstor table with clustering columns should fail.
def test_logstor_clustering_columns_disabled(cql, test_keyspace):
    with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with tables that have clustering columns"):
        with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                            " WITH storage_engine = 'logstor'") as table:
            pass

# Test that logstor tables can't be created with counter columns, since
# counters are not supported currently with logstor.
def test_logstor_counter_columns_disabled(cql, test_keyspace):
    with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with counter columns"):
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c counter",
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

# Test that logstor tables can't be created with non-frozen collection
# columns. A non-frozen collection is stored as separate cells which can be
# modified individually, but logstor stores the entire row as one value, and
# only supports writes that replace it entirely. Frozen collections are
# allowed (see test_logstor_frozen_map and
# test_logstor_frozen_collections_and_udt).
def test_logstor_non_frozen_collection_disabled(cql, test_keyspace):
    for coll in ['map<text, text>', 'set<int>', 'list<int>']:
        # Check that a column of each non-frozen collection type is rejected.
        with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with non-frozen"):
            with new_test_table(cql, test_keyspace, f'pk int PRIMARY KEY, v {coll}',
                                " WITH storage_engine = 'logstor'") as table:
                pass

# Like non-frozen collections, non-frozen user-defined types are stored as
# separate cells, so they are rejected in logstor tables. Frozen ones are
# allowed (see test_logstor_frozen_collections_and_udt).
def test_logstor_non_frozen_udt_disabled(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b text)') as typ:
        # Check that a non-frozen UDT column is rejected.
        with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with non-frozen"):
            with new_test_table(cql, test_keyspace, f'pk int PRIMARY KEY, v {typ}',
                                " WITH storage_engine = 'logstor'") as table:
                pass

# While non-frozen collections and UDTs are rejected in logstor tables,
# frozen ones are stored as a single value, so they are allowed. This test
# checks frozen sets, lists and UDTs (test_logstor_frozen_map checks frozen
# maps).
def test_logstor_frozen_collections_and_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b text)') as typ:
        with new_test_table(cql, test_keyspace, f'pk int PRIMARY KEY, s frozen<set<int>>, l frozen<list<int>>, u frozen<{typ}>',
                            " WITH storage_engine = 'logstor'") as table:
            # Check that a row with frozen set, list and UDT values can be
            # written and read back.
            cql.execute(f"INSERT INTO {table} (pk, s, l, u) VALUES (1, {{1, 2}}, [3, 3, 4], {{a: 5, b: 'five'}})")
            assert list(cql.execute(f'SELECT pk, s, l, u FROM {table} WHERE pk = 1')) == [(1, {1, 2}, [3, 3, 4], (5, 'five'))]
            # Check that overwriting the row replaces each value entirely.
            cql.execute(f"INSERT INTO {table} (pk, s, l, u) VALUES (1, {{6}}, [7], {{a: 8, b: 'eight'}})")
            assert list(cql.execute(f'SELECT pk, s, l, u FROM {table} WHERE pk = 1')) == [(1, {6}, [7], (8, 'eight'))]

# Test that a non-frozen collection or UDT can't be added to an existing
# logstor table with ALTER TABLE ADD either, but a frozen one can.
def test_logstor_alter_add_non_frozen(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b text)') as typ:
        with new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int',
                            " WITH storage_engine = 'logstor'") as table:
            # Check that adding a non-frozen collection or UDT is rejected.
            for t in ['map<text, text>', 'set<int>', 'list<int>', typ]:
                with pytest.raises(ConfigurationException, match="The 'logstor' storage engine cannot be used with non-frozen"):
                    cql.execute(f'ALTER TABLE {table} ADD c {t}')
            # Check that adding a frozen collection works, and the new
            # column can be written and read.
            cql.execute(f'ALTER TABLE {table} ADD c frozen<set<int>>')
            cql.execute(f'INSERT INTO {table} (pk, v, c) VALUES (1, 2, {{3, 4}})')
            assert list(cql.execute(f'SELECT pk, v, c FROM {table} WHERE pk = 1')) == [(1, 2, {3, 4})]

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
# keys live only in the log, so each partition should consist of a partition
# start, a single clustering row and a partition end, all coming from a single
# "logstor-log:" mutation source naming the log file and segment holding the
# record.
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
            # come from a single log record.
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
        assert len(rows) == 3
        assert all(r.mutation_source.startswith('logstor-log:') for r in rows)

        # Restricting the mutation_source clustering column works.
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1 AND mutation_source = 'logstor-cache'"))
        assert len(rows) == 3
        assert all(r.mutation_source == 'logstor-cache' for r in rows)

# Test that a deleted key shows up in MUTATION_FRAGMENTS() as a partition
# tombstone (with no rows), as logstor stores deletions as tombstone records.
def test_logstor_mutation_fragments_tombstone(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'"
                        " AND tombstone_gc = {'mode': 'disabled'}") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100) USING TIMESTAMP 1000")
        cql.execute(f"DELETE FROM {table} USING TIMESTAMP 2000 WHERE pk = 1")

        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        kinds = [r.mutation_fragment_kind for r in rows]
        assert kinds == ['partition start', 'partition end']
        assert rows[0].mutation_source.startswith('logstor-log:')
        tombstone = json.loads(rows[0].metadata)['tombstone']
        assert tombstone['timestamp'] == 2000

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

# Test that MUTATION_FRAGMENTS() reports nothing at all - not even an empty
# partition - for a key which is not in the index, and likewise when the query
# is restricted to a source the key does not have.
def test_logstor_mutation_fragments_no_source(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")

        assert list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 2")) == []
        # pk=1 was never read, so it is not in the logstor cache.
        assert list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1 AND mutation_source = 'logstor-cache'")) == []

# Test that dumping a key does not populate the logstor cache: the dump reads
# the log record with the cache bypassed, so an uncached key stays uncached no
# matter how often it is dumped.
def test_logstor_mutation_fragments_do_not_populate_cache(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")

        for _ in range(2):
            rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
            assert len(rows) == 3
            assert all(r.mutation_source.startswith('logstor-log:') for r in rows)

# Test that a logstor table with caching disabled never reports a cache source,
# not even for a key which was read.
def test_logstor_mutation_fragments_caching_disabled(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'"
                        " AND caching = {'enabled': 'false'}") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100

        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"))
        assert len(rows) == 3
        assert all(r.mutation_source.startswith('logstor-log:') for r in rows)

def is_cached(cql, table, pk):
    return bool(list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = {pk} AND mutation_source = 'logstor-cache'")))

# Test that ALTER TABLE ... WITH caching = {'enabled': 'false'} takes effect at once.
def test_logstor_alter_caching_disable(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"ALTER TABLE {table} WITH caching = {{'enabled': 'false'}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100
        assert not is_cached(cql, table, 1)

# Test that ALTER TABLE ... WITH caching = {'enabled': 'true'} takes effect at once.
def test_logstor_alter_caching_enable(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'"
                        " AND caching = {'enabled': 'false'}") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100
        assert not is_cached(cql, table, 1)
        cql.execute(f"ALTER TABLE {table} WITH caching = {{'enabled': 'true'}}")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100
        assert is_cached(cql, table, 1)

# A write while caching is disabled must evict a partition cached before, so that a read after
# caching is enabled again does not return the old value.
def test_logstor_alter_caching_overwrite_while_disabled(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100
        assert is_cached(cql, table, 1)
        cql.execute(f"ALTER TABLE {table} WITH caching = {{'enabled': 'false'}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 200)")
        assert not is_cached(cql, table, 1)
        cql.execute(f"ALTER TABLE {table} WITH caching = {{'enabled': 'true'}}")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 200

# Test that an ALTER of another option keeps the cached partitions.
def test_logstor_alter_other_option_keeps_cache(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_engine = 'logstor'") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 100)")
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100
        assert is_cached(cql, table, 1)
        cql.execute(f"ALTER TABLE {table} WITH comment = 'new comment'")
        assert is_cached(cql, table, 1)
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 100

# Test that the log source name identifies the segment holding the record, and
# is therefore shared by all the partitions stored in that segment - the way an
# sstable source is shared by the partitions of one sstable. A dump can be
# restricted to a single source, or to the log sources of all the partitions.
def test_logstor_mutation_fragments_source_restriction(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v text",
                        " WITH storage_engine = 'logstor'") as table:
        # 50KB values, 8 of them: bigger than a whole 128KB segment (the
        # default segment size), so they are spread across several segments,
        # and therefore several log sources.
        large_value = 'x' * (50 * 1024)
        for i in range(8):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, '{i}-{large_value}')")

        # A write returns before the separator moves its record out of the
        # shared active segment, and the record's source name changes when it
        # moves. Flush the table, which flushes its separator buffers, so that no
        # record moves between the dump and the restricted queries below.
        nodetool.flush(cql, table)

        # Group the partitions by the log source holding them.
        pks_by_source = {}
        for row in cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table})"):
            assert row.mutation_source.startswith('logstor-log:')
            pks_by_source.setdefault(row.mutation_source, set()).add(row.pk)
        assert set().union(*pks_by_source.values()) == set(range(8))
        assert len(pks_by_source) > 1

        # Restricting the query to one source returns exactly the partitions
        # stored in it, with all their fragments.
        for source, pks in pks_by_source.items():
            rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE mutation_source = '{source}' ALLOW FILTERING"))
            assert {r.pk for r in rows} == pks
            assert len(rows) == 3 * len(pks)

        # A range over the source kind returns all the log sources.
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({table})"
                                f" WHERE mutation_source > 'logstor-log' AND mutation_source < 'logstor-log;' ALLOW FILTERING"))
        assert len(rows) == 24

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
