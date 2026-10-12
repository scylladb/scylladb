# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for various operations on COUNTER columns.
# See also tests for casting involving counter columns in test_cast_data.py
###############################################################################

import pytest
from .util import new_test_table, unique_key_int
from cassandra.protocol import ConfigurationException, InvalidRequest, SyntaxException
from .rest_api import scylla_inject_error

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, i bigint, v int") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        yield table

# Test that the function counterasblob() exists and works as expected -
# same as bigintasblob on the same number (a counter is a 64-bit number).
# Reproduces #14742
def test_counter_to_blob(cql, table1, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET i = 1000 WHERE p = {p}')
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    expected = b'\x00\x00\x00\x00\x00\x00\x03\xe8'
    assert [(expected,)] == list(cql.execute(f"SELECT bigintasblob(i) FROM {table1} WHERE p={p}"))
    assert [(expected,)] == list(cql.execute(f"SELECT counterasblob(c) FROM {table2} WHERE p={p}"))
    # Although the representation of the counter and bigint types are the
    # same (64-bit), you can't use the wrong "*asblob()" function:
    with pytest.raises(InvalidRequest, match='counterasblob'):
        cql.execute(f"SELECT counterasblob(i) FROM {table1} WHERE p={p}")
    # The opposite order is allowed in Scylla because of #14319, so let's
    # split it into a second test test_counter_to_blob2:
@pytest.mark.xfail(reason="issue #14319")
def test_counter_to_blob2(cql, table1, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    # Reproduces #14319:
    with pytest.raises(InvalidRequest, match='bigintasblob'):
        cql.execute(f"SELECT bigintasblob(c) FROM {table2} WHERE p={p}")

# Test that the function blobascounter() exists and works as expected.
# Reproduces #14742
def test_counter_from_blob(cql, table1):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET i = 1000 WHERE p = {p}')
    assert [(1000,)] == list(cql.execute(f"SELECT blobascounter(bigintasblob(i)) FROM {table1} WHERE p={p}"))

# blobascounter() must insist to receive a properly-sized (8-byte) blob.
# If it accepts a shorter blob (e.g., 4 bytes) and returns that to the driver,
# it will confuse the driver (the driver will expect to read 8 bytes for the
# bigint but will get only 4).
# We have test_native_functions.py::test_blobas_wrong_size() that verifies
# that this protection works for the bigint type, but it turns out it also
# needs to be separately enforced for the counter type.
def test_blobascounter_wrong_size(cql, table1):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET v = 1000 WHERE p = {p}')
    with pytest.raises(InvalidRequest, match='blobascounter'):
        cql.execute(f"SELECT blobascounter(intasblob(v)) FROM {table1} WHERE p={p}")

# Drop a table while there is a counter update operation in progress.
# Verify the table waits for the operation to complete before it's destroyed.
# Reproduces scylladb/scylla-enterprise#4475
def test_counter_update_while_table_dropped(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table, \
         scylla_inject_error(cql, "apply_counter_update_delay_5s", one_shot=True):
        cql.execute_async(f'UPDATE {table} SET c = c + 1 WHERE p = 0')

# A counter can go negative and back: a sequence of increments and
# decrements, including ones written without spaces ("total+1").
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_counter_negative_values(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c text, total counter, PRIMARY KEY (p, c)") as table:
        for update, expected in [("total + 1", 1), ("total - 4", -3), ("total+1", -2), ("total -2", -4)]:
            cql.execute(f"UPDATE {table} SET total = {update} WHERE p = 1 AND c = 'a'")
            assert list(cql.execute(f"SELECT total FROM {table} WHERE p = 1 AND c = 'a'")) == [(expected,)]

# Ported from scylla-dtest counter_tests.py::TestCounters::test_int_rollover.
# A counter is a 64-bit integer, and incrementing it past the maximum value
# wraps around to the minimum value (and decrementing goes back), instead of
# failing or saturating. This is documented behavior, see
# https://github.com/scylladb/scylladb/issues/2225 (closed as WONTFIX).
# As in the dtest, the table is in a vnodes keyspace with RF=1 and is read
# with full scans after each update.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_int_rollover(cql, test_keyspace_vnodes):
    int64_max = 2**63 - 1
    int64_min = -2**63
    with new_test_table(cql, test_keyspace_vnodes, "t int, c counter, primary key(t)") as table:
        cql.execute(f"UPDATE {table} SET c = c + {int64_max} where t = 0")
        assert list(cql.execute(f"SELECT * from {table}")) == [(0, int64_max)], "Failed to update counter to MAX_INT"
        cql.execute(f"UPDATE {table} SET c = c + 1 where t = 0")
        assert list(cql.execute(f"SELECT * from {table}")) == [(0, int64_min)], "Int counter isn't rollover"
        cql.execute(f"UPDATE {table} SET c = c - 1 where t = 0")
        assert list(cql.execute(f"SELECT * from {table}")) == [(0, int64_max)], "Int counter isn't recovered"

# In a COMPACT STORAGE table an empty clustering key is not allowed, and this
# must also be enforced on the counter-update write path, not only for INSERT
# (see test_empty.py::test_insert_empty_string_key_compact). Non-empty keys
# must keep working.
def test_counter_update_empty_clustering_key_compact(compact_storage, cql, test_keyspace):
    schema = 'pk int, ck text, v counter, PRIMARY KEY (pk, ck)'
    with new_test_table(cql, test_keyspace, schema, 'WITH COMPACT STORAGE') as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"UPDATE {table} SET v = v + 1 WHERE pk = 0 AND ck = ''")
        with pytest.raises(InvalidRequest):
            cql.execute(f"UPDATE {table} SET v = v - 1 WHERE pk = 0 AND ck = ''")
        cql.execute(f"UPDATE {table} SET v = v + 5 WHERE pk = 0 AND ck = 'ck'")
        cql.execute(f"UPDATE {table} SET v = v - 2 WHERE pk = 0 AND ck = 'ck'")
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table}")) == [(0, 'ck', 3)]

# A dropped counter column cannot be re-added, because its old counter shards
# could resurface and corrupt the new counter (CASSANDRA-7831).
def test_counter_column_cannot_be_readded(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter, c2 counter") as table:
        cql.execute(f"UPDATE {table} SET c = c + 1, c2 = c2 + 1 WHERE p = 1")
        assert list(cql.execute(f"SELECT p, c FROM {table}")) == [(1, 1)]
        cql.execute(f"ALTER TABLE {table} DROP c")
        with pytest.raises(InvalidRequest, match='re-add previously dropped counter column c'):
            cql.execute(f"ALTER TABLE {table} ADD c counter")

# Counter updates cannot carry a TTL, neither can a counter table have a
# default_time_to_live. USING TIMESTAMP on counter updates is checked in
# cassandra_tests/validation/entities/timestamp_test.py.
def test_counter_update_with_ttl(cql, table2):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='TTL'):
        cql.execute(f"UPDATE {table2} USING TTL 5 SET c = c + 1 WHERE p = {p}")
    with pytest.raises(InvalidRequest, match='default_time_to_live'):
        cql.execute(f"ALTER TABLE {table2} WITH default_time_to_live = 100")

# Prepared counter updates with large positive and negative deltas, a second
# round of deltas adding up correctly, and deleting the counters through CQL
# (both a single counter cell and a whole row) removing the rows.
def test_counter_prepared_large_deltas_and_delete(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        update = cql.prepare(f"UPDATE {table} SET c = c + ? WHERE p = ?")
        # Both rounds together stay within the 64-bit range.
        first = [2**61, -2**61, 2**62 - 1, -2**62, 12345]
        second = [2**60, 2**61, -2**62 + 1, 2**62, -12345]
        for p, d in enumerate(first):
            cql.execute(update, [d, p])
        assert sorted(cql.execute(f"SELECT p, c FROM {table}")) == list(enumerate(first))
        for p, d in enumerate(second):
            cql.execute(update, [d, p])
        expected = [(p, a + b) for p, (a, b) in enumerate(zip(first, second))]
        assert sorted(cql.execute(f"SELECT p, c FROM {table}")) == expected
        for p in range(3):
            cql.execute(f"DELETE c FROM {table} WHERE p = {p}")
        for p in range(3, len(first)):
            cql.execute(f"DELETE FROM {table} WHERE p = {p}")
        assert list(cql.execute(f"SELECT * FROM {table}")) == []

# A static counter column is shared by all rows of a partition: it can be
# incremented together with a regular counter of one row, or on its own
# through the partition key alone, and every row shows the same value.
def test_static_counter_column(cql, test_keyspace):
    schema = "pk int, ck int, s counter static, v counter, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        pk = unique_key_int()
        n = 5
        for ck in range(n):
            cql.execute(f"UPDATE {table} SET s = s + 1, v = v + {ck + 1} WHERE pk = {pk} AND ck = {ck}")
        assert list(cql.execute(f"SELECT ck, s, v FROM {table} WHERE pk = {pk}")) == [(ck, n, ck + 1) for ck in range(n)]
        for i in range(4):
            if i % 2:
                cql.execute(f"UPDATE {table} SET s = s + 10 WHERE pk = {pk}")
            else:
                cql.execute(f"UPDATE {table} SET s = s + 10, v = v + 0 WHERE pk = {pk} AND ck = {i}")
        assert list(cql.execute(f"SELECT ck, s, v FROM {table} WHERE pk = {pk}")) == [(ck, n + 40, ck + 1) for ck in range(n)]


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes.

# A table cannot mix a counter column with a non-counter set column
# (CASSANDRA-4706).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_counter_with_set_column_rejected(cql, test_keyspace_vnodes):
    with pytest.raises(ConfigurationException, match=r"Cannot add a( non)? counter column"):
        cql.execute(f"CREATE TABLE {test_keyspace_vnodes}.test (id bigint PRIMARY KEY, count counter, things set<text>)")

# Collections of counters are rejected.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_collection_of_counters_rejected(cql, test_keyspace_vnodes):
    for column in ["l list<counter>", "s set<counter>", "m map<text, counter>"]:
        with pytest.raises((InvalidRequest, SyntaxException)):
            cql.execute(f"CREATE TABLE {test_keyspace_vnodes}.test (k int PRIMARY KEY, {column})")


# Adding a counter column to a table without counters fails with a
# configuration error, and does not crash the server (#7065). Ported from
# dtest counter_tests.py, on a vnodes keyspace like the dtest.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_alter_non_counter_with_counter(cql, test_keyspace_vnodes):
    with new_test_table(cql, test_keyspace_vnodes, 'a text, b text, PRIMARY KEY (a, b)', 'WITH CLUSTERING ORDER BY (b ASC)') as table:
        with pytest.raises(ConfigurationException):
            # The statement as the dtest wrote it, with the semicolon.
            cql.execute(f'ALTER TABLE {table} ADD "c" counter;')
        cql.execute(f"INSERT INTO {table} (a, b) VALUES ('a', 'b')")
        assert list(cql.execute(f'SELECT * FROM {table}')) == [('a', 'b')]
