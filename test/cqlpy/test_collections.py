# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for non-frozen collection columns: sets, maps and lists written,
# updated and deleted element by element or as a whole. Many more such tests,
# translated from Cassandra's unit tests, are in
# cassandra_tests/validation/entities/collections_test.py; frozen
# collections are tested in test_frozen_collection.py.
#
# The tests here were ported from scylla-dtest's cql_additional_tests.py.
# Like the dtests, they run in a keyspace with vnodes, and keep the dtests'
# table names.
#############################################################################

import time
from uuid import UUID

import pytest
from cassandra.protocol import InvalidRequest

from .nodetool import del_snapshot, flush_all, list_snapshots
from .util import config_value_context, new_named_table, unique_name


# Elements added to and removed from a set, a set overwritten as a whole,
# one element deleted, and the whole set deleted, which leaves no row (the
# row was only ever written by UPDATE).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_set_operations(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "user", "fn text, ln text, tags set<text>, PRIMARY KEY (fn, ln)") as table:
        update = f"UPDATE {table} SET {{}} WHERE fn='Tom' AND ln='Bombadil'"
        for op in ["tags = tags + { 'foo' }", "tags = tags + { 'bar' }", "tags = tags + { 'foo' }", "tags = tags + { 'foobar' }", "tags = tags - { 'bar' }"]:
            cql.execute(update.format(op))
        assert list(cql.execute(f"SELECT tags FROM {table}")) == [({"foo", "foobar"},)]
        update = f"UPDATE {table} SET {{}} WHERE fn='Bilbo' AND ln='Baggins'"
        select = f"SELECT tags FROM {table} WHERE fn='Bilbo' AND ln='Baggins'"
        cql.execute(update.format("tags = { 'a', 'c', 'b' }"))
        assert list(cql.execute(select)) == [({"a", "b", "c"},)]
        # The overwrite deletes the old set with a tombstone just before its
        # own timestamp; keep the two writes' timestamps apart, as the dtest.
        time.sleep(0.01)
        cql.execute(update.format("tags = { 'm', 'n' }"))
        assert list(cql.execute(select)) == [({"m", "n"},)]
        cql.execute(f"DELETE tags['m'] FROM {table} WHERE fn='Bilbo' AND ln='Baggins'")
        assert list(cql.execute(select)) == [({"n"},)]
        cql.execute(f"DELETE tags FROM {table} WHERE fn='Bilbo' AND ln='Baggins'")
        assert list(cql.execute(select)) == []

# Map elements set, overwritten and deleted by key, a map overwritten as a
# whole, and set to an empty map, which leaves no row.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_map_operations(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "user", "fn text, ln text, m map<text, int>, PRIMARY KEY (fn, ln)") as table:
        update = f"UPDATE {table} SET {{}} WHERE fn='Tom' AND ln='Bombadil'"
        for op in ["m['foo'] = 3", "m['bar'] = 4", "m['woot'] = 5", "m['bar'] = 6"]:
            cql.execute(update.format(op))
        cql.execute(f"DELETE m['foo'] FROM {table} WHERE fn='Tom' AND ln='Bombadil'")
        assert list(cql.execute(f"SELECT m FROM {table}")) == [({"woot": 5, "bar": 6},)]
        update = f"UPDATE {table} SET {{}} WHERE fn='Bilbo' AND ln='Baggins'"
        select = f"SELECT m FROM {table} WHERE fn='Bilbo' AND ln='Baggins'"
        cql.execute(update.format("m = { 'a' : 4 , 'c' : 3, 'b' : 2 }"))
        assert list(cql.execute(select)) == [({"a": 4, "b": 2, "c": 3},)]
        # See test_set_operations.
        time.sleep(0.01)
        cql.execute(update.format("m = { 'm' : 4 , 'n' : 1, 'o' : 2 }"))
        assert list(cql.execute(select)) == [({"m": 4, "n": 1, "o": 2},)]
        cql.execute(update.format("m = {}"))
        assert list(cql.execute(select)) == []

# List elements appended, prepended, set and deleted by index, and removed
# by value.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_list_operations(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "user", "fn text, ln text, tags list<text>, PRIMARY KEY (fn, ln)") as table:
        update = f"UPDATE {table} SET {{}} WHERE fn='Tom' AND ln='Bombadil'"
        for op in ["tags = tags + [ 'foo' ]", "tags = tags + [ 'bar' ]", "tags = tags + [ 'foo' ]", "tags = tags + [ 'foobar' ]"]:
            cql.execute(update.format(op))
        assert list(cql.execute(f"SELECT tags FROM {table}")) == [(["foo", "bar", "foo", "foobar"],)]
        update = f"UPDATE {table} SET {{}} WHERE fn='Bilbo' AND ln='Baggins'"
        select = f"SELECT tags FROM {table} WHERE fn='Bilbo' AND ln='Baggins'"
        cql.execute(update.format("tags = [ 'a', 'c', 'b', 'c' ]"))
        assert list(cql.execute(select)) == [(["a", "c", "b", "c"],)]
        cql.execute(update.format("tags = [ 'm', 'n' ] + tags"))
        assert list(cql.execute(select)) == [(["m", "n", "a", "c", "b", "c"],)]
        cql.execute(update.format("tags[2] = 'foo', tags[4] = 'bar'"))
        assert list(cql.execute(select)) == [(["m", "n", "foo", "c", "bar", "c"],)]
        cql.execute(f"DELETE tags[2] FROM {table} WHERE fn='Bilbo' AND ln='Baggins'")
        assert list(cql.execute(select)) == [(["m", "n", "c", "bar", "c"],)]
        cql.execute(update.format("tags = tags - [ 'bar' ]"))
        assert list(cql.execute(select)) == [(["m", "n", "c", "c"],)]

# Removing elements by value from, and setting an element by index of, a
# static list and a regular list in the same partition: the read of the
# current value these need returns the right list (#903).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_static_and_regular_list_operations(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "user", "fn text, ln text, static_tags list<text> static, tags list<text>, PRIMARY KEY (fn, ln)") as table:
        cql.execute(f"UPDATE {table} SET tags = tags + [ 'a', 'b', 'c', 'b' ] WHERE fn='Tom' AND ln='Bombadil'")
        cql.execute(f"update {table} set static_tags = static_tags + [ 'a', 'b', 'c', 'b' ] where fn='Tom'")
        cql.execute(f"UPDATE {table} SET tags = tags - [ 'b' ] WHERE fn='Tom' AND ln='Bombadil'")
        assert list(cql.execute(f"SELECT tags FROM {table} WHERE fn='Tom' AND ln='Bombadil'")) == [(["a", "c"],)]
        select_static = f"select static_tags from {table} where fn='Tom'"
        assert list(cql.execute(select_static)) == [(["a", "b", "c", "b"],)]
        cql.execute(f"update {table} set static_tags = static_tags - [ 'b' ] where fn='Tom'")
        assert list(cql.execute(select_static)) == [(["a", "c"],)]
        cql.execute(f"update {table} set static_tags[1] = 'b' where fn='Tom'")
        assert list(cql.execute(select_static)) == [(["a", "b"],)]

# A list, a map and a set in one row, each set and then extended. The list
# updates are prepared, so that both go to the same shard and are applied
# in order (#6846).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_multiple_collections_in_row(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "foo", "k uuid PRIMARY KEY, L list<int>, M map<text, int>, S set<int>") as table:
        k = UUID("b017f48f-ae67-11e1-9096-005056c00008")
        cql.execute(cql.prepare(f"UPDATE {table} SET L = [1, 3, 5] WHERE k = ?"), [k])
        cql.execute(cql.prepare(f"UPDATE {table} SET L = L + [7, 11, 13] WHERE k =?"), [k])
        cql.execute(f"UPDATE {table} SET S = {{1, 3, 5}} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cql.execute(f"UPDATE {table} SET S = S + {{7, 11, 13}} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cql.execute(f"UPDATE {table} SET M = {{'foo': 1, 'bar' : 3}} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cql.execute(f"UPDATE {table} SET M = M + {{'foobar' : 4}} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        [row] = list(cql.execute(f"SELECT L, M, S FROM {table} WHERE k = b017f48f-ae67-11e1-9096-005056c00008"))
        assert row.l == [1, 3, 5, 7, 11, 13]
        assert list(row.m.items()) == [("bar", 3), ("foo", 1), ("foobar", 4)]
        assert list(row.s) == [1, 3, 5, 7, 11, 13]

# One UPDATE setting a list element by index and a regular column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_list_element_and_regular_column_update(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, l list<int>, c int") as table:
        cql.execute(f"INSERT INTO {table}(k, l, c) VALUES(3, [0, 1, 2], 4)")
        cql.execute(f"UPDATE {table} SET l[0] = 1, c = 42 WHERE k = 3")
        assert list(cql.execute(f"SELECT l, c FROM {table} WHERE k = 3")) == [([1, 1, 2], 42)]

# Columns, also a collection, can be added to a table with a collection
# column (CASSANDRA-4982).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_alter_table_with_collections(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "collections", "key int PRIMARY KEY, aset set<text>") as table:
        cql.execute(f"ALTER TABLE {table} ADD c text")
        cql.execute(f"ALTER TABLE {table} ADD alist list<text>")
        assert list(cql.execute(f"SELECT key, aset, c, alist FROM {table}")) == []

# ttl() and writetime() of a non-frozen collection are rejected.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_ttl_and_writetime_of_collection_rejected(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, l set<int>") as table:
        for func in ["ttl", "writetime"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"SELECT {func}(l) FROM {table} WHERE k = 0")

# A set overwritten by an INSERT in a newer sstable than the original set:
# after node-wide flushes only the new set is read (CASSANDRA-5805).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_set_overwrite_across_sstables(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, s set<int>") as table:
        cql.execute(f"INSERT INTO {table}(k, s) VALUES (1, {{1}})")
        flush_all(cql)
        cql.execute(f"INSERT INTO {table}(k, s) VALUES (1, {{2}})")
        flush_all(cql)
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, {2})]

# A dropped collection column cannot be re-added with a different type,
# after its data was flushed (CASSANDRA-6276).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_readd_dropped_collection_other_type(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int primary key, v set<text>, x int") as table:
        cql.execute(f"insert into {table} (k, v) VALUES (0, {{'fffffffff'}})")
        flush_all(cql)
        cql.execute(f"alter table {table} drop v")
        with pytest.raises(InvalidRequest):
            cql.execute(f"alter table {table} add v set<int>")

# now() can be an element of a list literal (CASSANDRA-5795).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_now_in_list_literal(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, v list<timeuuid>") as table:
        cql.execute(f"INSERT INTO {table}(k, v) VALUES (0, [now()])")
        assert len(list(cql.execute(f"SELECT v FROM {table} WHERE k = 0")).pop().v) == 1

# Lists, maps and sets written by INSERT literals read back over protocol
# v4, in a keyspace that is then dropped with DROP KEYSPACE IF EXISTS.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_collections_insert_and_scan(cql, this_dc, scylla_only):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND TABLETS = {{'enabled': false}}")
    try:
        cql.execute(f"CREATE TABLE {ks}.cql2ct ( a int PRIMARY KEY, b list<int>, c map<int, int>, d set<int> );")
        for i in range(4):
            cql.execute(f"INSERT INTO {ks}.cql2ct (a, b, c, d) values ({i}, [{i},{i + 1}], {{{i}:{i + 1}}}, {{{i},{i + 1}}});")
        res = sorted(cql.execute(f"SELECT * FROM {ks}.cql2ct"))
        assert [tuple(r) for r in res] == [(i, [i, i + 1], {i: i + 1}, {i, i + 1}) for i in range(4)]
    finally:
        # Like on the dtest's node, auto_snapshot is on, so DROP KEYSPACE
        # snapshots the table; remove the snapshot.
        try:
            with config_value_context(cql, 'auto_snapshot', 'true'):
                cql.execute(f"DROP KEYSPACE IF EXISTS {ks}")
        finally:
            for snapshot in list_snapshots(cql):
                if any(s['ks'] == ks for s in snapshot['value']):
                    del_snapshot(cql, snapshot['key'], [ks])
