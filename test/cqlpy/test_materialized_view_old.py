# Copyright 2016-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# These file contains the original functional tests written for materialized
# views, originally written in C++ but translated (see issue #16134) to Python.
# The tests originally in view_schema_test.cc, view_complex_test.cc,
# view_schema_ckey_test.cc and view_schema_pkey_test.cc were moved here.

# The original C++ tests used to wrap reads of a view in an "eventually()"
# retry loop, because in general a materialized view is updated
# asynchronously - some time after the base-table write is acknowledged.
# The tests in this file do *not* need such retries: cqlpy runs against a
# single node, and there both Scylla and Cassandra apply the view update
# synchronously, as part of the base write. In Cassandra this is because
# StorageProxy::mutateMV() has a special case applying the view mutation
# inline when the paired view replica is the local node, instead of going
# through the batchlog and the asynchronous view-mutation stage.
#
# Note that this only holds as long as the base row and its paired view row
# live on the same node. So if we ever want to run these tests against a
# multi-node cluster, we shouldn't add retry loops here - we should instead
# make sure the tests use a keyspace whose data is all replicated on just
# one node (e.g., NetworkTopologyStrategy with RF=1 in a data center that
# has a single node).
#
# What *is* asynchronous even on a single node is the *view build* - the
# backfill of a view created on a table that already has data. Tests which
# need this must wait for the build to complete, and can't just read the
# view immediately after creating it.

import contextlib
import datetime
import pytest
import time
from decimal import Decimal
from uuid import UUID
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.util import Time

from . import nodetool
from .util import new_test_table, new_type, new_materialized_view, unique_name, is_scylla

# CQL usually folds identifier names - keyspace, table and column names -
# to lowercase. That is, unless the identifier is enclosed in double
# quotation marks (") then the identifier becomes case sensitive.
# Let's test that case-sensitive (quoted) column names can be used for
# materialized views. Test that data can be inserted and queried, and
# that case sensitive columns in views can be renamed.
# This test reproduces issues #3388 and #3391.
def test_case_sensitivity(cql, test_keyspace):
    schema = '"theKey" int, "theClustering" int, "theValue" int, primary key ("theKey", "theClustering")'
    with new_test_table(cql, test_keyspace, schema) as table:
        where = '"theKey" is not null and "theClustering" is not null'
        pk = '"theKey", "theClustering"'
        with new_materialized_view(cql, table, '*', pk, where) as mv1, \
             new_materialized_view(cql, table, '"theKey", "theClustering", "theValue"', pk, where) as mv2:
            cql.execute(f'insert into {table} ("theKey", "theClustering", "theValue") values (0, 0, 0)')
            for mv in [mv1, mv2]:
                assert [(0, 0, 0)] == list(cql.execute(f'select "theKey", "theClustering", "theValue" from {mv}'))
            cql.execute(f'alter table {table} rename "theClustering" to "Col"')
            for mv in [mv1, mv2]:
                assert [(0, 0, 0)] == list(cql.execute(f'select "theKey", "Col", "theValue" from {mv}'))

# A materialized view is read-only, and its schema is not its own: it can't
# be written to directly, and it can't be modified with ALTER TABLE (only
# with ALTER MATERIALIZED VIEW). Instead, the view's schema follows that of
# its base table - a column added to or renamed in the base table is added
# to or renamed in the view as well.
def test_access_and_schema(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c ascii, v bigint, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v, p, c',
                'v is not null and p is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v) values (0, 'foo', 1)")
            with pytest.raises(InvalidRequest, match='Cannot directly modify a materialized view'):
                cql.execute(f"insert into {mv} (p, c, v) values (1, 'foo', 1)")
            with pytest.raises(InvalidRequest, match='Cannot use ALTER TABLE'):
                cql.execute(f"alter table {mv} add foo text")
            with pytest.raises(InvalidRequest, match='Cannot use ALTER TABLE'):
                cql.execute(f"alter table {mv} with compaction = {{ 'class' : 'LeveledCompactionStrategy' }}")
            cql.execute(f"alter materialized view {mv} with compaction = {{ 'class' : 'LeveledCompactionStrategy' }}")
            cql.execute(f"alter table {table} add foo text")
            cql.execute(f"insert into {table} (p, c, v, foo) values (0, 'foo', 1, 'bar')")
            assert [('bar',)] == list(cql.execute(f"select foo from {mv}"))
            cql.execute(f"alter table {table} rename c to bar")
            assert [('foo',)] == list(cql.execute(f"select bar from {mv}"))

# A base-table column which the view doesn't select can be dropped from the
# base table, and the view continues to work normally.
#
# This is scylla_only, because it is a deliberate Scylla extension: Cassandra
# refuses to drop *any* regular column from a base table that has any
# materialized view at all ("Cannot drop column a on base table ... with
# materialized views", AlterTableStatement.java:516), while Scylla refuses
# only if one of the views actually needs the dropped column - i.e., selects
# it, or depends on its liveness. See issue #4448 and the C++ test
# test_mv_allow_some_column_drops, which is still in view_schema_test.cc and
# covers the rule in more detail.
def test_column_dropped_from_base(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c ascii, a int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c, v', 'v, p, c',
                'v is not null and p is not null and c is not null') as mv:
            cql.execute(f"alter table {table} drop a")
            cql.execute(f"insert into {table} (p, c, v) values (0, 'foo', 1)")
            assert [(1,)] == list(cql.execute(f"select v from {mv}"))

# Test that a view row follows the base row it was generated from: when the
# base row is updated so that the view's partition key changes, the old view
# row goes away and a new one appears in its place.
def test_updates(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, v int, primary key (k)') as table:
        with new_materialized_view(cql, table, '*', 'v, k',
                'k is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (k, v) values (0, 0)")
            assert [(0, 0)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 0)] == list(cql.execute(f"select k, v from {mv} where v = 0"))

            cql.execute(f"insert into {table} (k, v) values (0, 1)")
            assert [(0, 1)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [] == list(cql.execute(f"select k, v from {mv} where v = 0"))
            assert [(0, 1)] == list(cql.execute(f"select k, v from {mv} where v = 1"))

# Like test_updates above, except that here the update leaves the view's key
# columns (k and c) unchanged and modifies only v, which is a regular column
# in both the base table and the view. So the view row is not moved - it is
# just updated in place - which is a different code path, and we check that
# it produces the right result too.
def test_updates_no_read_before_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, primary key (k)') as table:
        with new_materialized_view(cql, table, '*', 'k, c',
                'k is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (k, c, v) values (0, 0, 0)")
            assert [(0, 0)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 0)] == list(cql.execute(f"select k, v from {mv} where k = 0"))

            cql.execute(f"insert into {table} (k, c, v) values (0, 0, 1)")
            assert [(0, 1)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 1)] == list(cql.execute(f"select k, v from {mv} where k = 0"))

# Test that after a materialized view is dropped, its name can be reused for
# a new view.
def test_reuse_name(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int') as table:
        mv = test_keyspace + '.' + unique_name()
        create = (f"create materialized view {mv} as select * from {table} "
                  "where v is not null and p is not null primary key (v, p)")
        try:
            cql.execute(create)
            cql.execute(f"drop materialized view {mv}")
            cql.execute(create)
        finally:
            cql.execute(f"drop materialized view if exists {mv}")

# The list of all the base-table columns of test_all_types()'s table below,
# and for the ones which have a simple "insert a value and read it back"
# check, also the CQL literal to write and the Python value we expect to read
# back. The columns with a None here need a more elaborate check, and are
# checked separately in the test.
ALL_TYPES_COLUMNS = [
    ('asciival', 'ascii', None),
    ('bigintval', 'bigint', ('12121212', 12121212)),
    ('blobval', 'blob', ('0x000001', b'\x00\x00\x01')),
    ('booleanval', 'boolean', ('true', True)),
    ('dateval', 'date', ("'1986-01-19'", datetime.date(1986, 1, 19))),
    ('decimalval', 'decimal', ('123123.123123', Decimal('123123.123123'))),
    ('doubleval', 'double', ('123123.123123', 123123.123123)),
    # A float is only 32-bit, so 123123.123123 is rounded to 123123.125
    ('floatval', 'float', ('123123.123123', 123123.125)),
    ('inetval', 'inet', ("'127.0.0.1'", '127.0.0.1')),
    ('intval', 'int', ('456', 456)),
    ('textval', 'text', ('\'"some " text\'', '"some " text')),
    ('timeval', 'time', ("'07:35:07.000111222'", Time('07:35:07.000111222'))),
    # A timestamp given as a number is milliseconds since the epoch
    ('timestampval', 'timestamp', ("'123123123123'", datetime.datetime(1973, 11, 26, 0, 52, 3, 123000))),
    ('timeuuidval', 'timeuuid', ('D2177dD0-EAa2-11de-a572-001B779C76e3', UUID('d2177dd0-eaa2-11de-a572-001b779c76e3'))),
    ('uuidval', 'uuid', ('6bddc89a-5644-11e4-97fc-56847afe9799', UUID('6bddc89a-5644-11e4-97fc-56847afe9799'))),
    ('varcharval', 'varchar', None),
    ('varintval', 'varint', ('1234567890123456789012345678901234567890', 1234567890123456789012345678901234567890)),
    ('listval', 'list<int>', None),
    ('frozenlistval', 'frozen<list<int>>', None),
    ('setval', 'set<uuid>', None),
    ('frozensetval', 'frozen<set<uuid>>', None),
    ('mapval', 'map<ascii, int>', None),
    ('frozenmapval', 'frozen<map<ascii, int>>', None),
    ('tupleval', 'frozen<tuple<int, ascii, uuid>>', None),
    ('vectorval', 'vector<int, 3>', None),
    ('udtval', None, None),  # the UDT's name is only known at run time
]

# A view's key column may not be a multi-cell column - a non-frozen
# collection - because such a column has no single value to key the view by.
# The base table's partition key k is also not usable here, but for a
# different reason: the view we try to create below would have "k" twice in
# its primary key.
ALL_TYPES_UNUSABLE_AS_VIEW_KEY = ['k', 'listval', 'setval', 'mapval']

# Test that a materialized view can be keyed by a column of any CQL type -
# and that when it is, the rest of the base row, of all types, is correctly
# copied into the view. We create one view per base-table column, keyed by
# that column, and then for each type write a value to the base table and
# read it back through that type's view.
def test_all_types(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b uuid, c set<text>)') as udt:
        columns = [(name, typ if typ else f'frozen<{udt}>', check)
                   for (name, typ, check) in ALL_TYPES_COLUMNS]
        schema = 'k int PRIMARY KEY, ' + ', '.join(f'{name} {typ}' for (name, typ, _) in columns)
        with new_test_table(cql, test_keyspace, schema) as table:
            for col in ALL_TYPES_UNUSABLE_AS_VIEW_KEY:
                # k would appear twice in the view's key below; the other three
                # are multi-cell. The two databases word both errors quite
                # differently, so match loosely.
                error = ('Duplicate.*PRIMARY KEY' if col == 'k'
                         else "MultiCell column|non-frozen collection type")
                with pytest.raises(InvalidRequest, match=error):
                    cql.execute(f'create materialized view {test_keyspace}.{unique_name()} as '
                                f'select * from {table} where {col} is not null and k is not null '
                                f'primary key ({col}, k)')
            with contextlib.ExitStack() as stack:
                mv = {name: stack.enter_context(new_materialized_view(cql, table, '*', f'{name}, k',
                            f'{name} is not null and k is not null'))
                      for (name, _, _) in columns if name not in ALL_TYPES_UNUSABLE_AS_VIEW_KEY}

                # ================ ascii ================
                # This is the first value written to the base row, so at this
                # point the view row's other columns are still null.
                cql.execute(f"insert into {table} (k, asciival) values (0, 'ascii text')")
                assert [(0, 'ascii text', None)] == list(cql.execute(
                    f"select k, asciival, udtval from {mv['asciival']} where asciival = 'ascii text'"))

                # All the other simple types are checked the same way: write
                # the value to the base table, and read it back through the
                # view keyed by that column - together with asciival, to check
                # that the view row also carries the rest of the base row.
                for name, _, check in columns:
                    if check is None:
                        continue
                    literal, expected = check
                    cql.execute(f"insert into {table} (k, {name}) values (0, {literal})")
                    assert [(0, expected, 'ascii text')] == list(cql.execute(
                        f"select k, {name}, asciival from {mv[name]} where {name} = {literal}"))

                # Overwriting a value which is the view's partition key moves
                # the view row from one view partition to another.
                cql.execute(f"insert into {table} (k, booleanval) values (0, false)")
                assert [] == list(cql.execute(
                    f"select k, booleanval, asciival from {mv['booleanval']} where booleanval = true"))
                assert [(0, False, 'ascii text')] == list(cql.execute(
                    f"select k, booleanval, asciival from {mv['booleanval']} where booleanval = false"))

                # ================ lists ================
                # A non-frozen list can't be a view key, so we read it through
                # the view keyed by intval, whose value was set in the loop
                # above. Every kind of list modification must reach the view.
                def check_listval(expected):
                    assert [(0, expected)] == list(cql.execute(
                        f"select k, listval from {mv['intval']} where intval = 456"))
                cql.execute(f"insert into {table} (k, listval) values (0, [1, 2, 3])")
                check_listval([1, 2, 3])
                cql.execute(f"insert into {table} (k, listval) values (0, [1])")
                check_listval([1])
                cql.execute(f"update {table} set listval = listval + [2] where k = 0")
                check_listval([1, 2])
                cql.execute(f"update {table} set listval = [0] + listval where k = 0")
                check_listval([0, 1, 2])
                cql.execute(f"update {table} set listval[1] = 10 where k = 0")
                check_listval([0, 10, 2])
                cql.execute(f"delete listval[1] from {table} where k = 0")
                check_listval([0, 2])
                # An empty list is not stored at all - it reads back as null,
                # in the base table and in the view alike.
                cql.execute(f"insert into {table} (k, listval) values (0, [])")
                assert [(0, None)] == list(cql.execute(f"select k, listval from {table} where k = 0"))
                check_listval(None)

                # frozen
                # A frozen list is a single value, so it can be a view key.
                for value, expected in [('[1, 2, 3]', [1, 2, 3]), ('[3, 2, 1]', [3, 2, 1]), ('[]', [])]:
                    cql.execute(f"insert into {table} (k, frozenlistval) values (0, {value})")
                    assert [(0, expected, 'ascii text')] == list(cql.execute(
                        f"select k, frozenlistval, asciival from {mv['frozenlistval']} "
                        f"where frozenlistval = {value}"))

                # ================ sets ================
                uuid1 = '6bddc89a-5644-11e4-97fc-56847afe9798'
                uuid2 = '6bddc89a-5644-11e4-97fc-56847afe9799'
                uuid3 = '6bddc89a-5644-0000-97fc-56847afe9799'
                def check_setval(expected):
                    assert [(0, expected)] == list(cql.execute(
                        f"select k, setval from {mv['intval']} where intval = 456"))
                cql.execute(f"insert into {table} (k, setval) values (0, {{{uuid1}, {uuid2}}})")
                check_setval({UUID(uuid1), UUID(uuid2)})
                # A duplicate element in the inserted set changes nothing
                cql.execute(f"insert into {table} (k, setval) values (0, {{{uuid1}, {uuid1}, {uuid2}}})")
                check_setval({UUID(uuid1), UUID(uuid2)})
                cql.execute(f"update {table} set setval = setval + {{{uuid3}}} where k = 0")
                check_setval({UUID(uuid1), UUID(uuid2), UUID(uuid3)})
                cql.execute(f"update {table} set setval = setval - {{{uuid3}}} where k = 0")
                check_setval({UUID(uuid1), UUID(uuid2)})
                # As with a list, an empty set reads back as null
                cql.execute(f"insert into {table} (k, setval) values (0, {{}})")
                check_setval(None)

                # frozen
                for value, expected in [('{}', set()),
                                        (f'{{{uuid1}, {uuid2}}}', {UUID(uuid1), UUID(uuid2)}),
                                        (f'{{6bddc89a-0000-11e4-97fc-56847afe9799, {uuid1}}}',
                                         {UUID('6bddc89a-0000-11e4-97fc-56847afe9799'), UUID(uuid1)})]:
                    cql.execute(f"insert into {table} (k, frozensetval) values (0, {value})")
                    assert [(0, expected, 'ascii text')] == list(cql.execute(
                        f"select k, frozensetval, asciival from {mv['frozensetval']} "
                        f"where frozensetval = {value}"))

                # ================ maps ================
                def check_mapval(expected):
                    assert [(0, expected)] == list(cql.execute(
                        f"select k, mapval from {mv['intval']} where intval = 456"))
                cql.execute(f"insert into {table} (k, mapval) values (0, {{'a': 1, 'b': 2}})")
                check_mapval({'a': 1, 'b': 2})
                cql.execute(f"update {table} set mapval['c'] = 3 where k = 0")
                check_mapval({'a': 1, 'b': 2, 'c': 3})
                cql.execute(f"update {table} set mapval['b'] = 10 where k = 0")
                check_mapval({'a': 1, 'b': 10, 'c': 3})
                cql.execute(f"delete mapval['b'] from {table} where k = 0")
                check_mapval({'a': 1, 'c': 3})
                # As with a list or a set, an empty map reads back as null
                cql.execute(f"insert into {table} (k, mapval) values (0, {{}})")
                check_mapval(None)

                # frozen
                for value, expected in [("{'a': 1, 'b': 2}", {'a': 1, 'b': 2}),
                                        ("{'a': 1, 'b': 2, 'c': 3}", {'a': 1, 'b': 2, 'c': 3})]:
                    cql.execute(f"insert into {table} (k, frozenmapval) values (0, {value})")
                    assert [(0, expected, 'ascii text')] == list(cql.execute(
                        f"select k, frozenmapval, asciival from {mv['frozenmapval']} "
                        f"where frozenmapval = {value}"))

                # ================ tuples ================
                cql.execute(f"insert into {table} (k, tupleval) values (0, (1, 'foobar', {uuid2}))")
                assert [(0, (1, 'foobar', UUID(uuid2)), 'ascii text')] == list(cql.execute(
                    f"select k, tupleval, asciival from {mv['tupleval']} "
                    f"where tupleval = (1, 'foobar', {uuid2})"))
                # A null inside the tuple is part of the tuple's value, so it
                # makes a different view key
                cql.execute(f"insert into {table} (k, tupleval) values (0, (1, null, {uuid2}))")
                assert [] == list(cql.execute(
                    f"select k, tupleval, asciival from {mv['tupleval']} "
                    f"where tupleval = (1, 'foobar', {uuid2})"))
                assert [(0, (1, None, UUID(uuid2)), 'ascii text')] == list(cql.execute(
                    f"select k, tupleval, asciival from {mv['tupleval']} "
                    f"where tupleval = (1, null, {uuid2})"))

                # ================ vectors ================
                for value, expected in [('[1, 2, 3]', [1, 2, 3]), ('[3, 2, 1]', [3, 2, 1])]:
                    cql.execute(f"insert into {table} (k, vectorval) values (0, {value})")
                    assert [(0, expected, 'ascii text')] == list(cql.execute(
                        f"select k, vectorval, asciival from {mv['vectorval']} "
                        f"where vectorval = {value}"))

                # ================ UDTs ================
                # A UDT value can be written with its fields named, in any
                # order, and read back by a literal naming them in a different
                # order, or by a positional literal - all of these denote the
                # same value, so they are the same view key.
                # Note that a UDT value can also be *written* with a positional
                # literal, but that crashes Cassandra, so it is checked in the
                # separate test test_all_types_udt_positional_write() below.
                for value in [f"{{a: 1, b: {uuid2}, c: {{'foo', 'bar'}}}}",
                              f"{{b: {uuid2}, a: 1, c: {{'foo', 'bar'}}}}"]:
                    cql.execute(f"insert into {table} (k, udtval) values (0, {value})")
                    for lookup in [value, f"(1, {uuid2}, {{'foo', 'bar'}})"]:
                        assert [(0, 1, UUID(uuid2), {'bar', 'foo'}, 'ascii text')] == list(cql.execute(
                            f"select k, udtval.a, udtval.b, udtval.c, asciival from {mv['udtval']} "
                            f"where udtval = {lookup}"))
                # A null field, or a missing field, is part of the UDT's value
                # and makes a different view key
                cql.execute(f"insert into {table} (k, udtval) values (0, "
                            f"{{a: null, b: {uuid2}, c: {{'foo', 'bar'}}}})")
                assert [] == list(cql.execute(
                    f"select k, udtval.a, udtval.b, udtval.c, asciival from {mv['udtval']} "
                    f"where udtval = {{a: 1, b: {uuid2}, c: {{'foo', 'bar'}}}}"))
                assert [(0, None, UUID(uuid2), {'bar', 'foo'}, 'ascii text')] == list(cql.execute(
                    f"select k, udtval.a, udtval.b, udtval.c, asciival from {mv['udtval']} "
                    f"where udtval = {{a: null, b: {uuid2}, c: {{'foo', 'bar'}}}}"))
                cql.execute(f"insert into {table} (k, udtval) values (0, {{a: 1, b: {uuid2}}})")
                assert [] == list(cql.execute(
                    f"select k, udtval.a, udtval.b, udtval.c, asciival from {mv['udtval']} "
                    f"where udtval = {{a: 1, b: {uuid2}, c: {{'foo', 'bar'}}}}"))
                assert [(0, 1, UUID(uuid2), None, 'ascii text')] == list(cql.execute(
                    f"select k, udtval.a, udtval.b, udtval.c, asciival from {mv['udtval']} "
                    f"where udtval = {{a: 1, b: {uuid2}}}"))

# The part of test_all_types() above which the original C++ test had inline,
# but which can't run on Cassandra: writing a UDT value with a *positional*
# literal, (1, ...), instead of a named one, {a: 1, ...}.
#
# Cassandra accepts a positional literal for a UDT when reading - the WHERE
# clause in test_all_types() above works there - because in Cassandra a
# UserType is a subclass of TupleType, so Tuples.Literal.prepare() happily
# prepares a tuple literal against a UDT receiver. But on the write path,
# UserTypes.Setter.execute() then does an unchecked cast of the prepared term
# to UserTypes.Value, and a tuple literal prepares into a Tuples.Value - so
# the INSERT dies with a server-side
#   java.lang.ClassCastException: class org.apache.cassandra.cql3.Tuples$Value
#   cannot be cast to class org.apache.cassandra.cql3.UserTypes$Value
# (Cassandra 5.0.8, UserTypes.java:342). A ClassCastException is never a
# correct answer - Cassandra should either accept the write, as Scylla does,
# or reject it with a proper error - so this is marked cassandra_bug and not
# scylla_only. I could not find this reported in Cassandra's JIRA; the closest
# is CASSANDRA-20237, the same kind of unchecked-cast crash for a tuple
# literal on the *read* path, fixed only in 6.0. In Cassandra trunk the
# UserTypes.Value class is gone entirely (refactored away by CASSANDRA-18813),
# so this crash may well have incidentally disappeared in 6.0 too.
def test_all_types_udt_positional_write(cql, test_keyspace, cassandra_bug):
    uuid2 = '6bddc89a-5644-11e4-97fc-56847afe9799'
    with new_type(cql, test_keyspace, '(a int, b uuid, c set<text>)') as udt:
        with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, udtval frozen<{udt}>') as table:
            with new_materialized_view(cql, table, '*', 'udtval, k',
                    'udtval is not null and k is not null') as mv:
                cql.execute(f"insert into {table} (k, udtval) values (0, (1, {uuid2}, {{'foo', 'bar'}}))")
                assert [(0, 1, UUID(uuid2), {'bar', 'foo'})] == list(cql.execute(
                    f"select k, udtval.a, udtval.b, udtval.c from {mv} "
                    f"where udtval = {{a: 1, b: {uuid2}, c: {{'foo', 'bar'}}}}"))

# A materialized view is dropped with DROP MATERIALIZED VIEW, not DROP TABLE.
def test_drop_table_with_mv(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int') as table:
        with new_materialized_view(cql, table, '*', 'v, p',
                'v is not null and p is not null') as mv:
            with pytest.raises(InvalidRequest, match='Cannot use DROP TABLE on'):
                cql.execute(f"drop table {mv}")

# A base table cannot be dropped while a materialized view still reads from
# it - the view has to be dropped first.
def test_drop_table_with_active_mv(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int') as table:
        mv = test_keyspace + '.' + unique_name()
        cql.execute(f"create materialized view {mv} as select * from {table} "
                    "where v is not null and p is not null primary key (v, p)")
        try:
            with pytest.raises(InvalidRequest, match='materialized views still depend on it'):
                cql.execute(f"drop table {table}")
        finally:
            cql.execute(f"drop materialized view {mv}")
        # Now that the view is gone, the base table can be dropped. We let
        # new_test_table() above do that when it exits.

# Changing the type of a base-table column to a compatible type is allowed
# even when that column is also a key column of a materialized view - the
# view's copy of the column changes type as well.
#
# Note that this test, and the four ALTER ... TYPE tests below it, are
# scylla_only: Cassandra removed support for changing a column's type, and
# now fails any such request with "Altering column types is no longer
# supported".
def test_alter_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c text, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'p, c', 'p is not null and c is not null'):
            cql.execute(f"alter table {table} alter c type blob")

# As test_alter_table above, but here the base table's clustering column is
# in reversed (descending) order, and the view's is not.
def test_alter_reversed_type_base_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c text, primary key (p, c)',
            extra='with clustering order by (c desc)') as table:
        with new_materialized_view(cql, table, '*', 'p, c', 'p is not null and c is not null',
                extra='with clustering order by (c asc)'):
            cql.execute(f"alter table {table} alter c type blob")

# The same, the other way around: the view's clustering column is in reversed
# (descending) order, and the base table's is not.
def test_alter_reversed_type_view_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c text, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'p, c', 'p is not null and c is not null',
                extra='with clustering order by (c desc)'):
            cql.execute(f"alter table {table} alter c type blob")

# Here the altered column c is only a regular column in the base table, and
# becomes a clustering key in the view. Changing text to blob is allowed,
# because the two types have the same representation and sort the same way.
def test_alter_compatible_type(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c text, primary key (p)') as table:
        with new_materialized_view(cql, table, '*', 'p, c', 'p is not null and c is not null',
                extra='with clustering order by (c desc)'):
            cql.execute(f"alter table {table} alter c type blob")

# But changing int to blob is not allowed, because the two types sort
# differently, and c is a clustering key of the view.
def test_alter_incompatible_type(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c int, primary key (p)') as table:
        with new_materialized_view(cql, table, '*', 'p, c', 'p is not null and c is not null',
                extra='with clustering order by (c desc)'):
            # Note that Scylla reports this particular check - the ordering
            # compatibility of a view's clustering key - as a configuration
            # error rather than an invalid request.
            with pytest.raises(ConfigurationException, match='Cannot change c from type int to type blob'):
                cql.execute(f"alter table {table} alter c type blob")

# Dropping a materialized view which doesn't exist is an error - whether it
# is the view or its whole keyspace which is missing - unless IF EXISTS is
# used, and then it silently does nothing.
#
# The original C++ test only checked that the failing cases fail, and we do
# the same here rather than matching a specific error, because the two
# databases disagree on both the message and the error type: Scylla says
# "Cannot drop non existing materialized view '...' in keyspace '...'." as a
# ConfigurationException, Cassandra says "Materialized view '...' doesn't
# exist" as an InvalidRequest.
def test_drop_non_existing(cql, test_keyspace):
    for name in [f'{test_keyspace}.view_does_not_exist',
                 'keyspace_does_not_exist.view_does_not_exist']:
        with pytest.raises((ConfigurationException, InvalidRequest)):
            cql.execute(f"drop materialized view {name}")
        cql.execute(f"drop materialized view if exists {name}")

# A view whose SELECT names only some of its primary key columns - here just
# p, while the view's key is (v, p, c). The unnamed key columns are selected
# implicitly, so the view still has all three.
#
# This, and the two tests after it, are marked cassandra_bug, because
# Cassandra 4 and 5 reject such a view with "Unknown column 'v' referenced in
# PRIMARY KEY for materialized view" - they require every one of the view's
# key columns to be named in the SELECT. The reason we consider Scylla's
# behavior the correct one, and Cassandra's a bug, is that implicit selection
# of the view's key columns is the documented behavior ("All primary key
# columns are automatically included") and is what Cassandra 3 did - so this
# is an undocumented regression in Cassandra 4, reported as CASSANDRA-20701
# ("Materialized view should automatically SELECT view's primary key
# columns"), which at the time of writing is still unresolved. The same
# reasoning, and the same marker, appear at test_mv_select_key_columns() in
# test_materialized_view.py.
def test_create_mv_with_unrestricted_pk_parts(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c ascii, v bigint, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p', 'v, p, c',
                'v is not null and p is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v) values (0, 'foo', 1)")
            assert [(1, 0, 'foo')] == list(cql.execute(f"select * from {mv}"))

# Deleting a whole base partition removes all of that partition's rows from
# the view as well.
# cassandra_bug (CASSANDRA-20701) for the same reason as the previous test:
# the view's SELECT doesn't name all of the view's key columns.
def test_partition_tombstone(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p', 'p, c, v',
                'p is not null and c is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v) values (1, 2, 200)")
            cql.execute(f"insert into {table} (p, c, v) values (1, 3, 300)")
            assert 2 == len(list(cql.execute(f"select * from {mv}")))
            cql.execute(f"delete from {table} where p = 1")
            assert 0 == len(list(cql.execute(f"select * from {mv}")))

# The same, for the deletion of a single base row rather than a whole
# partition - only that row's view row goes away.
# cassandra_bug (CASSANDRA-20701) for the same reason as the previous two
# tests: the view's SELECT doesn't name all of the view's key columns.
def test_ck_tombstone(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p', 'p, c, v',
                'p is not null and c is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v) values (1, 2, 200)")
            cql.execute(f"insert into {table} (p, c, v) values (1, 3, 300)")
            assert 2 == len(list(cql.execute(f"select * from {mv}")))
            cql.execute(f"delete from {table} where p = 1 and c = 3")
            assert 1 == len(list(cql.execute(f"select * from {mv}")))

# A materialized view cannot have a static column - not as one of its key
# columns, and not even as an ordinary column of the view, whether it is
# named explicitly in the SELECT or picked up implicitly by "select *".
# Only a view which leaves the base table's static columns out altogether
# can be created.
def test_static_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, sv int static, v int, primary key (p, c)') as table:
        for select, pk in [('*', 'sv, p, c'), ('v, sv', 'v, p, c'), ('*', 'v, p, c')]:
            where = ('p is not null and c is not null and '
                     + ('sv is not null' if 'sv' in pk else 'v is not null'))
            with pytest.raises(InvalidRequest, match="[Ss]tatic column 'sv'"):
                with new_materialized_view(cql, table, select, pk, where):
                    pass
        with new_materialized_view(cql, table, 'v, p, c', 'v, p, c',
                'p is not null and c is not null and v is not null') as mv:
            for i in range(100):
                cql.execute(f"insert into {table} (p, c, sv, v) values (0, {i % 2}, {i * 100}, {i})")
            assert 2 == len(list(cql.execute(f"select * from {mv}")))
            # The view has no sv column, so it can't be selected from the view
            with pytest.raises(InvalidRequest, match='Unrecognized name sv|Undefined column name sv'):
                cql.execute(f"select sv from {mv}")

# A base table can have a static column even if its view doesn't select it,
# and then writing a base row with or without a value for that static column
# both produce the expected view row.
def test_static_data(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int static, primary key (a, b)',
            extra='with clustering order by (b asc)') as table:
        with new_materialized_view(cql, table, 'a, b', 'b, a', 'a is not null and b is not null',
                extra='with clustering order by (a asc)') as mv:
            cql.execute(f"insert into {table} (a, b) values (1, 2)")
            assert [(1, 2)] == list(cql.execute(f"select a, b from {table} where a = 1"))
            assert [(1, 2)] == list(cql.execute(f"select a, b from {mv} where b = 2"))

            cql.execute(f"insert into {table} (a, b, c) values (3, 4, 5)")
            assert [(3, 4)] == list(cql.execute(f"select a, b from {table} where a = 3"))
            assert [(3, 4)] == list(cql.execute(f"select a, b from {mv} where b = 4"))

# A base-table write with a timestamp older than the row's current one is
# ignored, and so must leave the view alone as well - while a write with a
# newer timestamp must move the view row.
def test_old_timestamps(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v, p, c',
                'p is not null and c is not null and v is not null') as mv:
            for i in range(100):
                cql.execute(f"insert into {table} (p, c, v) values (0, {i % 2}, 1)")
            assert 2 == len(list(cql.execute(f"select * from {mv}")))
            assert [(0,), (1,)] == list(cql.execute(f"select c from {mv} where p = 0 and v = 1"))

            # Make sure an old TS does nothing
            cql.execute(f"update {table} using timestamp 100 set v = 5 where p = 0 and c = 0")
            assert [(0,), (1,)] == list(cql.execute(f"select c from {mv} where p = 0 and v = 1"))
            assert [] == list(cql.execute(f"select c from {mv} where p = 0 and v = 5"))

            # Latest TS
            cql.execute(f"update {table} set v = 5 where p = 0 and c = 0")
            assert [(0,)] == list(cql.execute(f"select c from {mv} where p = 0 and v = 5"))
            assert [(1,)] == list(cql.execute(f"select c from {mv} where p = 0 and v = 1"))

# When a view's key column and an ordinary column are written by separate
# updates, each with its own timestamp, the view row must reflect the winner
# of each column separately - including across a row deletion which shadows
# only the writes older than it.
def test_regular_column_timestamp_updates(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v1 int, v2 int') as table:
        with new_materialized_view(cql, table, '*', 'p, v1',
                'p is not null and v1 is not null') as mv:
            cql.execute(f"update {table} using timestamp 1 set v1 = 0, v2 = 0 where p = 0")
            cql.execute(f"update {table} using timestamp 1 set v2 = 1 where p = 0")
            cql.execute(f"update {table} using timestamp 1 set v1 = 1 where p = 0")
            assert [(0, 1, 1)] == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"delete from {table} using timestamp 2 where p = 0")

            cql.execute(f"update {table} using timestamp 3 set v1 = 0, v2 = 0 where p = 0")
            cql.execute(f"update {table} using timestamp 4 set v1 = 1 where p = 0")
            cql.execute(f"update {table} using timestamp 5 set v2 = 1 where p = 0")
            cql.execute(f"update {table} using timestamp 6 set v1 = 2 where p = 0")
            cql.execute(f"update {table} using timestamp 7 set v2 = 2 where p = 0")
            assert [(0, 2, 2)] == list(cql.execute(f"select * from {mv}"))

# A counter table cannot have a materialized view.
def test_counters_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, count counter') as table:
        with pytest.raises(InvalidRequest, match='Materialized views are not supported on counter tables'):
            with new_materialized_view(cql, table, '*', 'count, p',
                    'p is not null and count is not null'):
                pass

# A long sequence of writes to the same base row with explicit, out-of-order
# timestamps, checking after each step that the view agrees with the base
# table about which write won for which column. The whole sequence is run
# twice: once as-is, and once flushing all memtables after every step, so
# that the following reads have to go back to the sstables.
#
# For the flush to have that effect the row cache needs to be off - otherwise
# the reads are still served from memory and the flush proves nothing.
# On Cassandra it already is off, twice over: row_cache_size defaults to 0,
# and a table's "caching" property defaults to rows_per_partition=NONE, so no
# rows are cached even if it weren't. Scylla's row cache, in contrast, is
# always on, so there we have to switch it off for these two tables.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_complex_timestamp_updates(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, v3 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'v1, p, c',
                'p is not null and c is not null and v1 is not null', extra=no_cache) as mv:
            # Set initial values TS=0, leaving v3 null and verify view
            cql.execute(f"insert into {table} (p, c, v1, v2) values (0, 0, 1, 0) using timestamp 0")
            assert [(1, 0, 0, 0, None)] == list(cql.execute(f"select * from {mv}"))

            # Update v1's timestamp TS=2
            cql.execute(f"update {table} using timestamp 2 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v1 @ TS=3, tombstones v1=1 and adds v1=0 partition
            cql.execute(f"update {table} using timestamp 3 set v1 = 0 where p = 0 and c = 0")
            maybe_flush()
            assert [] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v1 back to 1 with TS=4
            cql.execute(f"update {table} using timestamp 4 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0, None)] == list(cql.execute(f"select v2, v3 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Add v3 @ TS=1
            cql.execute(f"update {table} using timestamp 1 set v3 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0, 1)] == list(cql.execute(f"select v2, v3 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v2 @ TS=2
            cql.execute(f"update {table} using timestamp 2 set v2 = 2 where p = 0 and c = 0")
            maybe_flush()
            assert [(2,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v2 @ TS=3
            cql.execute(f"update {table} using timestamp 3 set v2 = 4 where p = 0 and c = 0")
            maybe_flush()
            assert [(4,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Tombstone v1
            cql.execute(f"delete from {table} using timestamp 5 where p = 0 and c = 0")
            assert [] == list(cql.execute(f"select v2 from {mv}"))

            # Add the row back without v2
            cql.execute(f"insert into {table} (p, c, v1) values (0, 0, 1) using timestamp 6")
            # Make sure v2 doesn't pop back in.
            assert [(None,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # New partition
            # Insert a row @ TS=0
            cql.execute(f"insert into {table} (p, c, v1, v2, v3) values (1, 0, 0, 0, 0) using timestamp 0")

            # Overwrite PK, v1 and v3 @ TS=1, but don't overwrite v2
            cql.execute(f"insert into {table} (p, c, v1, v3) values (1, 0, 0, 0) using timestamp 1")

            # Delete @ TS=0 (which should only delete v2)
            cql.execute(f"delete from {table} using timestamp 0 where p = 1 and c = 0")
            assert [(0, 1, 0, None, 0)] == list(cql.execute(f"select * from {mv} where v1 = 0 and p = 1 and c = 0"))

            cql.execute(f"update {table} using timestamp 2 set v1 = 1 where p = 1 and c = 0")
            maybe_flush()
            cql.execute(f"update {table} using timestamp 3 set v1 = 0 where p = 1 and c = 0")
            maybe_flush()
            assert [(0, 1, 0, None, 0)] == list(cql.execute(f"select * from {mv} where v1 = 0 and p = 1 and c = 0"))

            cql.execute(f"update {table} using timestamp 3 set v2 = 0 where p = 1 and c = 0")
            maybe_flush()
            assert [(0, 1, 0, 0, 0)] == list(cql.execute(f"select * from {mv} where v1 = 0 and p = 1 and c = 0"))

# Deleting a range of base rows - a whole clustering prefix, or a slice of
# one - removes exactly those rows from the view, even though in the view
# they are spread over many different partitions.
def test_range_tombstone(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c1 int, c2 int, v int, primary key (p, c1, c2)') as table:
        with new_materialized_view(cql, table, '*', '(v, p), c1, c2',
                'p is not null and c1 is not null and c2 is not null and v is not null') as mv:
            for i in range(100):
                cql.execute(f"insert into {table} (p, c1, c2, v) values (0, {i % 2}, {i}, 1)")
            assert 100 == len(list(cql.execute(f"select * from {mv}")))

            cql.execute(f"delete from {table} where p = 0 and c1 = 0")
            assert 50 == len(list(cql.execute(f"select * from {mv}")))

            cql.execute(f"delete from {table} where p = 0 and c1 = 1 and c2 >= 50 and c2 < 101")
            assert 25 == len(list(cql.execute(f"select * from {mv}")))

# A collection column of the base table is copied to the view, both when it
# is written together with the view's key column and when the two are written
# by separate statements.
def test_collections(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, v int, lv list<int>, primary key (p)') as table:
        with new_materialized_view(cql, table, '*', 'v, p',
                'p is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (p, v, lv) values (0, 0, [1, 2, 3])")
            assert [(0, [1, 2, 3])] == list(cql.execute(f"select p, lv from {mv} where v = 0"))

            cql.execute(f"insert into {table} (p, v) values (1, 1)")
            cql.execute(f"insert into {table} (p, lv) values (1, [1, 2, 3])")
            assert [(1, [1, 2, 3])] == list(cql.execute(f"select p, lv from {mv} where v = 1"))

# Overwriting the base column which is the view's partition key moves the
# view row to a new view partition.
def test_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, v int, primary key (p)') as table:
        with new_materialized_view(cql, table, '*', 'v, p',
                'p is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (p, v) values (0, 0)")
            assert [(0, 0)] == list(cql.execute(f"select * from {mv} where v = 0"))

            cql.execute(f"insert into {table} (p, v) values (0, 1)")
            assert [(1, 0)] == list(cql.execute(f"select * from {mv} where v = 1"))

# A base row written with a timestamp older than an existing row tombstone
# is dead on arrival, and must not produce a view row either.
def test_row_deletion(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v1, c, p',
                'p is not null and c is not null and v1 is not null') as mv:
            cql.execute(f"delete from {table} using timestamp 6 where p = 1 and c = 1")
            cql.execute(f"insert into {table} (p, c, v1, v2) values (1, 1, 1, 1) using timestamp 3")
            assert [] == list(cql.execute(f"select * from {mv}"))

# When many writes give the same base row different values for the view's
# partition key, the view ends up with exactly one row - the one for the
# value that won.
def test_conflicting_timestamp(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v, c, p',
                'p is not null and c is not null and v is not null') as mv:
            for i in range(50):
                cql.execute(f"insert into {table} (p, c, v) values (1, 1, {i})")
            assert [(49, 1, 1)] == list(cql.execute(f"select * from {mv}"))

# A view's clustering order is its own: it may reverse the base table's, or
# order by a different column altogether, and defaults to ascending when the
# view doesn't ask for an order.
def test_clustering_order(cql, test_keyspace):
    # Each view below is given as its primary key, its CLUSTERING ORDER BY
    # clause, and the column we read back with the order we expect it in.
    views = [('a, b, c', 'with clustering order by (b desc, c asc)', 'b', [(2,), (1,)]),
             ('a, c, b', 'with clustering order by (c asc, b asc)', 'c', [(1,), (2,)]),
             ('a, b, c', '', 'b', [(1,), (2,)]),
             ('a, c, b', 'with clustering order by (c desc, b asc)', 'c', [(2,), (1,)])]
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)',
            extra='with clustering order by (b asc, c desc)') as table:
        with contextlib.ExitStack() as stack:
            mvs = [stack.enter_context(new_materialized_view(cql, table, '*', pk,
                        'a is not null and b is not null and c is not null', extra=extra))
                   for pk, extra, _, _ in views]
            cql.execute(f"insert into {table} (a, b, c, d) values (1, 1, 1, 1)")
            cql.execute(f"insert into {table} (a, b, c, d) values (1, 2, 2, 2)")
            for mv, (_, _, column, expected) in zip(mvs, views):
                assert expected == list(cql.execute(f"select {column} from {mv}"))

# Both kinds of multi-row deletion in the base table - a clustering range and
# a whole partition - reach the view, even though the deleted base rows end
# up in different view partitions.
def test_multiple_deletes(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'c, p',
                'p is not null and c is not null') as mv:
            for c in [1, 2, 3]:
                cql.execute(f"insert into {table} (p, c) values (1, {c})")
            assert [(1, 1), (1, 2), (1, 3)] == sorted(cql.execute(f"select p, c from {mv}"))

            cql.execute(f"delete from {table} where p = 1 and c > 1 and c < 3")
            assert [(1, 1), (1, 3)] == sorted(cql.execute(f"select p, c from {mv}"))

            cql.execute(f"delete from {table} where p = 1")
            assert [] == list(cql.execute(f"select p, c from {mv}"))

# A view's primary key may contain at most one column which is not part of
# the base table's primary key.
def test_multiple_non_primary_keys_in_view(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, e int, primary key ((a, b), c)') as table:
        where = ('a is not null and b is not null and c is not null and '
                 'd is not null and e is not null')
        for pk in ['(d, a), b, e, c', '(a, b), c, d, e']:
            with pytest.raises(InvalidRequest, match='Cannot include more than one non-primary key column'):
                with new_materialized_view(cql, table, '*', pk, where):
                    pass

# Setting to null a base column which is one of the view's clustering key
# columns removes the view row - and a later update of an unrelated column
# doesn't bring it back.
def test_null_in_clustering_columns(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'p, v1, c',
                'p is not null and c is not null and v1 is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v1, v2) values (0, 1, 2, 3)")
            assert [(0, 1, 2, 3)] == list(cql.execute(f"select p, c, v1, v2 from {mv}"))

            cql.execute(f"update {table} set v1 = null where p = 0 and c = 1")
            assert [] == list(cql.execute(f"select p, c, v1, v2 from {mv}"))

            cql.execute(f"update {table} set v2 = 9 where p = 0 and c = 1")
            assert [] == list(cql.execute(f"select p, c, v1, v2 from {mv}"))

# A materialized view may not have a default_time_to_live of its own - not
# when it is created, and not later with ALTER - because a view row always
# expires together with the base row it came from.
def test_create_and_alter_mv_with_ttl(cql, test_keyspace):
    # Scylla and Cassandra word this differently, and Cassandra words the
    # CREATE and ALTER cases differently from each other, so match loosely.
    ttl_error = 'default_time_to_live.*for a materialized view'
    with new_test_table(cql, test_keyspace, 'p int primary key, v int',
            extra='with default_time_to_live = 60') as table:
        with pytest.raises(InvalidRequest, match=ttl_error):
            with new_materialized_view(cql, table, '*', 'v, p',
                    'p is not null and v is not null',
                    extra='with default_time_to_live = 30'):
                pass
        with new_materialized_view(cql, table, '*', 'v, p',
                'p is not null and v is not null') as mv:
            with pytest.raises(InvalidRequest, match=ttl_error):
                cql.execute(f"alter materialized view {mv} with default_time_to_live = 30")

# Which restrictions a view's SELECT may and may not have. Every one of the
# base table's key columns has to be restricted somehow, but beyond the usual
# IS NOT NULL a key column may also be pinned to a value or a range, with all
# the usual forms of restriction - including on a tuple of columns, and with
# the value given by a cast or a function call.
def test_create_with_select_restrictions(cql, test_keyspace):
    pk = '(a, b), c, d'
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, e int, primary key ((a, b), c, d)') as table:
        # Leaving any one of the base key columns unrestricted is an error
        for where in ['b is not null and c is not null and d is not null',
                      'a is not null and c is not null and d is not null',
                      'a is not null and b is not null and d is not null',
                      'a is not null and b is not null and c is not null']:
            with pytest.raises(InvalidRequest, match='Primary key column.*IS NOT NULL'):
                with new_materialized_view(cql, table, '*', pk, where):
                    pass
        # And so is having no WHERE clause at all
        with pytest.raises(InvalidRequest, match='Primary key column.*IS NOT NULL'):
            cql.execute(f"create materialized view {test_keyspace}.{unique_name()} as "
                        f"select * from {table} primary key (a, b, c, d)")
        # All of these, on the other hand, are allowed
        for where in ['a = 1 and b = 1 and c is not null and d is not null',
                      'a is not null and b is not null and c = 1 and d is not null',
                      'a is not null and b is not null and c = 1 and d = 1',
                      'a = 1 and b = 1 and c = 1 and d = 1',
                      'a = 1 and b = 1 and c > 1 and d is not null',
                      'a = 1 and b = 1 and c = 1 and d in (1, 2, 3)',
                      'a = 1 and b = 1 and (c, d) = (1, 1)',
                      'a = 1 and b = 1 and (c, d) > (1, 1)',
                      'a = 1 and b = 1 and (c, d) in ((1, 1), (2, 2))',
                      'a = (int) 1 and b = 1 and c = 1 and d = 1',
                      'a = blobasint(intasblob(1)) and b = 1 and c = 1 and d = 1']:
            with new_materialized_view(cql, table, '*', pk, where):
                pass

# A view's WHERE clause may pin a base key column to a value given by a
# function call or by a type cast, not just by a plain literal. Either way
# the view holds exactly the base rows matching that value - and renaming the
# column in the base table doesn't disturb it.
@pytest.mark.parametrize("restriction", ['blobAsInt(intAsBlob(1))', '(int) 1'],
                         ids=['function', 'type_cast'])
def test_filter_with_function_or_type_cast(cql, test_keyspace, restriction):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'p, c',
                f'p = {restriction} and c is not null') as mv:
            for p, c, v in [(0, 0, 0), (0, 1, 1), (1, 0, 2), (1, 1, 3)]:
                cql.execute(f"insert into {table} (p, c, v) values ({p}, {c}, {v})")
            assert [(1, 0, 2), (1, 1, 3)] == list(cql.execute(f"select p, c, v from {mv}"))

            cql.execute(f"alter table {table} rename p to foo")
            assert [(1, 0, 2), (1, 1, 3)] == list(cql.execute(f"select foo, c, v from {mv}"))

# A view's WHERE clause may restrict a column of any type to a value. Here
# the base table's primary key is made of one column of each type, the view
# restricts every one of them, and a row matching all of those restrictions
# must show up in the view.
#
# Each entry below is a column's name, its type, and the value the view
# restricts it to and which we then write to the base table. The UDT column's
# type is only known at run time, so it is left as None here.
RESTRICTIONS_ON_ALL_TYPES = [
    ('asciival', 'ascii', "'abc'"),
    ('bigintval', 'bigint', '123'),
    ('blobval', 'blob', '0xfeed'),
    ('booleanval', 'boolean', 'true'),
    ('dateval', 'date', "'1987-03-23'"),
    ('decimalval', 'decimal', '123.123'),
    ('doubleval', 'double', '123.123'),
    ('floatval', 'float', '123.123'),
    ('inetval', 'inet', "'127.0.0.1'"),
    ('intval', 'int', '123'),
    ('textval', 'text', "'abc'"),
    ('timeval', 'time', "'07:35:07.000111222'"),
    ('timestampval', 'timestamp', '123123123'),
    ('timeuuidval', 'timeuuid', '6BDDC89A-5644-11E4-97FC-56847AFE9799'),
    ('uuidval', 'uuid', '6BDDC89A-5644-11E4-97FC-56847AFE9799'),
    ('varcharval', 'varchar', "'abc'"),
    ('varintval', 'varint', '123123123'),
    ('frozenlistval', 'frozen<list<int>>', '[1, 2, 3]'),
    ('frozensetval', 'frozen<set<uuid>>', '{6BDDC89A-5644-11E4-97FC-56847AFE9799}'),
    ('frozenmapval', 'frozen<map<ascii, int>>', "{'a': 1, 'b': 2}"),
    ('tupleval', 'frozen<tuple<int, ascii, uuid>>', "(1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799)"),
    ('vectorval', 'vector<int, 3>', '[1, 2, 3]'),
    ('udtval', None, "{a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}}"),
]

def test_restrictions_on_all_types(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b uuid, c set<text>)') as udt:
        columns = [(name, typ if typ else f'frozen<{udt}>', value)
                   for (name, typ, value) in RESTRICTIONS_ON_ALL_TYPES]
        names = ', '.join(name for (name, _, _) in columns)
        schema = (', '.join(f'{name} {typ}' for (name, typ, _) in columns)
                  + f', primary key ({names})')
        where = ' and '.join(f'{name} = {value}' for (name, _, value) in columns)
        values = ', '.join(value for (_, _, value) in columns)
        with new_test_table(cql, test_keyspace, schema) as table:
            with new_materialized_view(cql, table, '*', names, where) as mv:
                cql.execute(f"insert into {table} ({names}) values ({values})")
                assert 1 == len(list(cql.execute(f"select * from {mv}")))

# Test a view defined by a SELECT which filters by a non-primary key column
# which also happens to be a new primary key column in the view.
# This used to cause problems (see issue #3430), but no longer does.
# We still have problems in issue #3430 when one non-PK column is filtered,
# and a different one is added to the view's PK (see other tests below).
#
# This is scylla_only. Cassandra refuses to restrict a non-primary-key column
# at all - "Non-primary key columns can only be restricted with 'IS NOT NULL'"
# - unless the unsafe system property cassandra.mv_allow_filtering_nonkey_
# columns_unsafe is set. Scylla deliberately allows the one case this test
# uses: the filtered column is exactly the column added to the view's primary
# key. As explained by CASSANDRA-13798, the danger of filtering a non-key
# column is that the view row's liveness then depends on several base columns
# at once, which does not happen when the filtered column *is* the one added
# to the view's key.
def test_non_primary_key_restrictions(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b)') as table:
        with new_materialized_view(cql, table, '*', 'a, b, c',
                'a is not null and b is not null and c is not null and c = 1') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a in [0, 1]:
                for b in [0, 1]:
                    for c in [0, 1]:
                        cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            # Only the last write to each of the four base rows survives, and
            # all four of those have c=1, so all four are in the view.
            matching = [(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0), (1, 1, 1, 0)]
            check(matching)

            # Insert new rows that do not match the filter c=1, so will cause no
            # change to the view table:
            cql.execute(f"insert into {table} (a, b, c, d) values (2, 0, 0, 0)")
            cql.execute(f"insert into {table} (a, b, c, d) values (2, 1, 2, 0)")
            check(matching)

            # Insert two new base rows that do match the filter c=1, so will
            # add new view rows as well. This test is superfluous, as above
            # we already added 4 rows in the same fashion.
            cql.execute(f"insert into {table} (a, b, c, d) values (1, 2, 1, 0)")
            cql.execute(f"insert into {table} (a, b, c, d) values (1, 3, 1, 0)")
            check(matching + [(1, 2, 1, 0), (1, 3, 1, 0)])

            # Delete one of the rows we just added which matches the filter,
            # so a view row will also be removed.
            cql.execute(f"delete from {table} where a = 1 and b = 2")
            check(matching + [(1, 3, 1, 0)])

            # Change the c on one of the rows we just added from 1 to 0.
            # Because it previously had c=1, it had a matching view row, but
            # now that it has c=0 this view row will have to be deleted.
            # A row with a=1,b=3 will still exist in the base table, but not
            # in the view table.
            cql.execute(f"update {table} set c = 0 where a = 1 and b = 3")
            check(matching)

            # Change the c on the row which now has c=0 back to c=1, should
            # cause the view row to be added again.
            cql.execute(f"update {table} set c = 1 where a = 1 and b = 3")
            check(matching + [(1, 3, 1, 0)])

            # Finally delete this row that now has c=1. The view row should also
            # get deleted (as we've already tested above).
            cql.execute(f"delete from {table} where a = 1 and b = 3")
            check(matching)

            # The following update creates a new base row, which doesn't have c=1
            # (it has an empty c) so it will not create a new view row or change
            # any existing view row.
            cql.execute(f"update {table} set d = 1 where a = 0 and b = 2")
            check(matching)

            # This sets d=1 on a base row which already exists and has c=1,
            # matching the view's filter, so the data also appears in the view
            # row:
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 1")
            check([(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0), (1, 1, 1, 1)])

            # This deletes a base row we created above which didn't have c=1
            # so a view row was not created for it, c is still not 1 and now
            # now we don't need to delete any view row.
            cql.execute(f"delete from {table} where a = 0 and b = 2")
            check([(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0), (1, 1, 1, 1)])

            # This deletes a row which does have c=1, so it matches the view
            # filter and has a corresponding view row which should be deleted
            cql.execute(f"delete from {table} where a = 1 and b = 1")
            check([(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0)])

            # Delete an entire partition. This partition has two rows, both match
            # the view filter c=1, and cause two view rows to also be deleted.
            cql.execute(f"delete from {table} where a = 0")
            check([(1, 0, 1, 0)])

# This is an test of a view filtered by a non-key column (a column which is
# neither in the base's primary key, nor the view primary key).
# The unique difficulty with filtering by a non-key column is that the value
# of such column can be *updated* - and also be expired with TTL - so the
# question of whether a base row matches or doesn't match the filter can
# change. That means we may need to remove and re-insert the same view row
# when one of the columns is modified back and forth.
# The following two tests, test_non_primary_key_restrictions_update()
# and test_non_primary_key_restrictions_ttl(), reproduce issue #3430 in two
# ways, and still don't work today, so they are marked xfail. Until #3430 is
# fixed they fail already on the CREATE MATERIALIZED VIEW, which this first
# test checks is refused.
def test_non_primary_key_restrictions_forbidden(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        with pytest.raises(InvalidRequest, match='Non-primary key columns'):
            with new_materialized_view(cql, table, '*', 'a, b',
                    'a is not null and b is not null and c = 1'):
                pass

@pytest.mark.xfail(reason="issue #3430")
def test_non_primary_key_restrictions_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        with new_materialized_view(cql, table, '*', 'a, b',
                'a is not null and b is not null and c = 1') as mv:
            # Insert a base row with c=0, which does not match the filter c=1.
            # The view will have no rows. Then change c from 0 to 1 and see the
            # row appear in the view, change it back to 0 and see it disappear,
            # and change it back to 1 to see it reappear.
            # We have a bug with the last re-appearance (the tombstone continues
            # to shadow the view row we wanted to re-add).
            cql.execute(f"insert into {table} (a, b, c) values (1, 11, 0)")
            assert [] == list(cql.execute(f"select a, b, c from {mv}"))
            cql.execute(f"update {table} set c = 1 where a = 1")
            assert [(1, 11, 1)] == list(cql.execute(f"select a, b, c from {mv}"))
            cql.execute(f"update {table} set c = 0 where a = 1")
            assert [] == list(cql.execute(f"select a, b, c from {mv}"))
            # The bug is here - when we set c = 1 again, we expect to see the
            # view row re-added. And it isn't.
            cql.execute(f"update {table} set c = 1 where a = 1")
            assert [(1, 11, 1)] == list(cql.execute(f"select a, b, c from {mv}"))
    # TODO: when the above tests works, write a similar one just with multiple
    # columns in the in the filter (e.g., c = 1 and d = 1). These columns could
    # be modified with different timestamps, we need to make sure the row
    # deletions and insertions are also timestamped properly.

# This is another reproducer for #3430. While in the above test we updated
# column "c" to remove make it match and un-match the filter, here we use
# a TTL to expire c, and have it un-match the filter.
@pytest.mark.xfail(reason="issue #3430")
def test_non_primary_key_restrictions_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        with new_materialized_view(cql, table, '*', 'a, b',
                'a is not null and b is not null and c = 1') as mv:
            # Insert a base row without c, and set c=1 (matching the filter)
            # with a TTL. The view will then have a row, but it should disappear
            # when the TTL expires.
            # We later re-add c=1, and expect to see the view row appear again.
            cql.execute(f"insert into {table} (a, b, c) values (1, 11, 0)")
            assert [] == list(cql.execute(f"select a, b, c from {mv}"))
            # A TTL is counted in whole seconds, from the start of the second
            # in which the write happened - so a "ttl 1" written late in a
            # second expires almost at once. Wait for the start of the next
            # second, so that the row below is sure to still be alive when we
            # read it, and only expires during the sleep further down.
            t = time.time()
            time.sleep(1 - (t - int(t)))
            cql.execute(f"update {table} using ttl 1 set c = 1 where a = 1")
            assert [(1, 11, 1)] == list(cql.execute(f"select a, b, c from {mv}"))
            # The bug was here: When c expires, we expect to see the view row
            # expire. Instead, the view row remained, and just its c column
            # expired.
            time.sleep(1.2)
            assert [] == list(cql.execute(f"select a, b, c from {mv}"))
            # After the above passes, we also expect to be able to bring the
            # view row back to life by setting c = 1.
            cql.execute(f"update {table} set c = 1 where a = 1")
            assert [(1, 11, 1)] == list(cql.execute(f"select a, b, c from {mv}"))

# In the above two reproducers for #3430, the column c was not part of the
# base table's key (as we explained, this is important) but also wasn't in
# the view's key. In this test, we make c part of the view's key. This makes
# things easier for Scylla, because anyway modifying c (which is part of the
# view key) is expected to add or remove entire rows and we have mechanisms
# to deal with that (properly timestamped shadowable tombstones). The
# following two tests with the "vk" (view key) suffix worked. Let's make sure
# it continues to work.
# Only the first of the two is here; its twin,
# test_non_primary_key_restrictions_ttl_vk, needs to expire a TTL and so
# stayed in view_schema_test.cc - see the comment there.
# This test is scylla_only for the same reason as
# test_non_primary_key_restrictions above - Cassandra doesn't allow
# restricting c, which isn't a base key column.
def test_non_primary_key_restrictions_update_vk(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'a int, c int, primary key (a)') as table:
        with new_materialized_view(cql, table, '*', 'a, c',
                'a is not null and c is not null and c = 1') as mv:
            # Insert a base row with c=0, which does not match the filter c=1.
            # The view will have no rows. Then change c from 0 to 1 and see the
            # row appear in the view, change it back to 0 and see it disappear,
            # and change it back to 1 to see it reappear.
            cql.execute(f"insert into {table} (a, c) values (1, 0)")
            assert [] == list(cql.execute(f"select a, c from {mv}"))
            cql.execute(f"update {table} set c = 1 where a = 1")
            assert [(1, 1)] == list(cql.execute(f"select a, c from {mv}"))
            cql.execute(f"update {table} set c = 0 where a = 1")
            assert [] == list(cql.execute(f"select a, c from {mv}"))
            cql.execute(f"update {table} set c = 1 where a = 1")
            assert [(1, 1)] == list(cql.execute(f"select a, c from {mv}"))


# Test reproducing https://issues.apache.org/jira/browse/CASSANDRA-10910
#
# Cassandra has two regression tests for that issue, differing only in whether
# the view filters on c: ViewTimesTest.testRegularColumnTimestampUpdates,
# which test_regular_column_timestamp_updates above corresponds to, and
# ViewFiltering2Test.testRestrictedRegularColumnTimestampUpdates, which this
# one does. So it may look odd that a test for a Cassandra bug is scylla_only.
# The reason is that restricting c, which isn't a base key column, needs
# Cassandra's unsafe cassandra.mv_allow_filtering_nonkey_columns_unsafe
# property, which its own ViewFiltering tests switch on for themselves and
# which we don't set - see test_non_primary_key_restrictions above.
def test_restricted_regular_column_timestamp_updates(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'k int primary key, c int, val int') as table:
        with new_materialized_view(cql, table, '*', 'k, c',
                'k is not null and c is not null and c = 1') as mv:
            cql.execute(f"update {table} using timestamp 1 set c = 0, val = 0 where k = 0")
            cql.execute(f"update {table} using timestamp 3 set c = 1 where k = 0")
            cql.execute(f"update {table} using timestamp 2 set val = 1 where k = 0")
            cql.execute(f"update {table} using timestamp 4 set c = 1 where k = 0")
            cql.execute(f"update {table} using timestamp 3 set val = 2 where k = 0")
            assert [(1, 0, 2)] == list(cql.execute(f"select c, k, val from {mv}"))

# A write with a timestamp older than the base row's is ignored, and so must
# leave the view alone - while a newer one moves the view row to the partition
# of its new value. Same as test_old_timestamps above, but here the view's
# partition key is a regular column of the base table rather than a key one.
def test_old_timestamps_with_restrictions(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, val text, primary key (k, c)') as table:
        with new_materialized_view(cql, table, '*', 'val, k, c',
                'k is not null and c is not null and val is not null') as mv:
            for i in range(100):
                cql.execute(f"insert into {table} (k, c, val) values (0, {i % 2}, 'baz') using timestamp 300")
            assert 2 == len(list(cql.execute(f"select * from {mv}")))
            assert [(0,), (1,)] == list(cql.execute(f"select c from {mv} where val = 'baz'"))

            # Make sure an old TS does nothing
            cql.execute(f"update {table} using timestamp 100 set val = 'bar' where k = 0 and c = 1")
            assert [(0,), (1,)] == list(cql.execute(f"select c from {mv} where val = 'baz'"))
            assert [] == list(cql.execute(f"select c from {mv} where val = 'bar'"))

            # Latest TS
            cql.execute(f"update {table} using timestamp 500 set val = 'bar' where k = 0 and c = 1")
            assert [(0,)] == list(cql.execute(f"select c from {mv} where val = 'baz'"))
            assert [(1,)] == list(cql.execute(f"select c from {mv} where val = 'bar'"))

# Like test_complex_timestamp_updates above - a long sequence of writes with
# explicit, out-of-order timestamps, checking after each step that the view
# agrees with the base table - but exercising a different set of steps, and
# again run both with and without a flush after each one.
#
# As in that test, the flush only pushes the following reads down to the
# sstables if the row cache is off, which on Scylla means saying so per table.
#
# Despite the "restricted" in its name, this test's view restricts nothing
# beyond IS NOT NULL, so unlike the other "restricted" tests above it runs on
# Cassandra too and needs no scylla_only.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_complex_restricted_timestamp_update(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, v3 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'v1, p, c',
                'p is not null and c is not null and v1 is not null', extra=no_cache) as mv:
            # Set initial values TS=0, matching the restriction and verify view
            cql.execute(f"insert into {table} (p, c, v1, v2) values (0, 0, 1, 0) using timestamp 0")
            assert [(1, 0, 0, 0, None)] == list(cql.execute(f"select * from {mv}"))

            # Update v1's timestamp TS=2
            cql.execute(f"update {table} using timestamp 2 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v1 @ TS=3, tombstones v1=1 and tries to add v1=0 partition
            cql.execute(f"update {table} using timestamp 3 set v1 = 0 where p = 0 and c = 0")
            maybe_flush()
            assert 1 == len(list(cql.execute(f"select v2 from {mv} where v1 = 0 and p = 0 and c = 0")))

            # Update v1 back to 1 with TS=4
            cql.execute(f"update {table} using timestamp 4 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0, None)] == list(cql.execute(f"select v2, v3 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Add v3 @ TS=1
            cql.execute(f"update {table} using timestamp 1 set v3 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(0, 1)] == list(cql.execute(f"select v2, v3 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v2 @ TS=2
            cql.execute(f"update {table} using timestamp 2 set v2 = 2 where p = 0 and c = 0")
            maybe_flush()
            assert [(2,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Update v2 @ TS=3
            cql.execute(f"update {table} using timestamp 3 set v2 = 1 where p = 0 and c = 0")
            maybe_flush()
            assert [(1,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # Tombstone v1
            cql.execute(f"delete from {table} using timestamp 5 where p = 0 and c = 0")
            assert [] == list(cql.execute(f"select v2 from {mv}"))

            # Add the row back without v2
            cql.execute(f"insert into {table} (p, c, v1) values (0, 0, 1) using timestamp 6")
            # Make sure v2 doesn't pop back in.
            assert [(None,)] == list(cql.execute(f"select v2 from {mv} where v1 = 1 and p = 0 and c = 0"))

            # New partition
            # Insert a row @ TS=0
            cql.execute(f"insert into {table} (p, c, v1, v2, v3) values (1, 0, 1, 0, 0) using timestamp 0")

            # Overwrite PK, v1 and v3 @ TS=1, but don't overwrite v2
            cql.execute(f"insert into {table} (p, c, v1, v3) values (1, 0, 1, 0) using timestamp 1")

            # Delete @ TS=0 (which should only delete v2)
            cql.execute(f"delete from {table} using timestamp 0 where p = 1 and c = 0")
            assert [(1, 1, 0, None, 0)] == list(cql.execute(f"select * from {mv} where v1 = 1 and p = 1 and c = 0"))

            cql.execute(f"update {table} using timestamp 2 set v1 = 1 where p = 1 and c = 1")
            maybe_flush()
            cql.execute(f"update {table} using timestamp 3 set v1 = 1 where p = 1 and c = 0")
            maybe_flush()
            assert [(1, 1, 0, None, 0)] == list(cql.execute(f"select * from {mv} where v1 = 1 and p = 1 and c = 0"))

            cql.execute(f"update {table} using timestamp 3 set v2 = 0 where p = 1 and c = 0")
            maybe_flush()
            assert [(1, 1, 0, 0, 0)] == list(cql.execute(f"select * from {mv} where v1 = 1 and p = 1 and c = 0"))

# Two tests for how a base row's deletion, and its resurrection by later
# writes, reach the view - one where the view's key is made only of base key
# columns, and one where the view's partition key is a regular base column.
# Each is run both with and without a flush after every step; as in the other
# flushing tests, the flush only means anything with the row cache off.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_complex_timestamp_with_base_pk_columns_in_view_pk_deletion(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            # Set initial values TS=1
            cql.execute(f"insert into {table} (p, c, v1, v2) values (1, 2, 3, 4) using timestamp 1")
            maybe_flush()
            assert [(3, 4, 1)] == list(cql.execute(
                f"select v1, v2, WRITETIME(v2) from {mv} where p = 1 and c = 2"))

            # Delete row TS=2
            cql.execute(f"delete from {table} using timestamp 2 where p = 1 and c = 2")
            maybe_flush()
            assert [] == list(cql.execute(f"select * from {mv}"))

            # Add PK @ TS=3
            cql.execute(f"insert into {table} (p, c) values (1, 2) using timestamp 3")
            maybe_flush()
            assert [(2, 1, None, None)] == list(cql.execute(f"select * from {mv}"))

            # Reset values TS=10
            cql.execute(f"insert into {table} (p, c, v1, v2) values (1, 2, 3, 4) using timestamp 10")
            maybe_flush()
            assert [(3, 4, 10)] == list(cql.execute(
                f"select v1, v2, WRITETIME(v2) from {mv} where p = 1 and c = 2"))

            # Update values TS=20
            cql.execute(f"update {table} using timestamp 20 set v2 = 5 where p = 1 and c = 2")
            maybe_flush()
            assert [(3, 5, 20)] == list(cql.execute(
                f"select v1, v2, WRITETIME(v2) from {mv} where p = 1 and c = 2"))

            # Delete row TS=10
            cql.execute(f"delete from {table} using timestamp 10 where p = 1 and c = 2")
            maybe_flush()
            assert [(None, 5, 20)] == list(cql.execute(f"select v1, v2, WRITETIME(v2) from {mv}"))

