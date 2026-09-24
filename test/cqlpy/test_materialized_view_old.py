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
from decimal import Decimal
from uuid import UUID
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.util import Time

from .util import new_test_table, new_type, new_materialized_view, unique_name

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
