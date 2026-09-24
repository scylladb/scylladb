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

from test.pylib.skip_types import skip_env

from . import nodetool
from .test_materialized_view import wait_for_view_built
from .util import new_test_table, new_type, new_materialized_view, unique_name, is_scylla, ScyllaMetrics

# The "clock" fixture lets a test move the server's clock forward, instead of
# really waiting - so a test can make a TTL expire, or data become expired,
# without taking seconds to run. It is the Python counterpart of the C++
# tests' forward_jump_clocks(), and like it, clock.jump(n) is cumulative and
# stays in effect until the end of the test.
#
# How it does it depends on what we are testing:
#  * On Scylla, it sets the test-only "test_clocks_offset_seconds"
#    configuration option through the system.config virtual table. This is
#    instantaneous.
#  * On a Scylla release build the option doesn't exist - it is only compiled
#    in builds which enable error injection - so the test is skipped.
#  * On Cassandra, which has no such knob, it really sleeps. The test is then
#    slow, but still tests the same thing, which is what lets us keep running
#    these tests on Cassandra.
#
# The offset is reset when the test ends, so a test run against a shared or
# pre-existing server leaves that server's clock as it found it. Two things
# are worth knowing about that reset:
#
#  1. Moving the offset back makes the timestamps which the *server* generates
#     go backwards for as long as the jump was, so anything the server wrote
#     while the clock was forward has a future timestamp and can shadow a
#     later write to the same row. The writes a test itself makes are not a
#     problem - the Python driver timestamps them on the client side, so the
#     server's offset doesn't reach them - but schema changes are written by
#     the server. Creating or dropping a table while the clock is moved
#     forward is still safe here, because each test uses its own unique table
#     name and so its own schema rows, but a test which repeatedly recreates
#     the *same* name should not use this fixture.
#  2. Updating system.config only affects the node we are connected to, so
#     this only works on a single-node cluster - which is what cqlpy tests.
#
# TODO: This fixture is useful beyond materialized views, so it should
# eventually move to util.py.
OFFSET_CONFIG = 'test_clocks_offset_seconds'

class Clock:
    # A TTL which is long enough that nothing expires until the test jumps the
    # clock past it on purpose. Many of the tests below just need "a long TTL"
    # and then jump ttl+1 seconds to expire it - the actual number is
    # meaningless to them. On Scylla we can afford to keep the original number
    # from the C++ tests, because jumping the clock is free.
    ttl = 100

    # On Cassandra the jump is a real sleep, so the number has to be small -
    # but not too small, because the test has to get from the write to the
    # check before the TTL runs out on its own. What makes that slow is
    # nodetool: against Cassandra it is an external Java program, and each
    # flush spends about a second starting a JVM (measured: 0.96-1.21s). The
    # tightest tests here flush twice between the TTL'd write and the check
    # that the row is still alive, so roughly 2.3 seconds pass before it runs.
    # A TTL of 3 left too little room for that and made those tests flaky;
    # 8 leaves a margin of well over 5 seconds.
    ttl_cassandra = 8

    def __init__(self, cql):
        self._cql = cql
        self._jumped = 0
        self._original = None
        if not is_scylla(cql):
            self.ttl = self.ttl_cassandra
            return
        row = cql.execute(f"SELECT value FROM system.config WHERE name = '{OFFSET_CONFIG}'").one()
        if row is None:
            skip_env(f"Scylla is missing the {OFFSET_CONFIG} option - "
                     "try compiling in dev/debug/sanitize mode")
        self._original = int(row.value)

    # Move the server's clock "seconds" seconds forward, cumulatively.
    def jump(self, seconds):
        self._jumped += seconds
        if self._original is None:
            time.sleep(seconds)
        else:
            self._set(self._original + self._jumped)

    def _set(self, offset):
        self._cql.execute("UPDATE system.config SET value = %s WHERE name = %s",
                          (str(offset), OFFSET_CONFIG))

    def _restore(self):
        if self._original is not None and self._jumped:
            self._set(self._original)

@pytest.fixture(scope="function")
def clock(cql):
    c = Clock(cql)
    yield c
    c._restore()

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
# it, or depends on its liveness. See issue #4448 and
# test_mv_allow_some_column_drops below, which covers the rule in more
# detail.
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

# A TTL on a base row is carried over to the view row, and when it expires
# the view row goes away with it. A TTL on an individual column only takes
# that column's value away, and if the row itself is still alive - because
# some other column, or the row marker, is - the view row stays, with a null
# in place of the expired column.
def test_ttl(cql, test_keyspace, clock):
    schema = 'p int, c int, v1 int, v2 int, v3 int, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, 'p, c, v1, v2', 'v1, c, p',
                'p is not null and c is not null and v1 is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v1, v2, v3) values (0, 0, 0, 0, 0) using ttl 3")
            assert 1 == len(list(cql.execute(f"select * from {mv}")))
            clock.jump(4)
            assert 0 == len(list(cql.execute(f"select * from {mv}")))

            cql.execute(f"insert into {table} (p, c, v1, v2, v3) values (1, 1, 1, 1, 1) using ttl 3")
            clock.jump(1)
            assert [(1,)] == list(cql.execute(f"select v2 from {mv}"))

            # Rewrite the row without a TTL. This resurrects the row - it now
            # has a row marker which never expires - but doesn't rewrite v2,
            # which keeps the TTL it was given above and expires on schedule.
            cql.execute(f"insert into {table} (p, c, v1) values (1, 1, 1)")
            clock.jump(4)
            assert [(None,)] == list(cql.execute(f"select v2 from {mv}"))

            cql.execute(f"insert into {table} (p, c, v1, v2, v3) values (2, 2, 2, 2, 2) using ttl 3")
            assert 1 == len(list(cql.execute(f"select * from {mv} where v1 = 2")))
            clock.jump(2)
            # v3 is not selected by the view, so giving it a longer TTL keeps
            # the *base* row alive past the expiry of everything else - but
            # the view row, whose columns have all expired, still goes away.
            cql.execute(f"update {table} using ttl 8 set v3 = 4 where p = 2 and c = 2")
            clock.jump(2)
            assert [] == list(cql.execute(f"select * from {mv} where v1 = 2"))
            assert [(2, 2, None, None, 4)] == list(cql.execute(f"select * from {table} where p = 2 and c = 2"))

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

# The twin of the test above: instead of changing c away from the value the
# view filters on, let c expire.
# This test is scylla_only for the same reason as the test above.
def test_non_primary_key_restrictions_ttl_vk(cql, test_keyspace, scylla_only, clock):
    with new_test_table(cql, test_keyspace, 'a int, c int, primary key (a)') as table:
        with new_materialized_view(cql, table, '*', 'a, c',
                'a is not null and c is not null and c = 1') as mv:
            # Insert a base row without c, and set c=1 (matching the filter)
            # with a TTL. The view will then have a row, but it should disappear
            # when the TTL expires.
            # We later re-add c=1, and expect to see the view row appear again.
            cql.execute(f"insert into {table} (a, c) values (1, 0)")
            assert [] == list(cql.execute(f"select a, c from {mv}"))
            cql.execute(f"update {table} using ttl 5 set c = 1 where a = 1")
            assert [(1, 1)] == list(cql.execute(f"select a, c from {mv}"))
            clock.jump(6)
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

@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_complex_timestamp_with_base_non_pk_columns_in_view_pk_deletion(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int primary key, v1 int, v2 int',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'v1, p',
                'p is not null and v1 is not null', extra=no_cache) as mv:
            # Set initial values TS=1
            cql.execute(f"insert into {table} (p, v1, v2) values (3, 1, 5) using timestamp 1")
            maybe_flush()
            assert [(5, 1)] == list(cql.execute(
                f"select v2, WRITETIME(v2) from {mv} where v1 = 1 and p = 3"))

            # Delete row TS=2
            cql.execute(f"delete from {table} using timestamp 2 where p = 3")
            maybe_flush()
            assert [] == list(cql.execute(f"select * from {mv}"))

            # Add PK @ TS=3
            cql.execute(f"insert into {table} (p, v1) values (3, 1) using timestamp 3")
            maybe_flush()
            assert [(1, 3, None)] == list(cql.execute(f"select * from {mv}"))

            # Insert v2 @ TS=2
            cql.execute(f"insert into {table} (p, v1, v2) values (3, 1, 4) using timestamp 2")
            maybe_flush()
            assert [(1, 3, None)] == list(cql.execute(f"select * from {mv}"))

            # Insert v2 @ TS=3
            cql.execute(f"update {table} using timestamp 3 set v2 = 4 where p = 3")
            maybe_flush()
            assert [(1, 3, 4, 3)] == list(cql.execute(
                f"select v1, p, v2, WRITETIME(v2) from {mv}"))

# Test that we are not allowed to create a view without the "is not null"
# restrictions on all the view's primary key columns.
# We want to be sure that in every case, the error is caught when creating
# the view - not later when adding data to the base table, as we discovered
# was happening in some cases in issue #2628.
#
# Scylla currently makes one exception, and lets the IS NOT NULL be omitted
# for a column which is the base's only partition key column - the reasoning
# being that a partition key can never be null. Cassandra requires it even
# there, and that disagreement is issue #11979, checked by
# test_is_not_null_requirement() in test_materialized_view.py. This test
# therefore sticks to the cases the two agree on.
def test_is_not_null(cql, test_keyspace):
    def check(table, pk, good, bad):
        for where in good:
            with new_materialized_view(cql, table, '*', pk, where):
                pass
        for where in bad:
            with pytest.raises(InvalidRequest, match='Primary key column.*IS NOT NULL'):
                with new_materialized_view(cql, table, '*', pk, where):
                    pass

    # Test 1: with one partition column in the base table.
    # This should work with the "where v is not null" restriction
    # on the view's new key column, but fail without it.
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int, w int') as table:
        check(table, 'v, p',
              good=['v is not null and p is not null'],
              # should fail, missing restriction on v
              bad=['p is not null'])
        # should fail, missing restriction on v (p is also missing, but
        # as can be seen from the success above, not mandatory).
        with pytest.raises(InvalidRequest, match='Primary key column.*IS NOT NULL'):
            cql.execute(f"create materialized view {test_keyspace}.{unique_name()} as "
                        f"select * from {table} primary key (v, p)")
        # Test adding rows to cf and all views on it which we succeeded adding
        # above. In issue #2628, we saw that the view creation was succeeding
        # above despite the missing "is not null", and then the updates here
        # were failing. This was wrong.
        cql.execute(f"insert into {table} (p, v, w) values (1, 2, 3)")

    # Test 2: where the base table has a composite partition key.
    # It appears (see Cassandra's CreateViewStatement.getColumnIdentifier())
    # that when the partition key is composite (composed of multiple columns)
    # individual columns may be null, so we must have an IS NOT NULL
    # restriction on those (p1 and p2 below) too, and it's no longer optional.
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, v int, primary key ((p1, p2))') as table:
        check(table, 'v, p1, p2',
              good=['p1 is not null and p2 is not null and v is not null'],
              bad=['p2 is not null and v is not null',       # missing p1
                   'p1 is not null and v is not null',       # missing p2
                   'p1 is not null and p2 is not null'])     # missing v
        cql.execute(f"insert into {table} (p1, p2, v) values (1, 2, 3)")

    # Test 3: this time the base has a non-composite partition key p1,
    # and also a clustering key c. The IS NOT NULL is needed on c, and on
    # the new view primary key column - v:
    with new_test_table(cql, test_keyspace, 'p1 int, c int, v int, primary key (p1, c)') as table:
        check(table, 'v, p1, c',
              good=['c is not null and v is not null and p1 is not null'],
              bad=['p1 is not null and v is not null',       # missing c
                   'p1 is not null and c is not null'])      # missing v
        cql.execute(f"insert into {table} (p1, c, v) values (1, 2, 3)")

    # FIXME: we should also test that beyond "IS NOT NULL" being
    # verified on view creation, it also does its job when adding
    # rows - that those with NULL values are properly ignored.

# Test that it is forbidden to add more than one new column to the
# view's primary key beyond what was in the base's primary key.
def test_only_one_allowed(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int, w int') as table:
        with pytest.raises(InvalidRequest, match='Cannot include more than one non-primary key column'):
            with new_materialized_view(cql, table, '*', 'v, w, p',
                    'v is not null and w is not null'):
                pass

# Test that a view cannot be created without its primary key containing all
# columns of the base's primary key. This reproduces issue #2720.
def test_view_key_must_include_base_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        # Adding a column (b) to cf's primary key (a) is fine:
        with new_materialized_view(cql, table, '*', 'b, a',
                'a is not null and b is not null'):
            pass
        # But missing any of cf's primary columns in the view, is not.
        with pytest.raises(InvalidRequest, match='Cannot create [Mm]aterialized [Vv]iew.*without primary key columns'):
            with new_materialized_view(cql, table, '*', 'b', 'b is not null'):
                pass

    # A slightly more elaborate case, which actually reproduces the
    # problem we had issue #2720 - in this case we didn't detect the
    # error of the missing key column.
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a, b)') as table:
        with pytest.raises(InvalidRequest, match='Cannot create [Mm]aterialized [Vv]iew.*without primary key columns'):
            # error: "a" is missing in this key.
            with new_materialized_view(cql, table, '*', 'c, b',
                    'c is not null and b is not null'):
                pass

# Adding columns to the base table must not disturb an existing view, and a
# later update must still reach it.
def test_alter_table_with_updates(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c, v1, v2', 'v1, p, c',
                'p is not null and c is not null and v1 is not null') as mv:
            cql.execute(f"update {table} set v1 = 4, v2 = 5 where p = 1 and c = 1")
            for column in ['f', 'o', 't', 'x', 'z']:
                cql.execute(f"alter table {table} add {column} int")
            cql.execute(f"update {table} set v2 = 7 where p = 1 and c = 1")
            assert [(1, 1, 4, 7)] == list(cql.execute(f"select p, c, v1, v2 from {mv}"))

# Test that a regular column which we did not add to the view is really
# not in the view. Even if to fix issue #3362 we add "virtual cells"
# for the unselected columns, those should not be visible to the end-user
# of the view table.
# Scylla and Cassandra word "there is no such column" differently.
def no_such_column(name):
    return f'Unrecognized name {name}|Undefined column name {name}'

def test_unselected_column(cql, test_keyspace):
    schema = 'p int, c int, x int, y list<int>, z set<int>, w map<int,int>, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, 'p, c', 'c, p',
                'p is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (p, c, x) values (1, 2, 3)")
            assert [(1, 2, None, 3, None, None)] == list(cql.execute(f"select * from {table}"))
            # Check that when we ask for all of vcf's columns, we only get the
            # ones we actually selected - c and p, not x, y, z, or w:
            assert [(2, 1)] == list(cql.execute(f"select * from {mv}"))
            # Check that we cannot explicitly select the x, y, z or w columns in
            # vcf as they are not one of the columns we selected for the view.
            # Check that we also cannot use the x column as a restriction,
            # despite it nominally existing as a virtual column. This
            # reproduces issue #4216: the error for "where x = 0" used to be a
            # confusing complaint about the constant not fitting x's type,
            # instead of simply saying there is no such column.
            # Note that the original C++ test distinguished the two by the C++
            # exception type, which isn't visible over CQL - both arrive as an
            # InvalidRequest - so here we check the message instead.
            for column in ['x', 'y', 'z', 'w']:
                with pytest.raises(InvalidRequest, match=no_such_column(column)):
                    cql.execute(f"select {column} from {mv}")
            # This is a baseline check for the error we should expect
            # when a completely non-existent column name is used.
            with pytest.raises(InvalidRequest, match=no_such_column('nonexistent')):
                cql.execute(f"select * from {mv} where nonexistent = 0")
            with pytest.raises(InvalidRequest, match=no_such_column('x')):
                cql.execute(f"select * from {mv} where x = 0")

# A column of the base table which the view didn't select exists in the view
# as a "virtual column" (see issue #3362), but must stay invisible to the
# user - including to WRITETIME() and TTL(), which must refuse it just as
# they would a name that doesn't exist at all.
def test_hide_ttl_and_writetime_for_virtual_columns(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            'k int, c int, a int, b int, e int, f int, g int, primary key(k, c)') as table:
        with new_materialized_view(cql, table, 'k,c,a,b', 'c, k',
                'k IS NOT NULL AND c IS NOT NULL') as mv1, \
             new_materialized_view(cql, table, 'k,c,a,b', 'c, k, a',
                'k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL') as mv2:
            for mv in [mv1, mv2]:
                for function in ['WRITETIME', 'TTL']:
                    with pytest.raises(InvalidRequest, match=no_such_column('e')):
                        cql.execute(f"SELECT {function}(e) FROM {mv}")

# Whether a view row is alive depends on the liveness of the base row as a
# whole and of each of the columns the view selected - and not at all on the
# columns it didn't select, except that those keep the base row itself alive.
# This walks a single base row through a long series of writes and deletions
# at assorted timestamps, checking the base table and the view after each one.
def test_no_base_column_in_view_pk_complex_timestamp(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace,
            'k int, c int, a int, b int, e int, f int, primary key(k, c)') as table:
        with new_materialized_view(cql, table, 'k,c,a,b', 'c, k',
                'k IS NOT NULL AND c IS NOT NULL') as mv:
            def check(base, view):
                assert base == list(cql.execute(f"SELECT * FROM {table}"))
                assert view == list(cql.execute(f"SELECT * FROM {mv}"))

            # update unselected, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1")
            check([(1, 1, None, None, 1, None)], [(1, 1, None, None)])

            # remove unselected, add selected column, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 2 SET e=null, b=1 WHERE k=1 AND c=1")
            check([(1, 1, None, 1, None, None)], [(1, 1, None, 1)])

            # remove selected column, view row is removed
            cql.execute(f"UPDATE {table} USING TIMESTAMP 2 SET e=null, b=null WHERE k=1 AND c=1")
            check([], [])

            # update unselected with ts=3, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET f=1 WHERE k=1 AND c=1")
            check([(1, 1, None, None, None, 1)], [(1, 1, None, None)])

            # insert livenesssInfo, view row should be alive
            cql.execute(f"INSERT INTO {table}(k,c) VALUES(1,1) USING TIMESTAMP 3")
            check([(1, 1, None, None, None, 1)], [(1, 1, None, None)])

            # remove unselected, view row should be alive because of base livenessInfo alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET f=null WHERE k=1 AND c=1")
            check([(1, 1, None, None, None, None)], [(1, 1, None, None)])

            # add selected column, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET a=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, None)], [(1, 1, 1, None)])

            # update unselected, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 4 SET f=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, 1)], [(1, 1, 1, None)])

            # delete with ts=3, view row should be alive due to unselected@ts4
            cql.execute(f"DELETE FROM {table} USING TIMESTAMP 3 WHERE k=1 AND c=1")
            check([(1, 1, None, None, None, 1)], [(1, 1, None, None)])

            # remove unselected, view row should be removed
            cql.execute(f"UPDATE {table} USING TIMESTAMP 4 SET f=null WHERE k=1 AND c=1")
            check([], [])

            # add selected with ts=7, view row is alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 7 SET b=1 WHERE k=1 AND c=1")
            check([(1, 1, None, 1, None, None)], [(1, 1, None, 1)])

            # remove selected with ts=7, view row is dead
            cql.execute(f"UPDATE {table} USING TIMESTAMP 7 SET b=null WHERE k=1 AND c=1")
            check([], [])

            # add selected with ts=5, view row is alive (selected column should not affects each other)
            cql.execute(f"UPDATE {table} USING TIMESTAMP 5 SET a=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, None)], [(1, 1, 1, None)])

            # add selected with ttl
            cql.execute(f"UPDATE {table} USING TTL {clock.ttl} SET a=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, None)], [(1, 1, 1, None)])

            clock.jump(clock.ttl + 1)
            assert [] == list(cql.execute(f"SELECT * FROM {mv}"))

            # update unselected with ttl, view row should be alive
            cql.execute(f"UPDATE {table} USING TTL {clock.ttl} SET f=1 WHERE k=1 AND c=1")
            check([(1, 1, None, None, None, 1)], [(1, 1, None, None)])

            clock.jump(clock.ttl + 1)
            check([], [])

# The same walk as test_no_base_column_in_view_pk_complex_timestamp above, but
# for a view whose own key includes a base column which is *not* part of the
# base's key - here a. A view row can then only exist while that column is
# alive, which is what makes this case different.
def test_base_column_in_view_pk_complex_timestamp(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace,
            'k int, c int, a int, b int, e int, f int, primary key(k, c)') as table:
        with new_materialized_view(cql, table, 'k, c, a, b', 'k, c, a',
                'k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL') as mv:
            def check(base, view):
                assert base == list(cql.execute(f"SELECT * FROM {table}"))
                assert view == list(cql.execute(f"SELECT * FROM {mv}"))

            # update unselected, view row should not be here
            cql.execute(f"UPDATE {table} USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1")
            assert [] == list(cql.execute(f"SELECT * FROM {mv}"))

            # Set selected, view row should appear
            cql.execute(f"UPDATE {table} USING TIMESTAMP 1 SET a=1, e=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, 1, None)], [(1, 1, 1, None)])

            # remove unselected, add selected column, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 2 SET e=null, b=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, 1, None, None)], [(1, 1, 1, 1)])

            # remove selected column, view row is removed
            cql.execute(f"UPDATE {table} USING TIMESTAMP 2 SET a=null, e=null, b=null WHERE k=1 AND c=1")
            check([], [])

            # update unselected with ts=3, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET a=1, f=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, 1)], [(1, 1, 1, None)])

            # insert livenesssInfo, view row should be alive
            cql.execute(f"INSERT INTO {table}(k,c,a) VALUES(1,1,1) USING TIMESTAMP 3")
            check([(1, 1, 1, None, None, 1)], [(1, 1, 1, None)])

            # remove unselected, view row should be alive because of base livenessInfo alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET a=1, f=null WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, None)], [(1, 1, 1, None)])

            # update unselected, view row should be alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 4 SET a=1, f=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, 1)], [(1, 1, 1, None)])

            # delete with ts=3, view row should be alive due to unselected@ts4
            cql.execute(f"DELETE FROM {table} USING TIMESTAMP 3 WHERE k=1 AND c=1")
            check([(1, 1, 1, None, None, 1)], [(1, 1, 1, None)])

            # remove unselected, view row should be removed
            cql.execute(f"UPDATE {table} USING TIMESTAMP 4 SET a=null, f=null WHERE k=1 AND c=1")
            check([], [])

            # add selected with ts=7, view row is alive
            cql.execute(f"UPDATE {table} USING TIMESTAMP 7 SET a=1, b=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, 1, None, None)], [(1, 1, 1, 1)])

            # remove selected with ts=7, view row is dead
            cql.execute(f"UPDATE {table} USING TIMESTAMP 7 SET a=null, b=null WHERE k=1 AND c=1")
            check([], [])

            # add selected with ttl
            cql.execute(f"UPDATE {table} USING TTL {clock.ttl} SET a=1, b=1 WHERE k=1 AND c=1")
            check([(1, 1, 1, 1, None, None)], [(1, 1, 1, 1)])

            clock.jump(clock.ttl + 1)
            assert [] == list(cql.execute(f"SELECT * FROM {mv}"))

# The test revolves around timestamps in materialized views and their relation
# to timestamps in the base table. Values in an MV should have the same
# timestamp as the corresponding ones in the base table. However, that only
# applies to values that are readable with `WRITETIME`. Those that are not
# readable encompass unselected columns, even if a view has virtual columns that
# correspond to them. Because of that, Scylla employs an optimization that
# prevents emitting redundant view updates -- that's what this test verifies.
# For that end, we use two MVs:
#
# * mv1: its primary key is a permutation of the base table's primary key.
#        Because of that, it will have virtual columns corresponding to
#        unselected columns from the base table. Creating a value in such a
#        column (in the base table) will generate a view update to the MV.
#        However, updating it will not generate an update UNLESS it changes the
#        cell's TTL.
# * mv2: its primary key consists of the columns from the base table's primary
#        key and one regular column. Because of that, the MV will NOT have any
#        virtual columns corresponding to the unselected columns from the base
#        table. As a result, no view updates will be generated for unselected
#        columns as a result.
#
# scylla_only: the optimization is Scylla's own, and counting the view updates
# it did or didn't emit means reading Scylla's metrics - see writes_to() and
# view_updates_generated() below. Nothing else runs while this test does, so the
# node-wide counter's delta counts only what this test caused.
# How many writes a table has taken, from Scylla's per-table metrics.
def writes_to(cql, table):
    ks, cf = table.split('.')
    return int(ScyllaMetrics.query(cql).get(
        'scylla_column_family_write_latency_count', {'ks': ks, 'cf': cf}) or 0)

# How many view updates this node has generated, from Scylla's metrics. This
# counter is node-wide - Scylla does keep a per-table one, but doesn't export
# it - so callers compare it against a baseline of their own.
def view_updates_generated(cql):
    m = ScyllaMetrics.query(cql)
    return sum(int(m.get(f'scylla_database_total_view_updates_pushed_{where}') or 0)
               for where in ['local', 'remote'])

def test_view_update_generating_writetime(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
            'k int, c int, a int, b int, e int, f int, g int, primary key(k, c)') as table:
        with new_materialized_view(cql, table, 'k,c,a,b', 'c, k',
                'k IS NOT NULL AND c IS NOT NULL') as mv1, \
             new_materialized_view(cql, table, 'k,c,a,b', 'c, k, a',
                'k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL') as mv2:
            # Wait for both views to finish being built before counting
            # anything. The base table is empty at this point, so it may look
            # as if there is nothing to build - but the build runs in the
            # background, and if it only gets going once the test has started
            # writing, it copies those rows into the view itself and the counts
            # below come out too high. wait_for_view_built() is how the other
            # cqlpy tests avoid this same race.
            for mv in [mv1, mv2]:
                wait_for_view_built(cql, mv)
            before = view_updates_generated(cql)
            def check(writetime_of, writetime, mv1_updates, mv2_updates, total_updates):
                assert [(writetime,)] == list(cql.execute(f"SELECT WRITETIME({writetime_of}) FROM {table}"))
                assert (mv1_updates, mv2_updates, total_updates) == (
                    writes_to(cql, mv1), writes_to(cql, mv2),
                    view_updates_generated(cql) - before)

            # A view update is generated for mv1 because the row has a complete
            # primary key in that view and we need to mark that the value in the
            # corresponding virtual column is present.
            #
            # A view update is NOT generated for mv2 because the row still has
            # an incomplete primary key in that view (it lacks `a`).
            cql.execute(f"UPDATE {table} USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1")
            check('e', 1, 1, 0, 1)

            # The row still doesn't have a complete PK for mv2.
            #
            # Updating an unselected column will NOT produce a view update, so
            # no update for mv1 either.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 2 SET e=1 WHERE k=1 AND c=1")
            check('e', 2, 1, 0, 1)

            # A view update is generated for mv1 because the `b` column is part
            # of the view.
            #
            # A view update is NOT generated for mv2 because the row still has
            # an incomplete primary key in that view.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 3 SET b=1 WHERE k=1 AND c=1")
            check('b', 3, 2, 0, 2)

            # A view update is generated for mv1 because `a` is part of the
            # view.
            #
            # A view update is generated for mv2 because `a` is part of the view
            # AND the row has finally a complete primary key.
            #
            # The timestamp from the previous CQL statement is preserved for
            # `b`.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 4 SET a=1 WHERE k=1 AND c=1")
            check('b', 3, 3, 1, 4)

            # `f` is an unselected column for both MVs, so a view update will
            # only be generated to mv1 (to the corresponding virtual column)
            # because the value in the cell is only created now.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 5 SET f=40 WHERE k=1 AND c=1")
            check('f', 5, 4, 1, 5)

            # Updating an unselected column will not produce view updates.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 6 SET f=40 WHERE k=1 AND c=1")
            check('f', 6, 4, 1, 5)

            # `g` is an unselected column for both MVs, so a view update will
            # only be generated to mv1 (to the corresponding virtual column)
            # because the value in the cell is only created now.
            cql.execute(f"UPDATE {table} USING TIMESTAMP 7 SET g=40 WHERE k=1 AND c=1")
            check('g', 7, 5, 1, 6)

            # Updating the TTL of an unselected column will produce a view
            # update to the virtual column.
            cql.execute(f"UPDATE {table} USING TTL 300 AND TIMESTAMP 8 SET g=40 WHERE k=1 AND c=1")
            check('g', 8, 6, 1, 7)

# Usually if only an unselected column in the base table is modified, we expect
# an optimization that a view update is not done, but we had an
# bug(https://scylladb.atlassian.net/browse/SCYLLADB-808) where the existence of
# a collection selected in the view caused us to skip this optimization, even
# when it was not modified. This test reproduces this bug.
#
# In this test we verify that we correctly skip (or not) view updates to a view
# that selects a collection column. We use two MVs, similarly as in the test
# above test.
#
# scylla_only for the same reasons as the test above - it counts view updates
# by reading Scylla's metrics, and the optimization is Scylla's own.
def test_view_update_unmodified_collection(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
            'k int, c int, a int, b list<int>, g int, primary key(k, c)') as table:
        with new_materialized_view(cql, table, 'k,c,a,b', 'c, k',
                'k IS NOT NULL AND c IS NOT NULL') as mv1, \
             new_materialized_view(cql, table, 'k,c,a,b', 'c, k, a',
                'k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL') as mv2:
            # Wait for the builds before counting, as in the test above -
            # a build which starts after the test has written rows would copy
            # them into the view and inflate the counts.
            for mv in [mv1, mv2]:
                wait_for_view_built(cql, mv)
            before = view_updates_generated(cql)
            def check(mv1_updates, mv2_updates, total_updates):
                assert (mv1_updates, mv2_updates, total_updates) == (
                    writes_to(cql, mv1), writes_to(cql, mv2),
                    view_updates_generated(cql) - before)

            cql.execute(f"INSERT INTO {table} (k, c, a) VALUES (1, 1, 1)")
            check(1, 1, 2)

            # We update an unselected column and the collection remains NULL, so
            # we should generate an update to the virtual column in mv1 but not
            # to mv2.
            cql.execute(f"UPDATE {table} SET g=1 WHERE k=1 AND c=1")
            check(2, 1, 3)

            # We update the collection with an initial value
            cql.execute(f"UPDATE {table} SET b=[1] WHERE k=1 AND c=1")
            check(3, 2, 5)

            # We update an unselected column again with a non-NULL selected
            # collection. Because the liveness of the updated column is
            # unchanged and no other selected column is updated (in particular,
            # the collection column), we should generate no view updates.
            cql.execute(f"UPDATE {table} SET g=2 WHERE k=1 AND c=1")
            check(3, 2, 5)

# A batch which deletes a base row and also writes a different one must leave
# the view consistent with the base table - here everything the batch touches
# ends up deleted, so both must end up empty.
def test_conflicting_batch(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key(p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v, c, p',
                'p IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL') as mv:
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (0, 0, 0)")
            assert [(0, 0, 0)] == list(cql.execute(f"SELECT * FROM {mv}"))

            cql.execute(f"""begin unlogged batch
                  DELETE FROM {table} WHERE p = 1;
                  INSERT INTO {table} (p, c, v) VALUES (1, 1, 1);
                  DELETE FROM {table} WHERE p = 0 AND c = 0;
                apply batch""")

            assert [] == list(cql.execute(f"SELECT * FROM {table}"))
            assert [] == list(cql.execute(f"SELECT * FROM {mv}"))

# Test whether it is possible to drop columns from a base table which has
# materialized views. This should be allowed, unless one of the views "needs"
# the column, where needs means either this column was selected by the view,
# or is a virtual column (i.e., the *liveness* of this column matters).
# Reproduces issue #4448.
# Because our secondary indexes are also implemented on top of materialized
# views, the ability or inability to drop columns where secondary indexes
# exist also needs to be tested - see the separate test case
# test_secondary_index_allow_some_column_drops() in secondary_index_test.cc.
#
# scylla_only: allowing any of these drops at all is a Scylla extension.
# Cassandra refuses to drop any regular column from a base table which has a
# view - see the comment on test_column_dropped_from_base above.
def test_mv_allow_some_column_drops(cql, test_keyspace, scylla_only):
    needed = 'materialized view .* needs this column'
    # When the view has a new key column that didn't exist in the base,
    # virtual columns aren't needed, so unselected columns aren't needed
    # by the view and may be dropped. Check that the drop is allowed and
    # the view still works properly afterwards.
    with new_test_table(cql, test_keyspace, 'p int primary key, a int, b int, c int') as table:
        with new_materialized_view(cql, table, 'c', 'a, p', 'a is not null') as mv:
            cql.execute(f"insert into {table} (p, a, b, c) VALUES (1, 2, 3, 4)")
            assert [(1, 2, 3, 4)] == list(cql.execute(f"select * from {table}"))
            cql.execute(f"alter table {table} drop b")
            assert [(1, 2, 4)] == list(cql.execute(f"select * from {table}"))
            assert [(2, 1, 4)] == list(cql.execute(f"select * from {mv} where a = 2"))
            # Test that we cannot drop a selected column of a view. Both
            # c and a are selected (one as a new key column, one as a regular
            # column).
            for column in ['c', 'a']:
                with pytest.raises(InvalidRequest, match=needed):
                    cql.execute(f"alter table {table} drop {column}")
            # We also cannot drop a base's primary key column, of course.
            with pytest.raises(InvalidRequest, match='Cannot drop PRIMARY KEY part p'):
                cql.execute(f"alter table {table} drop p")
            # Also cannot drop a non existent column :-)
            with pytest.raises(InvalidRequest, match='Column xyz was not found'):
                cql.execute(f"alter table {table} drop xyz")

    # When a view has the same key columns as the base, virtual columns
    # are added for all unselected columns, because the *liveness* is
    # important for the view rows, even if the value isn't. In this case,
    # we do not allow to drop any base columns.
    with new_test_table(cql, test_keyspace,
            'p int, c int, a int, b int, d int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'd', 'c, p', 'c is not null'):
            for column in ['a', 'b', 'd']:
                with pytest.raises(InvalidRequest, match=needed):
                    cql.execute(f"alter table {table} drop {column}")
            for column in ['p', 'c']:
                with pytest.raises(InvalidRequest, match=f'Cannot drop PRIMARY KEY part {column}'):
                    cql.execute(f"alter table {table} drop {column}")

# A base table with a compound partition key, and views keyed by each of its
# columns in turn, in each position. A view may reorder the base's partition
# key columns, or move one of them into the view's clustering key, or add a
# regular column in front of them - but it may not name the same column twice.
def test_compound_partition_key(cql, test_keyspace):
    # Each view below is the primary key it asks for, the IS NOT NULL
    # restrictions it needs, and whether creating it should be accepted. The
    # rejected ones are exactly those naming a column twice.
    p12 = 'p1 is not null and p2 is not null'
    v12 = 'v is not null and ' + p12
    views = {
        'mv1_p1': ('p1, p1, p2', p12, False),
        'mv1_p2': ('p2, p1', p12, True),
        'mv1_v': ('v, p1, p2', v12, True),
        'mv2_p1': ('p1, p2', p12, True),
        'mv2_p2': ('p2, p2, p1', p12, False),
        'mv2_v': ('v, p2, p1', v12, True),
        'mv3_p1': ('(p1, p1), p2', p12, False),
        'mv3_p2': ('(p2, p1), p2', p12, False),
        'mv3_v': ('(v, p1), p2', v12, True),
    }
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, v int, primary key ((p1, p2))') as table:
        with contextlib.ExitStack() as stack:
            mv = {}
            for name, (pk, where, accepted) in views.items():
                if accepted:
                    mv[name] = stack.enter_context(
                        new_materialized_view(cql, table, '*', pk, where))
                else:
                    with pytest.raises(InvalidRequest, match='Duplicate.*PRIMARY KEY'):
                        with new_materialized_view(cql, table, '*', pk, where):
                            pass

            cql.execute(f"insert into {table} (p1, p2, v) values (0, 2, 5)")
            assert [(0, 5)] == list(cql.execute(f"select p1, v from {mv['mv1_p2']} where p2 = 2"))
            assert [(0, 5)] == list(cql.execute(
                f"select p1, v from {mv['mv2_p1']} where p2 = 2 and p1 = 0"))
            assert [(0,)] == list(cql.execute(f"select p1 from {mv['mv1_v']} where v = 5"))
            assert [(2,)] == list(cql.execute(
                f"select p2 from {mv['mv3_v']} where v = 5 and p1 = 0"))

            # Overwriting v moves the view row of every view keyed by it
            cql.execute(f"insert into {table} (p1, p2, v) values (0, 2, 8)")
            assert [(0, 8)] == list(cql.execute(f"select p1, v from {mv['mv1_p2']} where p2 = 2"))
            assert [(0, 8)] == list(cql.execute(
                f"select p1, v from {mv['mv2_p1']} where p2 = 2 and p1 = 0"))
            assert [] == list(cql.execute(f"select p1 from {mv['mv1_v']} where v = 5"))
            assert [] == list(cql.execute(f"select p2 from {mv['mv3_v']} where v = 5 and p1 = 0"))
            assert [(2,)] == list(cql.execute(
                f"select p2 from {mv['mv3_v']} where v = 8 and p1 = 0"))

# A base table with nothing but a partition key still gets a working view.
def test_partition_key_only_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, primary key ((p1, p2))') as table:
        with new_materialized_view(cql, table, '*', 'p2, p1',
                'p1 is not null and p2 is not null') as mv:
            cql.execute(f"insert into {table} (p1, p2) values (1, 1)")
            assert [(1, 1)] == list(cql.execute(f"select * from {mv}"))

# Deleting a single column of the base row removes it from the view row too -
# and when that column is one the view's partition key is built from, the view
# row goes away entirely.
def test_delete_single_column_in_view_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b)') as table:
        with new_materialized_view(cql, table, '*', 'd, a, b',
                'a is not null and b is not null and d is not null') as mv:
            cql.execute(f"insert into {table} (a, b, c, d) values (0, 0, 0, 0)")
            assert [(0, 0, 0, 0)] == list(cql.execute(f"select a, d, b, c from {mv}"))

            cql.execute(f"delete c from {table} where a = 0 and b = 0")
            assert [(0, 0, 0, None)] == list(cql.execute(f"select a, d, b, c from {mv}"))

            cql.execute(f"delete d from {table} where a = 0 and b = 0")
            assert [] == list(cql.execute(f"select a, d, b from {mv}"))

# A view may restrict one of the base's partition key columns to a value and
# leave the rest unrestricted. Whichever way the view then arranges its own
# key, and whichever way the base table arranges its own, the view must hold
# exactly the base rows matching that value, and follow them through updates
# and through deletions of a row or of a whole partition.
@pytest.mark.parametrize("base_pk", ['(a, b), c', 'a, b, c'],
                         ids=['compound_base_pk', 'simple_base_pk'])
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_partition_key_filtering_unrestricted_part(cql, test_keyspace, base_pk, pk):
    with new_test_table(cql, test_keyspace,
            f'a int, b int, c int, d int, primary key ({base_pk})') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a = 1 and b is not null and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a, b, c in [(0, 0, 0), (0, 1, 0), (1, 0, 0),
                            (1, 0, 1), (1, 1, 0), (1, 1, 1)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(1, 0, 0, 0), (1, 0, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 0 and b = 0 and c = 0")
            check([(1, 0, 0, 0), (1, 0, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 1 and b = 1 and c = 0")
            check([(1, 0, 0, 0), (1, 0, 1, 0), (1, 1, 0, 1), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(1, 0, 0, 0), (1, 0, 1, 0), (1, 1, 0, 1), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(1, 0, 0, 0), (1, 0, 1, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 0")
            check([(1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1")
            check([])

# Ordinary SELECT queries limit the ability to query for slices (ranges) of
# partition keys, because there is no way to implement such queries
# efficiently. Nor may a clustering column be restricted once an earlier one
# was restricted by a non-EQ relation. This test verifies those limits, which
# the test below it contrasts with what a view's SELECT is allowed to do.
def test_partition_key_slice_not_allowed_in_select(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, primary key (a)') as table:
        # The two databases explain this differently, but both end by
        # suggesting ALLOW FILTERING.
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"select * from {table} where a > 0")
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a, b, c)') as table:
        with pytest.raises(InvalidRequest, match='cannot be restricted'):
            cql.execute(f"select * from {table} where a = 1 and b > 0 and c > 0")
        with pytest.raises(InvalidRequest, match='cannot be restricted'):
            cql.execute(f"select * from {table} where a = 1 and c = 1 and b > 0")

# Test that although normal SELECT queries limit the ability to query
# for slices (ranges) of partition keys, because there is no way to implement
# such queries efficiently, the same slices *do* work for the SELECT statement
# defining a materialized view - there the condition is tested for each
# partition separately, so the performance concerns do not hold.
# This verifies part of issue #2367. See also test_clustering_key_in_restrictions.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_partition_key_filtering_with_slice(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key ((a, b), c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a > 0 and b > 5 and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a, b, c, d in [(0, 0, 1, 1), (0, 10, 1, 2), (1, 0, 2, 1),
                               (1, 10, 2, 2), (2, 1, 3, 1), (2, 10, 3, 2)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, {d})")
            check([(1, 10, 2, 2), (2, 10, 3, 2)])

            cql.execute(f"insert into {table} (a, b, c, d) values (3, 10, 4, 2)")
            check([(1, 10, 2, 2), (2, 10, 3, 2), (3, 10, 4, 2)])

            # A row which the slices filter out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 0 and b = 0 and c = 0")
            check([(1, 10, 2, 2), (2, 10, 3, 2), (3, 10, 4, 2)])

            cql.execute(f"update {table} set d = 100 where a = 3 and b = 10 and c = 4")
            check([(1, 10, 2, 2), (2, 10, 3, 2), (3, 10, 4, 100)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(1, 10, 2, 2), (2, 10, 3, 2), (3, 10, 4, 100)])

            cql.execute(f"delete from {table} where a = 3 and b = 10 and c = 4")
            check([(1, 10, 2, 2), (2, 10, 3, 2)])

# A view may restrict both of the base's partition key columns to values, and
# then holds only the rows of that one base partition.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_partition_key_compound_restrictions(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key ((a, b), c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a = 1 and b = 1 and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a in [0, 1]:
                for b in [0, 1]:
                    for c in [0, 1]:
                        cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(1, 1, 0, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 1 and b = 1 and c = 0")
            check([(1, 1, 0, 1), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 0 and c = 0")
            check([(1, 1, 0, 1), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1")
            check([])

# The same as test_partition_key_compound_restrictions above, but the view
# doesn't select d - so updating d changes nothing in the view, while the
# deletions still do.
def test_partition_key_restrictions_not_include_all(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key ((a, b), c)') as table:
        with new_materialized_view(cql, table, 'a, b, c', '(a, b), c',
                'a = 1 and b = 1 and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c from {mv}"))
            for a in [0, 1]:
                for b in [0, 1]:
                    for c in [0, 1]:
                        cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(1, 1, 0), (1, 1, 1)])

            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(1, 1, 0), (1, 1, 1)])

            cql.execute(f"update {table} set d = 1 where a = 1 and b = 1 and c = 0")
            check([(1, 1, 0), (1, 1, 1)])

            cql.execute(f"delete from {table} where a = 1 and b = 0 and c = 0")
            check([(1, 1, 0), (1, 1, 1)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(1, 1, 1)])

            cql.execute(f"delete from {table} where a = 1 and b = 1")
            check([])

# A view may restrict a base partition key column and a base clustering key
# column at the same time, leaving a third key column unrestricted.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_partition_key_and_clustering_key_filtering_restrictions(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a = 1 and b is not null and c = 1') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a, b, c in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0),
                            (1, 0, 1), (1, 1, -1), (1, 1, 0), (1, 1, 1)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(1, 0, 1, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 0 and b = 0 and c = 0")
            check([(1, 0, 1, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 1 and b = 1 and c = 1")
            check([(1, 0, 1, 0), (1, 1, 1, 1)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(1, 0, 1, 0), (1, 1, 1, 1)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 1")
            check([(1, 0, 1, 0)])

            # Deleting the whole base partition removes what is left
            cql.execute(f"delete from {table} where a = 1")
            check([])

# A base column may be empty - an empty string is a perfectly good value, as
# opposed to null - and a view whose key is built from such columns must still
# hold the row. Note that these views are created after the row already
# exists, so each has to be built before it can be read.
@pytest.mark.parametrize("pk", ['p1, v, c, p2', '(p2, v), c, p1', '(v, p2), c, p1',
                                '(c, v), p1, p2', '(v, c), p1, p2'])
def test_base_non_pk_columns_in_view_partition_key_are_non_empty(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace,
            'p1 int, p2 text, c text, v text, primary key ((p1, p2), c)') as table:
        cql.execute(f"insert into {table} (p1, p2, c, v) values (1, '', '', '')")
        with new_materialized_view(cql, table, '*', pk,
                'p1 is not null and p2 is not null and c is not null and v is not null') as mv:
            wait_for_view_built(cql, mv)
            assert [(1, '', '', '')] == list(cql.execute(f"select p1, p2, c, v from {mv}"))

# The clustering-key counterpart of
# test_delete_single_column_in_view_partition_key above: deleting the base
# column which the view's clustering key is built from removes the view row.
def test_delete_single_column_in_view_clustering_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b)') as table:
        with new_materialized_view(cql, table, '*', 'a, d, b',
                'a is not null and b is not null and d is not null') as mv:
            cql.execute(f"insert into {table} (a, b, c, d) values (0, 0, 0, 0)")
            assert [(0, 0, 0, 0)] == list(cql.execute(f"select a, d, b, c from {mv}"))

            cql.execute(f"delete c from {table} where a = 0 and b = 0")
            assert [(0, 0, 0, None)] == list(cql.execute(f"select a, d, b, c from {mv}"))

            cql.execute(f"delete d from {table} where a = 0 and b = 0")
            assert [] == list(cql.execute(f"select a, d, b from {mv}"))

# A view may restrict a base clustering key column to a value, and then holds
# only the base rows having it.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_clustering_key_eq_restrictions(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a is not null and b = 1 and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a in [0, 1]:
                for b in [0, 1]:
                    for c in [0, 1]:
                        cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 0 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 1, 0)])

            # A deletion of a clustering prefix, in several base partitions
            cql.execute(f"delete from {table} where a in (0, 1) and b = 1")
            check([])

# The same, for a view restricting a base clustering key column to several
# values - either as a range, or as a list of the values themselves. For this
# data the two pick out the same rows, so both are checked the same way.
# The "IN" case is another test for issue #2367.
@pytest.mark.parametrize("restriction", ['b >= 1', 'b IN (1, 2)'], ids=['slice', 'in'])
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_clustering_key_slice_or_in_restrictions(cql, test_keyspace, pk, restriction):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                f'a is not null and {restriction} and c is not null') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a, b, c in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1),
                            (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 2, 1)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 2, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 2, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 0 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 2, 1, 0)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 2, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 2, 1, 0)])

            # A deletion of a clustering range, in several base partitions
            cql.execute(f"delete from {table} where a in (0, 1) and b >= 1 and b <= 4")
            check([])

# The same, for a view whose restriction is on a tuple of two of the base's
# clustering key columns at once, rather than on a single column.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_clustering_key_multi_column_restrictions(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a is not null and (b, c) >= (1, 0)') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            # Note that (1, 1, -1) is left out of the view: its (b, c) of
            # (1, -1) sorts before the (1, 0) the view asks for.
            for a, b, c in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0),
                            (1, 0, 1), (1, 1, -1), (1, 1, 0), (1, 1, 1)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(0, 1, 0, 0), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 0 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 0, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 0")
            check([(0, 1, 0, 1), (0, 1, 1, 0), (1, 1, 1, 0)])

            # Deleting both base partitions empties the view
            cql.execute(f"delete from {table} where a in (0, 1)")
            check([])

# The same, for a view restricting the *last* of the base's clustering key
# columns, leaving the one before it unrestricted.
@pytest.mark.parametrize("pk", ['(a, b), c', '(b, a), c', 'a, b, c', 'c, b, a', '(c, a), b'])
def test_clustering_key_filtering_restrictions(cql, test_keyspace, pk):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c)') as table:
        with new_materialized_view(cql, table, '*', pk,
                'a is not null and b is not null and c = 1') as mv:
            def check(expected):
                assert sorted(expected) == sorted(cql.execute(f"select a, b, c, d from {mv}"))
            for a, b, c in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0),
                            (1, 0, 1), (1, 1, -1), (1, 1, 0), (1, 1, 1)]:
                cql.execute(f"insert into {table} (a, b, c, d) values ({a}, {b}, {c}, 0)")
            check([(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0), (1, 1, 1, 0)])

            # A row which the view filters out stays filtered out
            cql.execute(f"update {table} set d = 1 where a = 1 and b = 0 and c = 0")
            check([(0, 0, 1, 0), (0, 1, 1, 0), (1, 0, 1, 0), (1, 1, 1, 0)])

            cql.execute(f"update {table} set d = 1 where a = 0 and b = 1 and c = 1")
            check([(0, 0, 1, 0), (0, 1, 1, 1), (1, 0, 1, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 0 and b = 0 and c = 0")
            check([(0, 0, 1, 0), (0, 1, 1, 1), (1, 0, 1, 0), (1, 1, 1, 0)])

            cql.execute(f"delete from {table} where a = 1 and b = 1 and c = 1")
            check([(0, 0, 1, 0), (0, 1, 1, 1), (1, 0, 1, 0)])

            # Deleting both base partitions empties the view
            cql.execute(f"delete from {table} where a in (0, 1)")
            check([])

# This test reproduces issue #4340 - creating a materialized view without
# any clustering key used to fail. In this trivial example, we have a base
# table with no clustering key, and the view just keeps the same primary key.
# This should work.
def test_no_clustering_key_1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        # Before #4340 was fixed, we couldn't even create the following view:
        with new_materialized_view(cql, table, 'a, b', 'a', 'a is not null') as mv:
            # Let's check that after fixing #4340, we can not only create the
            # view, it also works as expected:
            cql.execute(f"insert into {table} (a, b, c) values (1, 2, 3)")
            assert [(1, 2, 3)] == list(cql.execute(f"select * from {table}"))
            assert [(1, 2)] == list(cql.execute(f"select * from {mv}"))

# This is a second reproducer for issue #4340. Here we create a more useful
# materialized view than the trivial one in the previous test, but in the
# view, put all key columns as partition keys, none of them in clustering
# keys. There is no reason why this shouldn't be allowed.
def test_no_clustering_key_2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, primary key (a)') as table:
        # Before #4340 was fixed, we couldn't even create the following view:
        with new_materialized_view(cql, table, 'a, b', '(a, b)',
                'a is not null and b is not null') as mv:
            # Let's check that after fixing #4340, we can not only create the
            # view, it also works as expected:
            cql.execute(f"insert into {table} (a, b, c) values (1, 2, 3)")
            assert [(1, 2, 3)] == list(cql.execute(f"select * from {table}"))
            assert [(1, 2)] == list(cql.execute(f"select * from {mv} where a = 1 and b = 2"))

# This test checks various cases where a base table row disappears - or does
# not disappear - when its last column is deleted (with DELETE or by setting
# it to null). We want to confirm that the view row disappears - or does not
# disappear - accordingly. This reproduces
# https://issues.apache.org/jira/browse/CASSANDRA-14393
#
# cassandra_bug: on Cassandra the third step below leaves the view empty while
# the base table still holds the row - the view disagrees with its own base
# table, which cannot be right. The reason is that this view selects neither a
# nor b, and deciding whether the view row should live then means knowing
# whether any unselected base column is still alive. Scylla tracks that with
# "virtual columns", added for issue #3362 (see test_unselected_column above
# and the test_3362_* tests below). Cassandra has no such thing, and adding it
# is exactly what the still-open CASSANDRA-13826, "Specialize row structure to
# support complex Materialized Views liveness", proposes.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_partial_delete_unselected_column(cql, test_keyspace, flush, cassandra_bug):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, a int, b int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            cql.execute(f"update {table} using timestamp 10 set b = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1)])

            cql.execute(f"delete b from {table} using timestamp 11 where p = 1 and c = 1")
            # Because above we used "update" to insert the b=1 cell, a so-called
            # row-marker is not added, and when we delete this cell, all trace of
            # this row disappears from the base table. Accordingly, it should
            # disappear from the view as well:
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 1 set a = 1 where p = 1 and c = 1")
            # Above we deleted only the "b" cell, not the entire row, so when we add
            # "a" with an earlier timestamp, it is not shadowed by the deletion, and
            # we have a row in the base table (and accordingly, in the view).
            maybe_flush()
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 18 set a = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1)])

            # This tests the same thing as the "DELETE" test above (deleting the only
            # cell causes no trace of the row to remain, and the row disappears from
            # the view as well) - it's just that we delete the cell by setting it to
            # "null" instead of using the "DELETE" command. See also
            # https://issues.apache.org/jira/browse/CASSANDRA-11805.
            cql.execute(f"update {table} using timestamp 20 set a = null where p = 1 and c = 1")
            maybe_flush()
            check([])

            # We now insert a row to the base table. It's without values for the
            # non-key columns, but the row nevertheless exists (this is implemented
            # via a "row marker"). None of the updates we did above with higher
            # timestamps delete this row - only its individual cells. So the row now
            # exists in the base table, so should also exist in the view table.
            cql.execute(f"insert into {table} (p, c) values (1, 1) using timestamp 15")
            check([(1, 1)])

# Like test_partial_delete_unselected_column above, but here the view selects
# a and b, so the question of whether the view row lives is about the base
# row's own liveness and that of the selected columns.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_partial_delete_selected_column(cql, test_keyspace, flush, clock):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace,
            'p int, c int, a int, b int, e int, f int, primary key (p, c)', extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c, a, b', 'p, c',
                'p is not null and c is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            cql.execute(f"update {table} using timestamp 10 set b = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 1)])

            cql.execute(f"delete b from {table} using timestamp 11 where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 1 set a = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, 1, None)])

            cql.execute(f"delete a from {table} using timestamp 1 where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"insert into {table} (p, c) values (1, 1) using timestamp 0")
            check([(1, 1, None, None)])

            cql.execute(f"update {table} using timestamp 12 set b = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 1)])

            cql.execute(f"delete b from {table} using timestamp 13 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, None)])

            cql.execute(f"delete from {table} using timestamp 14 where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"insert into {table} (p, c) values (1, 1) using timestamp 15")
            check([(1, 1, None, None)])

            cql.execute(f"update {table} using timestamp 15 and ttl {clock.ttl} set b = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 1)])

            clock.jump(clock.ttl + 1)
            check([(1, 1, None, None)])

            cql.execute(f"delete from {table} using timestamp 15 where p = 1 and c = 1")
            maybe_flush()
            check([])

            # removal generated by unselected column should not shadow selected column with smaller timestamp
            cql.execute(f"update {table} using timestamp 18 set e = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, None)])

            cql.execute(f"update {table} using timestamp 18 set e = null where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 16 set a = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, 1, None)])

# The base column a is the view's partition key, so the view row can only live
# as long as a does. When a is written with a TTL, the view row has to go away
# when that TTL expires - even though the base row outlives it, because b is
# still alive.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_update_column_in_view_pk_with_ttl(cql, test_keyspace, flush, clock):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int primary key, a int, b int',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'a, p',
                'p is not null and a is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"update {table} set a = 1 where p = 1")
            maybe_flush()
            check([(1, 1, None)])

            cql.execute(f"delete a from {table} where p = 1")
            maybe_flush()
            check([])

            cql.execute(f"insert into {table} (p) values (1)")
            check([])

            cql.execute(f"update {table} using ttl {clock.ttl} set a = 10 where p = 1")
            maybe_flush()
            check([(10, 1, None)])

            cql.execute(f"update {table} set b = 100 where p = 1")
            maybe_flush()
            check([(10, 1, 100)])

            clock.jump(clock.ttl + 1)
            check([])

# The base row is inserted with a TTL, so its row marker expires - but an
# unselected column, v, is then written without one (ttl 0 means no TTL at
# all). That keeps the base row alive after the marker is gone, and so the
# view row has to survive too.
def test_unselected_column_can_preserve_ttld_row_maker(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'c, p',
                'p is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (p, c) values (0, 0) using ttl {clock.ttl}")
            cql.execute(f"update {table} using ttl 0 set v = 0 where p = 0 and c = 0")
            clock.jump(clock.ttl + 1)
            assert [(0, 0)] == list(cql.execute(f"select * from {mv}"))

# A view selecting nothing but the base's key columns still has to know
# whether the base row is alive, which depends on the liveness of columns the
# view doesn't select - here v1 and v2.
#
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_update_column_not_in_view(cql, test_keyspace, flush, clock):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"update {table} using timestamp 0 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            check([(0, 0)])

            cql.execute(f"delete v1 from {table} using timestamp 1 where p = 0 and c = 0")
            maybe_flush()
            check([])

            # Written at the same timestamp as the deletion above, so the
            # deletion still wins and the row stays dead.
            cql.execute(f"update {table} using timestamp 1 set v1 = 1 where p = 0 and c = 0")
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 2 set v2 = 1 where p = 0 and c = 0")
            maybe_flush()
            check([(0, 0)])

            # v2 is still alive, so deleting v1 leaves the base row - and the
            # view row - in place.
            cql.execute(f"delete v1 from {table} using timestamp 3 where p = 0 and c = 0")
            maybe_flush()
            check([(0, 0)])

            cql.execute(f"delete v2 from {table} using timestamp 4 where p = 0 and c = 0")
            maybe_flush()
            check([])

            # The same again, but with v2 given a TTL instead of being deleted:
            # when it expires the base row dies with it, and so does the view row.
            cql.execute(f"update {table} using ttl {clock.ttl} set v2 = 1 where p = 0 and c = 0")
            maybe_flush()
            check([(0, 0)])

            clock.jump(clock.ttl + 1)
            check([])

            cql.execute(f"update {table} set v2 = 1 where p = 0 and c = 0")
            maybe_flush()
            check([(0, 0)])

            # The view needs to know whether v2 is alive, so v2 can't be dropped
            with pytest.raises(InvalidRequest, match='Cannot drop column v2'):
                cql.execute(f"alter table {table} drop v2")

# Modifying a collection which the view doesn't select still decides whether
# the base row - and so the view row - is alive, just as a plain column would.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_partial_update_with_unselected_collections(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace,
            'p int, c int, a int, b int, l list<int>, s set<int>, m map<int,text>, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c, a, b', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"update {table} set l=l+[1,2,3] where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, None)])

            cql.execute(f"update {table} set l=l-[1,2] where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, None)])

            cql.execute(f"update {table} set b = 3 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 3)])

            cql.execute(f"update {table} set b=null, l=l-[3], s=s-{{3}} where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"update {table} set m=m+{{3:'text'}}, l=l-[1], s=s-{{2}} where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, None)])

            # The view needs to know whether m is alive, so m can't be dropped
            with pytest.raises(InvalidRequest, match='Cannot drop column m'):
                cql.execute(f"alter table {table} drop m")

# The same as test_partial_update_with_unselected_collections above, for an
# unselected user-defined type column - including updating just one of its
# fields.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_partial_update_with_unselected_udt(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_type(cql, test_keyspace, '(a int, b int)') as ut:
        with new_test_table(cql, test_keyspace,
                f'p int, c int, a int, b int, u {ut}, primary key (p, c)', extra=no_cache) as table:
            with new_materialized_view(cql, table, 'p, c, a, b', 'c, p',
                    'p is not null and c is not null', extra=no_cache) as mv:
                def check(expected):
                    assert expected == list(cql.execute(f"select * from {mv}"))

                cql.execute(f"update {table} set u.a = 1 where p = 1 and c = 1")
                maybe_flush()
                check([(1, 1, None, None)])

                cql.execute(f"update {table} set b = 3 where p = 1 and c = 1")
                maybe_flush()
                check([(1, 1, None, 3)])

                cql.execute(f"update {table} set b=null, u.a = null where p = 1 and c = 1")
                maybe_flush()
                check([])

                cql.execute(f"update {table} set u = {{a: 1, b: 1}} where p = 1 and c = 1")
                maybe_flush()
                check([(1, 1, None, None)])

                # The view needs to know whether u is alive, so u can't be dropped
                with pytest.raises(InvalidRequest, match='Cannot drop.*column u'):
                    cql.execute(f"alter table {table} drop u")

# The view selects only the base's key columns, so whether a view row exists
# is decided entirely by the liveness of the row marker and of the unselected
# column v - and here both of them are given TTLs, at different times, so that
# each in turn is the thing keeping the base row alive.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_unselected_columns_ttl(cql, test_keyspace, flush, clock):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            # The row marker expires, but v outlives it - 1000 is kept as the
            # C++ original had it, because the test never jumps that far, so
            # it costs nothing even where the clock fixture really sleeps.
            cql.execute(f"insert into {table} (p, c) values (1, 1) using ttl {clock.ttl}")
            cql.execute(f"update {table} using ttl 1000 set v = 0 where p = 1 and c = 1")
            maybe_flush()

            clock.jump(clock.ttl + 1)
            assert [(1, 1)] == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"delete v from {table} where p = 1 and c = 1")
            maybe_flush()
            assert [] == list(cql.execute(f"select * from {mv}"))

            # Now the other way round: p=1 gets a marker which never expires
            # and a v which does, while p=3 gets only an expiring marker.
            cql.execute(f"insert into {table} (p, c) values (1, 1)")
            cql.execute(f"update {table} using ttl {clock.ttl} set v = 0 where p = 1 and c = 1")
            cql.execute(f"insert into {table} (p, c) values (3, 3) using ttl {clock.ttl}")

            clock.jump(clock.ttl + 1)
            assert [(1, 1)] == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))
            assert [] == list(cql.execute(f"select * from {mv} where p = 3 and c = 3"))

            cql.execute(f"update {table} set v = 0 where p = 3 and c = 3")
            maybe_flush()
            assert [(3, 3)] == list(cql.execute(f"select * from {mv} where p = 3 and c = 3"))

# A view keyed on a regular base column: the view row follows that column
# appearing, being set to null, being deleted with the whole base partition,
# and being written again.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_partition_deletion(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, a int, b int, c int, primary key (p)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'p, a',
                'p is not null and a is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"insert into {table} (p, a, b, c) values (1, 1, 1, 1) using timestamp 0")
            maybe_flush()
            check([(1, 1, 1, 1)])

            cql.execute(f"update {table} using timestamp 1 set a = null where p = 1")
            maybe_flush()
            check([])

            cql.execute(f"delete from {table} using timestamp 2 where p = 1")
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 3 set a = 1, b = 1 where p = 1")
            maybe_flush()
            check([(1, 1, 1, None)])

# CASSANDRA-13409: deleted columns must not reappear when the view key changes.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_commutative_row_deletion(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, v1 int, v2 int, primary key (p)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'v1, p',
                'p is not null and v1 is not null', extra=no_cache) as mv:
            def check(base, view, view_select='v1, p, v2, writetime(v2)'):
                assert base == list(cql.execute(f"select p, v1, v2 from {table}"))
                assert view == list(cql.execute(f"select {view_select} from {mv}"))

            cql.execute(f"insert into {table} (p, v1, v2) values (3, 1, 3) using timestamp 1")
            maybe_flush()
            check([(3, 1, 3)], [(3, 1)], view_select='v2, writetime(v2)')

            cql.execute(f"delete from {table} using timestamp 2 where p = 3")
            maybe_flush()
            check([], [], view_select='v2, writetime(v2)')

            cql.execute(f"insert into {table} (p, v1) values (3, 1) using timestamp 3")
            maybe_flush()
            check([(3, 1, None)], [(1, 3, None, None)])

            cql.execute(f"update {table} using timestamp 4 set v1 = 2 where p = 3")
            maybe_flush()
            check([(3, 2, None)], [(2, 3, None, None)])

            cql.execute(f"update {table} using timestamp 5 set v1 = 1 where p = 3")
            maybe_flush()
            check([(3, 1, None)], [(1, 3, None, None)])

            # Compacting must not bring the deleted v2 back
            nodetool.compact(cql, table)
            nodetool.compact(cql, mv)
            check([(3, 1, None)], [(1, 3, None, None)])

            # The deletion must win over the preceding write at the same timestamp.
            cql.execute(f"update {table} using timestamp 5 set v1 = null where p = 3")
            maybe_flush()
            check([(3, None, None)], [])

# After the base row's marker expires, the row is kept alive by the unselected
# column a - so the view row lives on too. Only when a goes as well does the
# view row go, and writing the selected column b then brings it back.
# The C++ original flushed after every write, unconditionally rather than as a
# parametrized variant, so this test does the same.
def test_unselected_column_with_expired_marker(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace,
            'p int, c int, a int, b int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c, b', 'c, p',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"update {table} set a = 1 where p = 1 and c = 1")
            nodetool.flush_all(cql)
            cql.execute(f"insert into {table} (p, c) values (1, 1) using ttl {clock.ttl}")
            nodetool.flush_all(cql)
            check([(1, 1, None)])

            clock.jump(clock.ttl + 1)
            check([(1, 1, None)])

            cql.execute(f"update {table} set a = null where p = 1 and c = 1")
            nodetool.flush_all(cql)
            check([])

            cql.execute(f"update {table} using timestamp 1 set b = 1 where p = 1 and c = 1")
            nodetool.flush_all(cql)
            check([(1, 1, 1)])

# The view's key column v1 can be rewritten at timestamps below that of the
# base row's marker, and the view follows it - while v2, written earlier
# still, keeps the write time it had all along.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_update_with_column_timestamp_smaller_than_pk(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, v1 int, v2 int, primary key (p)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'v1, p',
                'p is not null and v1 is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(
                    f"select v1, p, v2, writetime(v2) from {mv}"))

            cql.execute(f"insert into {table} (p, v1, v2) values (3, 1, 3) using timestamp 6")
            maybe_flush()
            check([(1, 3, 3, 6)])

            # A row marker written at a much later timestamp doesn't disturb
            # the columns written before it
            cql.execute(f"insert into {table} (p) values (3) using timestamp 20")
            maybe_flush()
            check([(1, 3, 3, 6)])

            cql.execute(f"update {table} using timestamp 7 set v1 = 2 where p = 3")
            maybe_flush()
            check([(2, 3, 3, 6)])

            cql.execute(f"update {table} using timestamp 8 set v1 = 1 where p = 3")
            maybe_flush()
            check([(1, 3, 3, 6)])

# When most of a view's rows have gone away - here because the base column
# their key is built from was deleted - a SELECT with a LIMIT must still
# return as many live rows as the limit asks for, not stop early at the dead
# ones. Checked against two views which arrange their key differently.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_expired_marker_with_limit(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    with new_test_table(cql, test_keyspace, 'p int, a int, b int, primary key (p)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'p, a',
                'p is not null and a is not null', extra=no_cache) as vcf1, \
             new_materialized_view(cql, table, '*', 'a, p',
                'p is not null and a is not null', extra=no_cache) as vcf2:
            for i in range(1, 101):
                cql.execute(f"insert into {table} (p, a, b) values ({i}, {i}, {i})")
            # Deleting a, which both views use as a key column, removes the
            # view row - leaving only the two rows where i is a multiple of 50.
            for i in range(1, 101):
                if i % 50 != 0:
                    cql.execute(f"delete a from {table} where p = {i}")
            if flush:
                nodetool.flush_all(cql)

            for view in [vcf1, vcf2]:
                assert 1 == len(list(cql.execute(f"select * from {view} limit 1")))
                assert 2 == len(list(cql.execute(f"select * from {view} limit 2")))
                assert [(50, 50, 50), (100, 100, 100)] == sorted(
                    cql.execute(f"select p, a, b from {view}"))

# The view's key column a is rewritten at timestamps above and below that of
# the base row's other columns, and set to null and back. The view row must
# follow it each time - including right after a compaction of the view.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_update_with_column_timestamp_bigger_than_pk(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, a int, b int, primary key (p)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'p, a',
                'p is not null and a is not null', extra=no_cache) as mv:
            def check(expected, limit=''):
                assert expected == list(cql.execute(f"select * from {mv} {limit}"))

            cql.execute(f"delete from {table} using timestamp 0 where p = 1")
            maybe_flush()

            cql.execute(f"insert into {table} (p, a, b) values (1, 1, 1) using timestamp 1")
            maybe_flush()
            check([(1, 1, 1)])

            cql.execute(f"update {table} using timestamp 10 set b = 2 where p = 1")
            maybe_flush()
            check([(1, 1, 2)])

            cql.execute(f"update {table} using timestamp 2 set a = 2 where p = 1")
            maybe_flush()
            check([(1, 2, 2)])

            # Compacting the view must not disturb the row
            nodetool.compact(cql, mv)
            check([(1, 2, 2)], limit='limit 1')

            cql.execute(f"update {table} using timestamp 11 set a = 1 where p = 1")
            maybe_flush()
            check([(1, 1, 2)], limit='limit 1')

            cql.execute(f"update {table} using timestamp 12 set a = null where p = 1")
            maybe_flush()
            check([], limit='limit 1')

            cql.execute(f"update {table} using timestamp 13 set a = 1 where p = 1")
            maybe_flush()
            check([(1, 1, 2)], limit='limit 1')

# A view whose key is made only of base key columns: its row lives exactly as
# long as the base row does, whether what keeps the base row alive is one of
# its regular columns or just its row marker.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_no_regular_base_column_in_view_pk(cql, test_keyspace, flush):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, '*', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"update {table} using timestamp 1 set v1 = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, 1, None)])

            cql.execute(f"update {table} using timestamp 2 set v1 = null, v2 = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 1)])

            cql.execute(f"update {table} using timestamp 2 set v2 = null where p = 1 and c = 1")
            maybe_flush()
            check([])

            # A bare row marker is enough to keep the base row, and the view
            # row, alive
            cql.execute(f"insert into {table} (p, c) values (1, 1) using timestamp 3")
            maybe_flush()
            check([(1, 1, None, None)])

            cql.execute(f"delete from {table} using timestamp 4 where p = 1 and c = 1")
            maybe_flush()
            check([])

            cql.execute(f"update {table} using timestamp 5 set v2 = 1 where p = 1 and c = 1")
            maybe_flush()
            check([(1, 1, None, 1)])

# v1 is the view's partition key, so setting it to null removes the view row.
# Writing it again with a TTL brings the view row back - but only until the
# TTL expires, and the base row's older, never-expiring row marker must not
# shadow that expiry and keep the view row alive.
# As in the test above, the C++ original flushed unconditionally rather than
# as a parametrized variant.
def test_shadowing_row_marker(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace, 'p int, v1 int, v2 int, primary key (p)') as table:
        with new_materialized_view(cql, table, '*', 'v1, p',
                'p is not null and v1 is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv}"))

            cql.execute(f"insert into {table} (p, v1, v2) values (1, 1, 1)")

            cql.execute(f"update {table} set v1 = null where p = 1")
            nodetool.flush_all(cql)
            check([])

            cql.execute(f"update {table} using ttl {clock.ttl} set v1 = 1 where p = 1")
            nodetool.flush_all(cql)
            check([(1, 1, 1)])

            clock.jump(clock.ttl + 1)
            check([])

# The whole base row is written with a TTL, and then an unselected column is
# written with a much longer one and deleted again. When the first TTL
# expires, nothing is left alive, and the longer-lived write which came in
# between must not shadow the row marker's own expiry and keep the view row.
#
# cassandra_bug: this is exactly what Cassandra gets wrong - it keeps the view
# row, with v1 null, after the row marker has expired. It is the same area as
# the other #3362 tests here: Cassandra doesn't track the liveness of the
# unselected column v2 separately from the row marker, so the write to v2
# shadows the marker's expiry. See the still-open CASSANDRA-13826.
@pytest.mark.parametrize("flush", [False, True], ids=["noflush", "flush"])
def test_marker_timestamp_is_not_shadowed_by_previous_update(cql, test_keyspace, flush, clock, cassandra_bug):
    no_cache = "with caching = {'enabled': 'false'}" if flush and is_scylla(cql) else ""
    def maybe_flush():
        if flush:
            nodetool.flush_all(cql)
    with new_test_table(cql, test_keyspace, 'p int, c int, v1 int, v2 int, primary key (p, c)',
            extra=no_cache) as table:
        with new_materialized_view(cql, table, 'p, c, v1', 'c, p',
                'p is not null and c is not null', extra=no_cache) as mv:
            cql.execute(f"insert into {table} (p, c, v1, v2) values (1, 1, 1, 1) using ttl {clock.ttl}")
            maybe_flush()
            # 1000 is kept as the original had it - the test never jumps that
            # far, so it costs nothing even where the fixture really sleeps.
            cql.execute(f"update {table} using ttl 1000 set v2 = 1 where p = 1 and c = 1")
            maybe_flush()
            cql.execute(f"delete v2 from {table} where p = 1 and c = 1")
            maybe_flush()
            clock.jump(clock.ttl + 1)
            assert [] == list(cql.execute(f"select * from {mv}"))

# A reproducer for issue #3362, not involving TTLs.
# The test involves a view that selects no column except the base's primary
# key, so view rows contain no cells besides a row marker, so as a base
# row appears and disappears as we update and delete individual cells in
# that row, we need to insert and delete the row marker with varying
# timestamps to make sure the view row appears and disappears as needed.
# But as we shall see, after enough trickery, we run out of timestamps
# to use to revive the row marker, and fail to revive it. So to fix
# issue #3362, we needed to remember all cells separately ("virtual
# cells").
#
# cassandra_bug: the last step below leaves Cassandra's view empty while the
# base row is alive. Remembering the unselected cells separately is exactly
# what Scylla did for #3362 and what Cassandra has not done - see the comment
# on test_partial_delete_unselected_column above, and the still-open
# CASSANDRA-13826 which proposes it.
def test_3362_no_ttls(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, a int, b int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            # In row p=1 c=1, insert two cells - b=1 at timestamp 10, a=1 at timestamp 20:
            cql.execute(f"update {table} using timestamp 10 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 20 set a = 1 where p = 1 and c = 1")
            check([(1, 1)])

            # Delete just a=1 (with timestamp 21). The base row will still exist (with b=1),
            # and accordingly the view row too:
            cql.execute(f"delete a from {table} using timestamp 21 where p = 1 and c = 1")
            check([(1, 1)])

            # At this point, we still have the base row with b=1 at timestamp 10
            # (and a=1 was deleted at timestamp 21). If we delete the b=1 at
            # timestamp 11, nothing will remain in the base row, and the view
            # row should disappear as well:
            cql.execute(f"delete b from {table} using timestamp 11 where p = 1 and c = 1")
            check([])

            # Now we finally reproduce #3362: We now add b=1 again, at timestamp
            # 12 (it was earlier deleted in timestamp 11). The base row is live
            # again, and so should the view row.
            # With issue #3362, the view row failed to become alive. The reason
            # is that to make the above is_empty() succeed, the implementation
            # deletes the row marker with timestamp 21 (the maximal timestamp
            # seen in the row). But now, we add a row marker again with the same
            # timestamp 21, but the deletion wins so the row marker is still
            # missing. (note that had data won over deletions, the is_empty()
            # test above would have failed instead).
            cql.execute(f"update {table} using timestamp 12 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])

# This is another reproducer for issue #3362, using TTLs instead of
# numerous back-and-forth additions and deletions.
#
# cassandra_bug for the same reason as test_3362_no_ttls above: Cassandra
# doesn't track the liveness of the unselected cells separately, so once a
# expires it loses the view row even though b is still alive. See the
# still-open CASSANDRA-13826.
def test_3362_with_ttls(cql, test_keyspace, clock, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, a int, b int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))
            # In row p=1 c=1, insert two cells - a=1 with ttl, and b=1 without
            # ttl. The ttl'ed cell is inserted first, with a newer timestamp.
            # The problem is that the view row's marker gets, with a new
            # timestamp, a ttl. Then, when we go to add another column with an
            # older timestamp, and try to set the row marker without a
            # ttl - the older timestamp of this update looses, and we wrongly
            # remain with a ttl on the view row marker.
            cql.execute(f"update {table} using timestamp 2 and ttl {clock.ttl} set a = 1 where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 1 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])

            # Pass the time forward. Cell 'a' will have expired, but
            # cell 'b' will still exist, so the base row still exists and the
            # corresponding view row should also exist too.
            clock.jump(clock.ttl + 1)

            # verify that the base row still exists (cell b didn't expire)
            assert [(1, 1, None, 1)] == list(cql.execute(f"select * from {table} where p = 1 and c = 1"))

            # verify that the view row still exists too.
            # This check failing is issue #3362.
            check([(1, 1)])

# The following are more test for issue #3362, same as test_3362_no_ttls
# and test_3362_with_ttls, just with a collection with items "1" and "2"
# instead of separate columns a and b. For brevity, comments were removed,
# so refer to the comments in the original code above.
#
# cassandra_bug for the same reason as test_3362_no_ttls: Cassandra doesn't
# track the liveness of the unselected column separately, so the last step
# leaves its view empty although the base row is alive.
@pytest.mark.parametrize("kind", ['set', 'list', 'map'])
def test_3362_no_ttls_with_collections(cql, test_keyspace, kind, cassandra_bug):
    column_type = {'set': 'set<int>', 'list': 'list<int>', 'map': 'map<int, int>'}[kind]
    # The literal for a collection holding just the element n. A map needs a
    # value for it, and 17 will do.
    def item(n):
        return {'set': f'{{{n}}}', 'list': f'[{n}]', 'map': f'{{{n} : 17}}'}[kind]
    with new_test_table(cql, test_keyspace, f'p int, c int, a {column_type}, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))
            def remove(n, timestamp):
                # A map element is removed by its key, the others by value
                if kind == 'map':
                    cql.execute(f"delete a[{n}] from {table} using timestamp {timestamp} "
                                "where p = 1 and c = 1")
                else:
                    cql.execute(f"update {table} using timestamp {timestamp} "
                                f"set a = a - {item(n)} where p = 1 and c = 1")

            cql.execute(f"update {table} using timestamp 10 set a = a + {item(2)} where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 20 set a = a + {item(1)} where p = 1 and c = 1")
            check([(1, 1)])

            remove(1, 21)
            check([(1, 1)])

            remove(2, 11)
            check([])

            cql.execute(f"update {table} using timestamp 12 set a = a + {item(2)} where p = 1 and c = 1")
            check([(1, 1)])

# The TTL versions of the collection tests above - the same as
# test_3362_with_ttls, with a collection instead of the separate columns a
# and b.
#
# cassandra_bug for the same reason as the tests above.
@pytest.mark.parametrize("kind", ['set', 'list', 'map'])
def test_3362_with_ttls_with_collections(cql, test_keyspace, kind, clock, cassandra_bug):
    column_type = {'set': 'set<int>', 'list': 'list<int>', 'map': 'map<int, int>'}[kind]
    def item(n):
        return {'set': f'{{{n}}}', 'list': f'[{n}]', 'map': f'{{{n} : 17}}'}[kind]
    with new_test_table(cql, test_keyspace, f'p int, c int, a {column_type}, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            cql.execute(f"update {table} using timestamp 2 and ttl {clock.ttl} "
                        f"set a = a + {item(1)} where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 1 "
                        f"set a = a + {item(2)} where p = 1 and c = 1")
            check([(1, 1)])

            # The first element expires, but the second is still alive, so the
            # base row - and with it the view row - must survive.
            clock.jump(clock.ttl + 1)
            check([(1, 1)])

# This is a version of test_3362_with_ttls with frozen collection fields
# instead of integer fields in test_3362_with_ttls. The intention is to
# verify that we properly fixed #3362 in this case - by replacing the
# frozen collection by a single virtual cell, not a collection.
#
# cassandra_bug for the same reason as the rest of the #3362 family.
def test_3362_with_ttls_frozen(cql, test_keyspace, clock, cassandra_bug):
    with new_test_table(cql, test_keyspace,
            'p int, c int, a frozen<set<int>>, b frozen<set<int>>, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            cql.execute(f"update {table} using timestamp 2 and ttl {clock.ttl} "
                        "set a = {1,2} where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 1 set b = {{3,4}} where p = 1 and c = 1")
            check([(1, 1)])

            clock.jump(clock.ttl + 1)
            check([(1, 1)])

# This is a version of test_3362_with_ttls with the added twist that the
# unselected column involved did not exist when the base table and view
# were originally created, but only added later with an "alter table".
# For this test to work, "alter table" will need to add the virtual
# columns in the view table for the newly created unselected column in
# the base table.
#
# This test is about handling changes to virtual columns as the base table
# columns change, but only for the "add" case. Theoretically we could have had
# problems in the "drop" and "rename" cases as well, but today, those are not
# supported:
# 1. Today we do not allow "alter table drop" to drop any column from a base
#    table with views - even unselected columns.
#    If we every do allow this, we need to also check that we drop the
#    virtual column from the view.
# 2. Today we do not allow "alter table rename" to rename any non-pk
#    column, so unselected columns also cannot be renamed. If this
#    limitation is ever lifted, we will need to check that if we
#    rename an unselected base column, the virtual column in the view is
#    also renamed.
#
# cassandra_bug for the same reason as the rest of the #3362 family.
def test_3362_with_ttls_alter_add(cql, test_keyspace, clock, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))

            # Add with "alter table" two additional columns to the base table -
            # a and b. These are not selected in the materialized view, and we
            # want to check that they are treated like unselected columns
            # (namely, virtual columns are added to the view).
            cql.execute(f"alter table {table} add a int")
            cql.execute(f"alter table {table} add b int")

            cql.execute(f"update {table} using timestamp 2 and ttl {clock.ttl} set a = 1 where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 1 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])

            clock.jump(clock.ttl + 1)
            assert [(1, 1, None, 1)] == list(cql.execute(f"select * from {table} where p = 1 and c = 1"))
            check([(1, 1)])

# Tests that after the fixes for issue #3362, various miscellaneous
# combinations of appearance and disappearance of unselected base cells
# and row markers which happen to cause view_updates::do_delete_old_entry()
# (i.e., deletion of the view row), work as expected.
#
# The C++ original had to sleep before its last check, because it wanted to
# confirm that a view row does *not* disappear and had no way to know when the
# view updates were done. Here that isn't needed: on a single node the view
# update is applied before the write is acknowledged - see the comment at the
# top of this file - so by the time the write returns, anything that was going
# to happen to the view already has.
#
# cassandra_bug for the same reason as test_3362_no_ttls above: Cassandra
# doesn't track the liveness of unselected base columns separately, so it
# doesn't notice when the last one of them dies. Concretely, it fails on step 2
# below - after the only live cell a is deleted the base row is gone, but
# Cassandra leaves the view row in place. This is the still-open
# CASSANDRA-13826.
def test_3362_row_deletion_1(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, c int, a int, b int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))
            # In row p=1 c=1:
            #  1. Insert a cell a=1, at timestamp 2
            #  2. Delete the cell a=1, at timestamp 10. The base row is now gone
            #     and so should the view row, and do_delete_old_entry() is called.
            #  3. Insert a full row for p=1 c=1 at timestamp 1. This is an
            #     "insert" so it also inserts a row marker. We already have
            #     a newer (ts=10) deletion of the cell a, but cell b is still
            #     alive and so is the row marker.
            #  4. Delete cell b at timestamp 3. Now both cells are dead, but
            #     the row should still alive and the view row should still exist.
            cql.execute(f"update {table} using timestamp 2 set a = 1 where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"delete a from {table} using timestamp 10 where p = 1 and c = 1")
            check([])

            cql.execute(f"insert into {table} (p, c) values (1, 1) using timestamp 1")
            check([(1, 1)])

            cql.execute(f"delete b from {table} using timestamp 3 where p = 1 and c = 1")
            # the base row should now be empty but still exist (there's still the row marker)
            assert [(1, 1, None, None)] == list(cql.execute(f"select * from {table} where p = 1 and c = 1"))
            # The row should still exists, because the row marker is still
            # alive. It was a bug that the row marker was deleted too,
            # because of a wrong row marker deletion set for timestamp 10.
            check([(1, 1)])

# Verify that do_delete_old_entry()'s r.apply(update.tomb()) works as expected.
# As in the test above, the original's sleeps are not needed here.
def test_3362_row_deletion_2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, a int, b int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c', 'p, c',
                'p is not null and c is not null') as mv:
            def check(expected):
                assert expected == list(cql.execute(f"select * from {mv} where p = 1 and c = 1"))
            # In row p=1 c=1, insert two cells - b=1 at timestamp 1, a=1 at timestamp 2.
            # We use "update", not "insert", so there will not be a row marker.
            cql.execute(f"update {table} using timestamp 1 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])

            cql.execute(f"update {table} using timestamp 2 set a = 1 where p = 1 and c = 1")
            check([(1, 1)])

            # Delete the entire base row, with timestamp 10. The view row should
            # also disappear.
            cql.execute(f"delete from {table} using timestamp 10 where p = 1 and c = 1")
            check([])

            # Reinsert an (unselected) cell in row p=1 c=1 at timestamp 3.
            # This is *before* the timestamp of the row's deletion (which was 10)
            # so the row should NOT reappear. For this to work, it is important
            # that view_updates::do_delete_old_entry() call r.apply(update.tomb()).
            cql.execute(f"update {table} using timestamp 3 set b = 1 where p = 1 and c = 1")
            check([])

            # If we reinsert the cell at timestamp 11, after the deletion, the base
            # row will re-emerge, and so should the view row
            cql.execute(f"update {table} using timestamp 11 set b = 1 where p = 1 and c = 1")
            check([(1, 1)])
