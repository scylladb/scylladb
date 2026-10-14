# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for the "X += <value>" and "X -= <value>" compound assignments in an
# UPDATE's SET clause, which Cassandra added in version 3.10 (CASSANDRA-12232)
# as shorthands for "X = X + <value>" and "X = X - <value>". Cassandra's own
# tests for this syntax, which we translated, check it on sets, maps and
# lists (testSets, testMaps and testLists in
# cassandra_tests/validation/entities/collections_test.py), on tuples
# (testTupleModification in
# cassandra_tests/validation/entities/tuple_type_test.py), and on
# counters in a COMPACT STORAGE table (testCounters in
# cassandra_tests/validation/operations/compact_storage_test.py).
# The tests here check additional corner cases.
###############################################################################

import pytest
from .util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest, SyntaxException

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, i int, b boolean, s set<text>, l list<text>, m map<text, int>, fs frozen<set<text>>") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        yield table

# Test "+=" and "-=" on a counter column in a regular table (Cassandra's
# test only checks a COMPACT STORAGE table), with a literal, a negative
# literal and a bind marker.
def test_compound_assignment_counter(cql, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET c += 7 WHERE p = {p}')
    assert list(cql.execute(f'SELECT c FROM {table2} WHERE p = {p}')) == [(7,)]
    cql.execute(f'UPDATE {table2} SET c -= 2 WHERE p = {p}')
    assert list(cql.execute(f'SELECT c FROM {table2} WHERE p = {p}')) == [(5,)]
    cql.execute(f'UPDATE {table2} SET c -= -3 WHERE p = {p}')
    assert list(cql.execute(f'SELECT c FROM {table2} WHERE p = {p}')) == [(8,)]
    stmt = cql.prepare(f'UPDATE {table2} SET c += ? WHERE p = {p}')
    cql.execute(stmt, [10])
    assert list(cql.execute(f'SELECT c FROM {table2} WHERE p = {p}')) == [(18,)]
    stmt = cql.prepare(f'UPDATE {table2} SET c -= ? WHERE p = {p}')
    cql.execute(stmt, [20])
    assert list(cql.execute(f'SELECT c FROM {table2} WHERE p = {p}')) == [(-2,)]

# "+=" and "-=" are single tokens, so a space can't be inserted in the
# middle of one: "c + = 1" is a syntax error.
def test_compound_assignment_space_inside_token(cql, table1, table2):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f'UPDATE {table2} SET c + = 1 WHERE p = {p}')
    with pytest.raises(SyntaxException):
        cql.execute(f'UPDATE {table2} SET c - = 1 WHERE p = {p}')
    with pytest.raises(SyntaxException):
        cql.execute(f"UPDATE {table1} SET s + = {{'a'}} WHERE p = {p}")

# Compound assignments can be mixed with other kinds of assignments in one UPDATE.
def test_compound_assignment_mixed_with_other_assignments(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET s = {{'a'}}, l = ['x'] WHERE p = {p}")
    cql.execute(f"UPDATE {table1} SET i = 3, s += {{'b'}}, l = l + ['y'], m['k'] = 2 WHERE p = {p}")
    assert list(cql.execute(f'SELECT i, s, l, m FROM {table1} WHERE p = {p}')) == [(3, {'a', 'b'}, ['x', 'y'], {'k': 2})]

# "+=" and "-=" are rejected on a column which is neither a counter nor a
# collection, exactly like "X = X + <value>" is. We check this on the
# boolean column b. We don't use a numeric or string column for this test,
# because "+=" on such a column is allowed in an LWT update (see
# test_compound_assignment_lwt_addition below, and test_lwt.py), and without
# LWT, Scylla rejects it with a different error message.
def test_compound_assignment_non_counter_column(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='counter'):
        cql.execute(f'UPDATE {table1} SET b += true WHERE p = {p}')
    with pytest.raises(InvalidRequest, match='counter'):
        cql.execute(f'UPDATE {table1} SET b -= true WHERE p = {p}')

# Compound assignments can be used on a collection in an LWT update.
def test_compound_assignment_lwt_collection(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, s, i) VALUES ({p}, {{'a', 'b'}}, 1)")
    rs = list(cql.execute(f"UPDATE {table1} SET s += {{'c'}} WHERE p = {p} IF EXISTS"))
    assert len(rs) == 1 and rs[0].applied
    rs = list(cql.execute(f"UPDATE {table1} SET s -= {{'a'}} WHERE p = {p} IF i = 1"))
    assert len(rs) == 1 and rs[0].applied
    rs = list(cql.execute(f"UPDATE {table1} SET s += {{'d'}} WHERE p = {p} IF i = 2"))
    assert len(rs) == 1 and not rs[0].applied
    assert list(cql.execute(f'SELECT s FROM {table1} WHERE p = {p}')) == [({'b', 'c'},)]

# In an LWT update, "X = X + <value>" is allowed not only on counters but
# also on a numeric non-counter column - in Scylla since issue #10568 (see
# test_lwt.py), and in Cassandra since Cassandra 4.1. Check that the
# "X += <value>" syntax works there too.
def test_compound_assignment_lwt_addition(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, i) VALUES ({p}, 5)')
    rs = list(cql.execute(f'UPDATE {table1} SET i += 3 WHERE p = {p} IF EXISTS'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT i FROM {table1} WHERE p = {p}')) == [(8,)]

# Similarly, "X = X - <value>" is allowed on a numeric non-counter column in
# an LWT update - in Scylla since issue #10568, but in Cassandra only since
# Cassandra 6.0 (CASSANDRA-20477). Check that the "X -= <value>" syntax works
# there too. The "-=" syntax itself was added in Cassandra 3.10, together
# with "+=", but before Cassandra 6.0 it couldn't be used in this case.
def test_compound_assignment_lwt_subtraction(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, i) VALUES ({p}, 5)')
    rs = list(cql.execute(f'UPDATE {table1} SET i -= 1 WHERE p = {p} IF i > 0'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT i FROM {table1} WHERE p = {p}')) == [(4,)]

# Without an LWT condition, "+=" and "-=" on a numeric non-counter column are
# rejected, like the long form is. Scylla's error message says an LWT
# condition is needed, Cassandra's says the column isn't a counter, so we
# don't check the message.
def test_compound_assignment_arithmetic_without_lwt(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f'UPDATE {table1} SET i += 1 WHERE p = {p}')
    with pytest.raises(InvalidRequest):
        cql.execute(f'UPDATE {table1} SET i -= 1 WHERE p = {p}')

# "+=" and "-=" are rejected on a frozen collection, exactly like
# "X = X + <value>" is, because a frozen collection can only be replaced
# as a whole.
def test_compound_assignment_frozen_collection(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='frozen'):
        cql.execute(f"UPDATE {table1} SET fs += {{'a'}} WHERE p = {p}")
    with pytest.raises(InvalidRequest, match='frozen'):
        cql.execute(f"UPDATE {table1} SET fs -= {{'a'}} WHERE p = {p}")

# "+=" is rejected on a primary key column, like any other assignment to it.
def test_compound_assignment_key_column(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='PRIMARY KEY'):
        cql.execute(f'UPDATE {table1} SET p += 1 WHERE p = {p}')
