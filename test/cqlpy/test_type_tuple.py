# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests involving the "tuple" column type.
#
# There are additional tests involving tuples in the context of other features
# (describe, aggregates, filtering, json, etc.) in other files. We also have
# many tests for tuples ported from Cassandra in cassandra_tests/. Here we only
# have a few tests that didn't fit elsewhere.
#############################################################################

import time
import pytest
from cassandra.protocol import InvalidRequest, SyntaxException
from .util import new_test_table, unique_key_int, new_function, new_aggregate, new_materialized_view

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, t tuple<int, int>') as table:
        yield table

# Unlike UDTs, tuples are always frozen, meaning that individual fields cannot
# be updated separately. So there is no point in allowing writetime() or ttl()
# on individual fields of a tuple - so we do reject them, and we'll test this
# here. Additionally, we check that writetime() and ttl() on the entire tuple
# column is allowed.
def test_writetime_on_tuple(cql, table1):
    # Subscript on a tuple is not valid for WRITETIME, since tuple is not
    # a map or set.
    with pytest.raises(InvalidRequest, match=' t '):
        cql.execute(f"SELECT WRITETIME(t[0]) FROM {table1}")
    # Field selection on a tuple is also not valid: tuples are not user
    # types, so the field_selection preparation itself rejects it.
    with pytest.raises(InvalidRequest, match='user type'):
        cql.execute(f"SELECT WRITETIME(t.a) FROM {table1}")
    # But WRITETIME() on the entire tuple column is allowed: it returns
    # the single timestamp of the whole (frozen) tuple cell.
    p = unique_key_int()
    timestamp = int(time.time() * 1000000) - 1234
    cql.execute(f"INSERT INTO {table1}(p, t) VALUES ({p}, (1, 2)) USING TIMESTAMP {timestamp}")
    assert list(cql.execute(f"SELECT WRITETIME(t) FROM {table1} WHERE p={p}")) == [(timestamp,)]

def test_ttl_on_tuple(cql, table1):
    # Subscript on a tuple is not valid for TTL, since tuple is not
    # a map or set.
    with pytest.raises(InvalidRequest, match=' t '):
        cql.execute(f"SELECT TTL(t[0]) FROM {table1}")
    # Field selection on a tuple is also not valid: tuples are not user
    # types, so the field_selection preparation itself rejects it.
    with pytest.raises(InvalidRequest, match='user type'):
        cql.execute(f"SELECT TTL(t.a) FROM {table1}")
    # But TTL() on the entire tuple column is allowed: it returns
    # the single timestamp of the whole (frozen) tuple cell.
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p, t) VALUES ({p}, (1, 2)) USING TTL 1000")
    ret = list(cql.execute(f"SELECT TTL(t) FROM {table1} WHERE p={p}"))
    # TTL() returns the remaining TTL, which may be slightly less than 1000 by the time we read it, so we check that it's between 900 and 1000.
    assert len(ret) == 1 and len(ret[0]) == 1 and 900 <= ret[0][0] <= 1000

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, t1 tuple<int>, t2 tuple<int, int>') as table:
        yield table

# "(x)" around a single term reads as a one-element tuple, but it reads just as
# well as a parenthesized x - and has to, once expressions can be grouped.  So
# CQL also spells the tuple with an explicit constructor, the way SQL spells it
# ROW(x), and that spelling means the same thing in either reading.
def test_tuple_constructor(cql, table2):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table2}(p, t1, t2) VALUES ({p}, tuple(1), tuple(2, 3))")
    assert list(cql.execute(f"SELECT t1, t2 FROM {table2} WHERE p={p}")) == [((1,), (2, 3))]
    # It is the same tuple the parentheses build.
    assert list(cql.execute(f"SELECT t1, t2 FROM {table2} WHERE t1 = tuple(1) AND t2 = (2, 3) ALLOW FILTERING")) == [((1,), (2, 3))]
    # Spelling is not case-sensitive, as for any other unquoted name.
    assert list(cql.execute(f"SELECT TUPLE(1) FROM {table2} WHERE p={p}")) == [((1,),)]

# Which of the two readings of "(x)" is in force is a cluster configuration
# option, so that the reading CQL has always had can be kept.
def test_parentheses_around_a_single_term(cql, table2, scylla_only):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table2}(p) VALUES ({p})")
    # By default "(1)" is the one-element tuple...
    assert list(cql.execute(f"SELECT (1) FROM {table2} WHERE p={p}")) == [((1,),)]
    cql.execute("ALTER CLUSTER WITH cql_parentheses_around_a_single_term_make_a_tuple = false")
    try:
        # ... and read the other way it is simply 1.
        assert list(cql.execute(f"SELECT (1) FROM {table2} WHERE p={p}")) == [(1,)]
        # The constructor builds the tuple in either reading, and parentheses
        # around two or more terms are a tuple in either reading.
        assert list(cql.execute(f"SELECT tuple(1) FROM {table2} WHERE p={p}")) == [((1,),)]
        assert list(cql.execute(f"SELECT (1, 2) FROM {table2} WHERE p={p}")) == [((1, 2),)]
    finally:
        cql.execute("ALTER CLUSTER WITH cql_parentheses_around_a_single_term_make_a_tuple = NULL")
    assert list(cql.execute(f"SELECT (1) FROM {table2} WHERE p={p}")) == [((1,),)]

# A tuple has at least one element; "tuple()" is not an empty one.
def test_tuple_constructor_needs_an_element(cql, table2):
    with pytest.raises(SyntaxException, match='at least one element'):
        cql.execute(f"SELECT tuple() FROM {table2}")

# The CQL text the database stores for itself - a view's WHERE clause, an
# aggregate's INITCOND - spells a one-element tuple tuple(x), so that it means
# the same thing however the cluster reads "(x)".  Both are written by the
# database's own printers, and both are read back when the schema is loaded.
def test_one_element_tuple_in_stored_view_where_clause(cql, test_keyspace, scylla_only):
    schema = "p int, c int, v int, PRIMARY KEY (p, c)"
    with new_test_table(cql, test_keyspace, schema) as table:
        where = "p IS NOT NULL AND (c) > (1)"
        with new_materialized_view(cql, table, "*", "c, p", where) as mv:
            mv_name = mv.split(".")[1]
            stored = cql.execute(f"SELECT where_clause FROM system_schema.views WHERE keyspace_name = '{test_keyspace}' AND view_name = '{mv_name}'").one().where_clause
            assert stored == "p IS NOT null AND tuple(c) > tuple(1)"
            desc = cql.execute(f"DESCRIBE MATERIALIZED VIEW {mv}").one().create_statement
            assert "tuple(c) > tuple(1)" in desc
            # The view is whole: its WHERE clause was read back to build it.
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 2, 3)")
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 0, 3)")
            assert sorted(r.c for r in cql.execute(f"SELECT c FROM {mv}")) == [2]

def test_one_element_tuple_in_stored_aggregate_initcond(cql, test_keyspace, scylla_only):
    sfunc_body = "(acc tuple<int>, v int) CALLED ON NULL INPUT RETURNS tuple<int> LANGUAGE lua AS 'return acc'"
    with new_function(cql, test_keyspace, sfunc_body) as sfunc:
        with new_aggregate(cql, test_keyspace, f"(int) SFUNC {sfunc} STYPE tuple<int> INITCOND (7)") as agg:
            stored = cql.execute(f"SELECT initcond FROM system_schema.aggregates WHERE keyspace_name = '{test_keyspace}' AND aggregate_name = '{agg}'").one().initcond
            assert stored == "tuple(7)"
            desc = cql.execute(f"DESCRIBE AGGREGATE {test_keyspace}.{agg}").one().create_statement
            assert "INITCOND tuple(7)" in desc
