# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the LIKE operator in SELECT's WHERE clause.
#
# Cassandra supports LIKE with ALLOW FILTERING (rather than only on columns
# with a SASI index) only since version 6.0 (CASSANDRA-17198), so the tests
# here which are not marked scylla_only fail on older Cassandra versions.
# Cassandra's LIKE also differs from Scylla's in several ways, so many tests
# here are Scylla-only; each such test explains why.
#############################################################################

from cassandra import InvalidRequest
import pytest

from .util import new_test_table


def rows(result):
    return sorted(list(row) for row in result)

# Scylla-only because Cassandra does not treat '_' as a wildcard in LIKE
# patterns (only '%'), so the '_' patterns below match nothing there.
def test_like_operator(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int primary key, s text") as t:
        assert rows(cql.execute(f"select s from {t} where s like 'abc' allow filtering")) == []
        cql.execute(f"insert into {t} (p, s) values (1, 'abc')")
        assert rows(cql.execute(f"select s from {t} where s like 'abc' allow filtering")) == [["abc"]]
        assert rows(cql.execute(f"select s from {t} where s like 'ab_' allow filtering")) == [["abc"]]
        cql.execute(f"insert into {t} (p, s) values (2, 'abb')")
        assert rows(cql.execute(f"select s from {t} where s like 'ab_' allow filtering")) == [["abb"], ["abc"]]
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"]]
        assert rows(cql.execute(f"select s from {t} where s like 'aaa' allow filtering")) == []

# Scylla-only because Cassandra does not treat '_' as a wildcard in LIKE
# patterns (only '%'), so the '_' patterns below match nothing there.
def test_like_operator_on_partition_key(cql, test_keyspace, scylla_only):
    # Fully constrained:
    with new_test_table(cql, test_keyspace, "s text primary key") as t:
        cql.execute(f"insert into {t} (s) values ('abc')")
        assert rows(cql.execute(f"select s from {t} where s like 'a__' allow filtering")) == [["abc"]]
        cql.execute(f"insert into {t} (s) values ('acc')")
        assert rows(cql.execute(f"select s from {t} where s like 'a__' allow filtering")) == [["abc"], ["acc"]]

    # Partially constrained:
    with new_test_table(cql, test_keyspace, "s1 text, s2 text, primary key((s1, s2))") as t:
        cql.execute(f"insert into {t} (s1, s2) values ('abc', 'abc')")
        assert rows(cql.execute(f"select s2 from {t} where s2 like 'a%' allow filtering")) == [["abc"]]
        cql.execute(f"insert into {t} (s1, s2) values ('aba', 'aba')")
        assert rows(cql.execute(f"select s2 from {t} where s2 like 'a%' allow filtering")) == [["aba"], ["abc"]]

def test_like_operator_on_clustering_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, s text, primary key(p, s)") as t:
        cql.execute(f"insert into {t} (p, s) values (1, 'abc')")
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"]]
        cql.execute(f"insert into {t} (p, s) values (2, 'acc')")
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"], ["acc"]]
        cql.execute(f"insert into {t} (p, s) values (2, 'acd')")
        assert rows(cql.execute(f"select s from {t} where p = 2 and s like '%c' allow filtering")) == [["acc"]]

# Scylla-only because Cassandra does not treat '_' as a wildcard in LIKE
# patterns (only '%'), and rejects the pattern '%' with "LIKE value can't
# be empty".
def test_like_operator_conjunction(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "s1 text primary key, s2 text") as t:
        cql.execute(f"insert into {t} (s1, s2) values ('abc', 'ABC')")
        cql.execute(f"insert into {t} (s1, s2) values ('a', 'A')")
        assert rows(cql.execute(f"select * from {t} where s1 like 'a%' and s2 like '__C' allow filtering")) == [["abc", "ABC"]]
        assert rows(cql.execute(f"select * from {t} where s1 like 'a%' and s1 like '__C' allow filtering")) == []
        assert rows(cql.execute(f"select s1 from {t} where s1 like 'a%' and s1 like '_' allow filtering")) == [["a"]]
        assert rows(cql.execute(f"select s1 from {t} where s1 like 'a%' and s1 like '%' allow filtering")) == [["a"], ["abc"]]
        assert rows(cql.execute(f"select s1 from {t} where s1 like 'a%' and s1 like '_b_' and s1 like '%c' allow filtering")) == [["abc"]]
        assert rows(cql.execute(f"select s1 from {t} where s1 like 'a%' and s1 = 'abc' allow filtering")) == [["abc"]]

# Scylla-only because Cassandra rejects the pattern '%' with "LIKE value
# can't be empty".
def test_like_operator_static_column(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int, c text, s text static, primary key(p, c)") as t:
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == []
        cql.execute(f"insert into {t} (p, s) values (1, 'abc')")
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"]]
        assert rows(cql.execute(f"select * from {t} where c like '%' allow filtering")) == []

# Scylla-only because Cassandra does not treat '_' as a wildcard in LIKE
# patterns (only '%'), so the '_b_' pattern below matches nothing there.
def test_like_operator_bind_marker(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "s text primary key") as t:
        cql.execute(f"insert into {t} (s) values ('abc')")
        stmt = cql.prepare(f"select s from {t} where s like ? allow filtering")
        assert rows(cql.execute(stmt, ["_b_"])) == [["abc"]]
        assert rows(cql.execute(stmt, ["%g"])) == []
        assert rows(cql.execute(stmt, ["%c"])) == [["abc"]]

# Scylla-only because Cassandra rejects the empty pattern with "LIKE value
# can't be empty".
def test_like_operator_blank_pattern(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int primary key, s text") as t:
        cql.execute(f"insert into {t} (p, s) values (1, 'abc')")
        assert rows(cql.execute(f"select s from {t} where s like '' allow filtering")) == []
        cql.execute(f"insert into {t} (p, s) values (2, '')")
        assert rows(cql.execute(f"select s from {t} where s like '' allow filtering")) == [[""]]

def test_like_operator_ascii(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "s ascii primary key") as t:
        cql.execute(f"insert into {t} (s) values ('abc')")
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"]]

def test_like_operator_varchar(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "s varchar primary key") as t:
        cql.execute(f"insert into {t} (s) values ('abc')")
        assert rows(cql.execute(f"select s from {t} where s like '%c' allow filtering")) == [["abc"]]

# A column of a non-string type cannot be the LHS of the LIKE operator.
# Scylla-only because Cassandra accepts LIKE on the numeric, timestamp,
# date and time columns, for which 123 is a valid literal, and rejects it
# on the other types only because 123 is not a valid literal for them.
@pytest.mark.parametrize("type", ["bigint", "blob", "boolean", "counter", "decimal", "double", "duration", "float", "inet", "int",
                                  "smallint", "timestamp", "tinyint", "uuid", "varint", "timeuuid", "date", "time"])
def test_like_operator_fails_on_non_string(cql, test_keyspace, scylla_only, type):
    with new_test_table(cql, test_keyspace, f"k {type}, p int primary key") as t:
        with pytest.raises(InvalidRequest, match="only on string types"):
            cql.execute(f"select * from {t} where k like 123 allow filtering")

# Scylla-only because Cassandra's grammar does not allow LIKE on token(),
# so it fails with a syntax error rather than an InvalidRequest.
def test_like_operator_on_token(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "s text primary key") as t:
        with pytest.raises(InvalidRequest, match="token function"):
            cql.execute(f"select * from {t} where token(s) like 'abc' allow filtering")
