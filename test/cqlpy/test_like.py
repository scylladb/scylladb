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
