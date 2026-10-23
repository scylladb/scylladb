# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the error a client gets when it uses a table or a keyspace that
# does not exist. Scylla reported these with different messages depending on
# the statement, so a client had to match each of them. The tests check that
# every statement gives one message for each case:
#   - a missing table: "Table 'ks.t' does not exist"
#   - a missing keyspace: "Keyspace 'ks' does not exist"
# Cassandra words these messages differently from statement to statement, so
# on Cassandra the tests check only that the error is InvalidRequest.
#
# Reproduces SCYLLADB-5624.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest

from .util import is_scylla, new_cql


@pytest.mark.xfail(reason="SCYLLADB-5624")
def test_select_from_missing_table(cql, test_keyspace):
    with pytest.raises(InvalidRequest) as error:
        cql.execute(f"SELECT * FROM {test_keyspace}.no_such_table")
    if is_scylla(cql):
        assert f"Table '{test_keyspace}.no_such_table' does not exist" in str(error.value)


@pytest.mark.xfail(reason="SCYLLADB-5624")
def test_insert_into_missing_table(cql, test_keyspace):
    with pytest.raises(InvalidRequest) as error:
        cql.execute(f"INSERT INTO {test_keyspace}.no_such_table (p) VALUES (1)")
    if is_scylla(cql):
        assert f"Table '{test_keyspace}.no_such_table' does not exist" in str(error.value)


def test_select_from_missing_keyspace(cql):
    with pytest.raises(InvalidRequest) as error:
        cql.execute("SELECT * FROM no_such_keyspace.no_such_table")
    if is_scylla(cql):
        assert "Keyspace 'no_such_keyspace' does not exist" in str(error.value)


def test_drop_index_in_missing_keyspace(cql):
    with pytest.raises(InvalidRequest) as error:
        cql.execute("DROP INDEX no_such_keyspace.no_such_index")
    if is_scylla(cql):
        assert "Keyspace 'no_such_keyspace' does not exist" in str(error.value)


def test_use_missing_keyspace(cql):
    with new_cql(cql) as ncql:
        with pytest.raises(InvalidRequest) as error:
            ncql.execute("USE no_such_keyspace")
    if is_scylla(cql):
        assert "Keyspace 'no_such_keyspace' does not exist" in str(error.value)
