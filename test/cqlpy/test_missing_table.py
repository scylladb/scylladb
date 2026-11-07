# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the error a client gets when it uses a table or a keyspace that
# does not exist. A missing table and a missing keyspace each have one message
# on every path, so clients can recognise them. Cassandra words them differently
# from statement to statement, so these tests are Scylla-only.
#############################################################################

import re

import pytest
from cassandra.protocol import InvalidRequest

from .util import new_cql


def test_select_from_missing_table(scylla_only, cql, test_keyspace):
    with pytest.raises(InvalidRequest, match=re.escape(f"Table '{test_keyspace}.no_such_table' does not exist")):
        cql.execute(f"SELECT * FROM {test_keyspace}.no_such_table")


def test_insert_into_missing_table(scylla_only, cql, test_keyspace):
    with pytest.raises(InvalidRequest, match=re.escape(f"Table '{test_keyspace}.no_such_table' does not exist")):
        cql.execute(f"INSERT INTO {test_keyspace}.no_such_table (p) VALUES (1)")


def test_select_from_missing_keyspace(scylla_only, cql):
    with pytest.raises(InvalidRequest, match=re.escape("Keyspace 'no_such_keyspace' does not exist")):
        cql.execute("SELECT * FROM no_such_keyspace.no_such_table")


def test_drop_index_in_missing_keyspace(scylla_only, cql):
    with pytest.raises(InvalidRequest, match=re.escape("Keyspace 'no_such_keyspace' does not exist")):
        cql.execute("DROP INDEX no_such_keyspace.no_such_index")


def test_use_missing_keyspace(scylla_only, cql):
    with new_cql(cql) as ncql:
        with pytest.raises(InvalidRequest, match=re.escape("Keyspace 'no_such_keyspace' does not exist")):
            ncql.execute("USE no_such_keyspace")
