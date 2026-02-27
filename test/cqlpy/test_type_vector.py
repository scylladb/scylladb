# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests involving the "vector" column type.
from .util import new_test_table


def test_vector_of_set_using_arguments_binding(cql, test_keyspace):
    """
    Tests that inserting and reading a vector of sets using argument binding
    succeeds.

    This test reproduces a problem where deserializing a value of a
    vector of sets incorrectly throws std::bad_cast exception.

    See issue #26704.
    """
    with new_test_table(cql, test_keyspace, "p int primary key, v vector<set<int>,2>") as table:
        prepared = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
        value_to_insert = [{1, 2, 3}, {4, 5, 6}]

        cql.execute(prepared, [0, value_to_insert])
        row = cql.execute(f"SELECT v FROM {table}").one()

        assert row is not None
        assert row.v == value_to_insert
