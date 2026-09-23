# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for various CQL statements, converted from test/boost/cql_query_test.cc.
#############################################################################

from .util import unique_name


def test_create_keyspace_statement(cql):
    ks = unique_name()
    cql.execute(f"create keyspace {ks} with replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }};")
    try:
        assert list(cql.execute(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")) == [(ks,)]
    finally:
        cql.execute(f"DROP KEYSPACE {ks}")
