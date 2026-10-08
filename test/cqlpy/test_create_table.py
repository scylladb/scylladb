# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the CREATE TABLE statement

import pytest

from cassandra.protocol import ConfigurationException
from .util import unique_name

def test_prepared_create_table_options_validated(cql, test_keyspace):
    """
    Scylla's CREATE TABLE validates its options when the statement is
    prepared, so nothing is cached before the checks run and a later
    execution cannot skip them. Cassandra validates them only when the
    statement is executed. Either is fine, as long as executing the
    prepared statement - not just the first time - doesn't succeed.
    """
    stmt = (f"CREATE TABLE {test_keyspace}.{unique_name()} (p int PRIMARY KEY)"
            " WITH compaction = {'class': 'SizeTieredCompactionStrategy'} AND min_index_interval = 0")
    try:
        prepared = cql.prepare(stmt)
    except ConfigurationException as e:
        assert "min_index_interval" in str(e)
        return
    for _ in range(2):
        with pytest.raises(ConfigurationException, match="min_index_interval"):
            cql.execute(prepared)
