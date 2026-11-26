# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the CREATE TABLE statement

import pytest

from cassandra.protocol import ConfigurationException, InvalidRequest
from .util import unique_name

def test_prepared_create_table_options_validated(cql, test_keyspace):
    """
    CREATE TABLE validates its options when the statement is prepared,
    so nothing is cached before the checks run and a later execution
    cannot skip them.
    """
    with pytest.raises(ConfigurationException, match="min_index_interval"):
        cql.prepare(f"CREATE TABLE {test_keyspace}.{unique_name()} (p int PRIMARY KEY)"
                    " WITH compaction = {'class': 'LeveledCompactionStrategy'} AND min_index_interval = 0")


def test_prepared_create_table_after_keyspace_dropped(cql):
    """
    A prepared CREATE TABLE stays cached when its keyspace is dropped.
    Executing it afterwards must fail with InvalidRequest, not with a
    server error.
    """
    keyspace = unique_name()
    cql.execute(f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    create_table = cql.prepare(f"CREATE TABLE {keyspace}.t (pk int PRIMARY KEY)")
    cql.execute(f"DROP KEYSPACE {keyspace}")

    with pytest.raises(InvalidRequest, match=f"Can't find a keyspace {keyspace}"):
        cql.execute(create_table)
