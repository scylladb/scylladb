# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the CREATE TABLE statement

import pytest

from cassandra.protocol import ConfigurationException
from .util import unique_name

def test_prepared_create_table_options_validated(cql, test_keyspace):
    """
    CREATE TABLE validates its options when the statement is prepared,
    so nothing is cached before the checks run and a later execution
    cannot skip them.
    """
    with pytest.raises(ConfigurationException, match="min_index_interval"):
        cql.prepare(f"CREATE TABLE {test_keyspace}.{unique_name()} (p int PRIMARY KEY)"
                    " WITH compaction = {'class': 'SizeTieredCompactionStrategy'} AND min_index_interval = 0")
