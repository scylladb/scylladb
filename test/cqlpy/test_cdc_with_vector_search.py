# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from cassandra.protocol import InvalidRequest

from .util import new_test_table
import pytest


VS_TTL_SECONDS = 86400  # 24 hours


def create_cdc(cql, table, options):
    try:
        cql.execute(f"ALTER TABLE {table} WITH cdc = {options}")
    except InvalidRequest as e:
        with pytest.raises(InvalidRequest, match="CDC log must meet the minimal requirements of Vector Search"):
            raise e
        return False
    return True


def create_index(cql, test_keyspace, table, column):
    query = f"CREATE INDEX v_idx ON {table} ({column}) USING 'vector_index'"
    try:
        cql.execute(query)
    except InvalidRequest as e:
        with pytest.raises(InvalidRequest, match="CDC log must meet the minimal requirements of Vector Search"):
            raise e
        return False
    cql.execute(f"DROP INDEX {test_keyspace}.v_idx")
    return True


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_create_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # The vector index requires CDC to be enabled with specific options:
        # - TTL must be at least 24 hours (86400 seconds)
        # - delta mode must be set to 'full'

        # Enable Vector Search by creating a vector index.
        cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Allow creating CDC log table with default options
        assert create_cdc(cql, table, {"enabled": True})

        # Disallow changing CDC's TTL to less than 24 hours
        assert not create_cdc(cql, table, {"enabled": True, "ttl": 1})
        assert create_cdc(cql, table, {"enabled": True, "ttl": 86400})

        # Allow changing CDC's TTL to 0 (infinite)
        assert create_cdc(cql, table, {"enabled": True, "ttl": 0})

        # Disallow changing CDC's delta to 'keys'
        assert not create_cdc(cql, table, {"enabled": True, "delta": "keys"})
        assert create_cdc(cql, table, {"enabled": True, "delta": "full"})

        # Allow changing CDC's preimage and postimage
        assert create_cdc(cql, table, {"enabled": True, "preimage": True})
        assert create_cdc(cql, table, {"enabled": True, "postimage": True})


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_disable_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Enable Vector Search by creating a vector index.
        cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Disallow disabling CDC when Vector Search is enabled
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Vector Search is enabled on the table"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': False}}")


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_enable_vector_search_with_cdc_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # The vector index requires CDC to be enabled with specific options:
        # - TTL must be at least 24 hours (86400 seconds)
        # - delta mode must be set to 'full'

        # Disallow creating the vector index when CDC's TTL is less than 24h
        assert create_cdc(cql, table, {'enabled': True, 'ttl': 1})
        assert not create_index(cql, test_keyspace, table, "v")

        # Allow creating the vector index when CDC's TTL is 0 (infinite)
        assert create_cdc(cql, table, {'enabled': True, 'ttl': 0})
        assert create_index(cql, test_keyspace, table, "v")

        # Disallow creating the vector index when CDC's delta is set to 'keys'
        assert create_cdc(cql, table, {'enabled': True, 'delta': 'keys'})
        assert not create_index(cql, test_keyspace, table, "v")

        # Allow creating the vector index when CDC's options fulfill the minimal requirements of Vector Search
        assert create_cdc(cql, table, {'enabled': True, 'ttl': 172800, 'delta': 'full', 'preimage': True, 'postimage': True})
        assert create_index(cql, test_keyspace, table, "v")
