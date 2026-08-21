# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the speculative_retry_user_table_default configuration option.
# The option is live-updatable, so the tests set it through the system.config
# virtual table and restore the original value when done. The tests are
# scylla_only because the option (and system.config) do not exist in Cassandra.
#############################################################################

import json
from contextlib import contextmanager

from .util import new_test_table, new_materialized_view

@contextmanager
def temporary_config_value(cql, name, value):
    original = json.loads(cql.execute(f"SELECT value FROM system.config WHERE name = '{name}'").one().value)
    cql.execute(f"UPDATE system.config SET value = '{value}' WHERE name = '{name}'")
    try:
        yield
    finally:
        cql.execute(f"UPDATE system.config SET value = '{original}' WHERE name = '{name}'")

def get_speculative_retry(cql, schema_table, name_column, ks, name):
    r = list(cql.execute(f"SELECT speculative_retry FROM system_schema.{schema_table} WHERE keyspace_name = '{ks}' AND {name_column} = '{name}'"))
    assert len(r) == 1
    return r[0].speculative_retry

# Check that the configured default applies to a base table and all its
# auxiliary tables (materialized view, secondary index and CDC log), and that
# an explicit speculative_retry table option overrides it.
def test_tables_respect_speculative_retry_config(cql, test_keyspace, scylla_only):
    with temporary_config_value(cql, 'speculative_retry_user_table_default', '200ms'):
        with new_test_table(cql, test_keyspace, "p int primary key, v int", "with cdc = {'enabled': true}") as table:
            with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
                cql.execute(f'CREATE INDEX ON {table}(v)')
                ks, cf = table.split('.')
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf) == '200.00ms'
                _, view = mv.split('.')
                assert get_speculative_retry(cql, 'views', 'view_name', ks, view) == '200.00ms'
                assert get_speculative_retry(cql, 'views', 'view_name', ks, f'{cf}_v_idx_index') == '200.00ms'
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, f'{cf}_scylla_cdc_log') == '200.00ms'
        with new_test_table(cql, test_keyspace, "p int primary key, v int", "with speculative_retry = '37.0PERCENTILE'") as table:
            ks, cf = table.split('.')
            assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf) == '37.0PERCENTILE'
            # ALTER TABLE overrides the configured default as well
            cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '50ms'")
            assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf) == '50.00ms'
            # and the value is retained when altering an unrelated property
            cql.execute(f"ALTER TABLE {table} WITH comment = 'test comment'")
            assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf) == '50.00ms'

# Check that the option is live-updatable: an updated value applies to tables
# created after the update, and an invalid updated value is ignored, keeping
# the last valid value.
def test_live_update_speculative_retry_config(cql, test_keyspace, scylla_only):
    with temporary_config_value(cql, 'speculative_retry_user_table_default', '200ms'):
        with new_test_table(cql, test_keyspace, "p int primary key, v int") as t1:
            ks, cf1 = t1.split('.')
            assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf1) == '200.00ms'
            cql.execute("UPDATE system.config SET value = 'NONE' WHERE name = 'speculative_retry_user_table_default'")
            with new_test_table(cql, test_keyspace, "p int primary key, v int") as t2:
                _, cf2 = t2.split('.')
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf2) == 'NONE'
                # Tables created before the update are not affected
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf1) == '200.00ms'
            cql.execute("UPDATE system.config SET value = 'dog' WHERE name = 'speculative_retry_user_table_default'")
            with new_test_table(cql, test_keyspace, "p int primary key, v int") as t3:
                _, cf3 = t3.split('.')
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf3) == 'NONE'
            # A subsequent valid update applies again
            cql.execute("UPDATE system.config SET value = '50ms' WHERE name = 'speculative_retry_user_table_default'")
            with new_test_table(cql, test_keyspace, "p int primary key, v int") as t4:
                _, cf4 = t4.split('.')
                assert get_speculative_retry(cql, 'tables', 'table_name', ks, cf4) == '50.00ms'
