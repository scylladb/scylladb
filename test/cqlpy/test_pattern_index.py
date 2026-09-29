# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for pattern indexes
#
# This file tests the pattern_index custom index class: schema and options
# validation.
###############################################################################

import pytest
from test.pylib.skip_types import skip_env
from .util import new_test_table, new_test_keyspace, unique_name
from cassandra.protocol import InvalidRequest


# Pattern search is not allowed in tables using vnodes, so all tests in this file need tablets
@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Pattern Search needs tablets enabled")


@pytest.mark.parametrize("column_type", ["text", "varchar", "ascii"])
def test_create_pattern_index_on_supported_text_column(cql, test_keyspace, column_type):
    """Pattern index should accept all textual CQL columns."""
    schema = f'p int primary key, title {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_create_pattern_index_uppercase_class(cql, test_keyspace):
    """Custom index class name lookup is case-insensitive."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'PATTERN_INDEX'")


@pytest.mark.parametrize("column_type", ["int", "blob", "list<text>", "vector<float, 3>"])
def test_create_pattern_index_on_unsupported_column_fails(cql, test_keyspace, column_type):
    """Pattern index must reject non-text column types."""
    schema = f'p int primary key, v {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Pattern index is only supported on text, varchar, or ascii columns"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'pattern_index'")


def test_create_pattern_index_on_key_or_static_column_fails(cql, test_keyspace):
    """Only regular columns can be indexed: key and static columns are rejected."""
    schema = 'p1 text, p2 text, c text, s text static, v text, PRIMARY KEY ((p1, p2), c)'
    with new_test_table(cql, test_keyspace, schema, "WITH CLUSTERING ORDER BY (c DESC)") as table:
        for column in ['p1', 'c', 's']:
            with pytest.raises(InvalidRequest, match="Pattern index is only supported on regular columns"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}({column}) USING 'pattern_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'pattern_index'")


def test_create_pattern_index_with_valid_options(cql, test_keyspace):
    """The only option is accepted; boolean values are case-insensitive."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'case_sensitive': 'FALSE'}}"
        )


def test_create_pattern_index_with_bad_option_value_fails(cql, test_keyspace):
    """case_sensitive must be a boolean."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Invalid value in option 'case_sensitive'"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
                f"WITH OPTIONS = {{'case_sensitive': 'maybe'}}"
            )


def test_create_pattern_index_with_unsupported_option_fails(cql, test_keyspace):
    """Unknown WITH OPTIONS keys should be rejected."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported option bad_option for pattern index"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
                f"WITH OPTIONS = {{'bad_option': 'bad_value'}}"
            )


def test_no_view_for_pattern_index(cql, test_keyspace):
    """A pattern index lives on the index node, not in a materialized view."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        views = list(cql.execute(
            f"SELECT view_name FROM system_schema.views "
            f"WHERE keyspace_name = '{test_keyspace}' AND base_table_name = '{table.split('.')[1]}' ALLOW FILTERING"))
        assert views == []


def test_describe_pattern_index(cql, test_keyspace):
    """DESCRIBE reproduces the class and the options."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'case_sensitive': 'false'}}"
        )
        desc = cql.execute(f"DESCRIBE INDEX {test_keyspace}.{index_name}").one().create_statement
        assert "USING 'pattern_index'" in desc
        assert "'case_sensitive': 'false'" in desc


def test_pattern_index_in_system_schema(cql, test_keyspace):
    """The index is recorded with its class and options, the way the Vector Store reads them."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'case_sensitive': 'false'}}"
        )
        table_name = table.split('.')[1]
        row = cql.execute(
            f"SELECT kind, options FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' "
            f"AND table_name = '{table_name}' AND index_name = '{index_name}'").one()
        assert row.kind == 'CUSTOM'
        assert row.options['class_name'] == 'pattern_index'
        assert row.options['target'] == 'title'
        assert row.options['case_sensitive'] == 'false'


def test_create_pattern_index_requires_tablets(cql, this_dc):
    """Pattern index creation must fail when the keyspace does not use tablets."""
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks:
        with new_test_table(cql, ks, 'p int primary key, title text') as table:
            with pytest.raises(InvalidRequest, match="Creating a pattern index requires the base table's keyspace to use tablets"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_create_pattern_index_cdc_low_ttl_fails(cql, test_keyspace):
    """Pattern index creation must fail when CDC TTL is below the 24-hour minimum."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'ttl': 1}") as table:
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_create_pattern_index_cdc_bad_delta_mode_fails(cql, test_keyspace):
    """Pattern index creation must fail when CDC delta mode is not 'full' and postimage is off."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'delta': 'keys'}") as table:
        with pytest.raises(InvalidRequest, match="delta mode must be set to 'full' or postimage must be enabled"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_cannot_disable_cdc_with_pattern_index(cql, test_keyspace):
    """ALTER TABLE to disable CDC must fail when a pattern index exists on the table."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Pattern Search is enabled"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")


def test_alter_cdc_low_ttl_with_pattern_index_fails(cql, test_keyspace):
    """ALTER TABLE to set CDC TTL below the 24-hour minimum must fail when a pattern index exists."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true, 'ttl': 1}}")


def test_drop_pattern_index(cql, test_keyspace):
    """DROP INDEX on a pattern index should succeed, and CDC can then be disabled again."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index'")
        cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")
