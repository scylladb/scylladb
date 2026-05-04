# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for fulltext indexes
#
# This file tests the fulltext_index custom index class mainly covering schema
# and options validation.
# It does not test the actual indexing or querying behavior.
###############################################################################

import pytest
import re
from .util import new_test_table, new_test_keyspace, unique_name
from cassandra.protocol import InvalidRequest

# Fulltext search is not allowed in tables using vnodes, so all tests in this file need tablets
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_tablets_and_scylla_only(skip_without_tablets, scylla_only):
    pass


@pytest.mark.parametrize("column_type", ["text", "varchar", "ascii"])
def test_create_fulltext_index_on_supported_text_column(cql, test_keyspace, column_type):
    """Fulltext index should accept all supported textual CQL columns."""
    schema = f'p int primary key, content {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")


def test_create_fulltext_index_uppercase_class(cql, test_keyspace):
    """Custom index class name lookup is case-insensitive."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'FULLTEXT_INDEX'")


@pytest.mark.parametrize("column_type", ["int", "blob", "vector<float, 3>"])
def test_create_fulltext_index_on_unsupported_column_fails(cql, test_keyspace, column_type):
    """Fulltext index must reject non-text column types."""
    schema = f'p int primary key, v {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Fulltext index is only supported on text, varchar, or ascii columns"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'fulltext_index'")


def test_create_fulltext_index_with_analyzer_option(cql, test_keyspace):
    """All supported analyzer values should be accepted."""
    analyzers = [
        'standard', 'english', 'german', 'french', 'spanish', 'italian',
        'portuguese', 'russian', 'simple', 'whitespace',
    ]
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for analyzer in analyzers:
            index_name = unique_name()
            cql.execute(
                f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index' "
                f"WITH OPTIONS = {{'analyzer': '{analyzer}'}}"
            )
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")


def test_create_fulltext_index_with_cjk_analyzers_fails(cql, test_keyspace):
    """CJK analyzers should be rejected. (VECTOR-672)"""
    analyzers = [
        'chinese', 'japanese', 'korean',
    ]
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for analyzer in analyzers:
            index_name = unique_name()
            with pytest.raises(InvalidRequest, match="Invalid value in option 'analyzer'"):
                cql.execute(
                    f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index' "
                    f"WITH OPTIONS = {{'analyzer': '{analyzer}'}}"
                )


def test_create_fulltext_index_with_analyzer_case_insensitive(cql, test_keyspace):
    """Analyzer option values should be case-insensitive."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index' "
            f"WITH OPTIONS = {{'analyzer': 'ENGLISH'}}"
        )


def test_create_fulltext_index_with_bad_analyzer_fails(cql, test_keyspace):
    """Unsupported analyzer values should be rejected."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Invalid value in option 'analyzer'"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index' "
                f"WITH OPTIONS = {{'analyzer': 'nonexistent_analyzer'}}"
            )


def test_create_fulltext_index_with_positions_option(cql, test_keyspace):
    """positions option accepts 'true' and 'false'."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for val in ['true', 'false']:
            index_name = unique_name()
            cql.execute(
                f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index' "
                f"WITH OPTIONS = {{'positions': '{val}'}}"
            )
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")


def test_create_fulltext_index_with_bad_positions_fails(cql, test_keyspace):
    """Invalid positions value should be rejected."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Invalid value in option 'positions'"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index' "
                f"WITH OPTIONS = {{'positions': 'maybe'}}"
            )


def test_create_fulltext_index_with_unsupported_option_fails(cql, test_keyspace):
    """Unknown WITH OPTIONS keys should be rejected."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported option bad_option for fulltext index"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index' "
                f"WITH OPTIONS = {{'bad_option': 'bad_value'}}"
            )


def test_create_fulltext_index_with_multiple_options(cql, test_keyspace):
    """Multiple valid options should be accepted together."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index' "
            f"WITH OPTIONS = {{'analyzer': 'english', 'positions': 'false'}}"
        )


def test_no_view_for_fulltext_index(cql, test_keyspace):
    """Fulltext index should not create a backing materialized view."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        table_name = table.split('.')[1]
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        result = cql.execute(
            f"SELECT * FROM system_schema.views "
            f"WHERE keyspace_name = '{test_keyspace}' AND base_table_name = '{table_name}' "
            f"ALLOW FILTERING"
        )
        assert len(result.current_rows) == 0, \
            "Fulltext index should not create a view in system_schema.views"


def test_describe_fulltext_index(cql, test_keyspace):
    """DESCRIBE INDEX should output correct CQL with USING 'fulltext_index'."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index' "
            f"WITH OPTIONS = {{'analyzer': 'english'}}"
        )
        desc = cql.execute(f"DESCRIBE INDEX {test_keyspace}.{index_name}")
        desc_stmt = desc.one().create_statement
        expected_desc_pattern = (
            rf"CREATE CUSTOM INDEX {re.escape(index_name)} "
            rf"ON {re.escape(table)}\(content\) "
            r"USING 'fulltext_index' "
            r"WITH OPTIONS = "
            r"\{'analyzer': 'english'\};?"
        )
        assert re.fullmatch(expected_desc_pattern, desc_stmt)


def test_describe_fulltext_index_no_options(cql, test_keyspace):
    """DESCRIBE INDEX without user options should not include WITH OPTIONS."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index'"
        )
        desc = cql.execute(f"DESCRIBE INDEX {test_keyspace}.{index_name}")
        desc_stmt = desc.one().create_statement
        assert "USING 'fulltext_index'" in desc_stmt
        assert "WITH OPTIONS" not in desc_stmt


def test_create_fulltext_index_if_not_exists(cql, test_keyspace):
    """IF NOT EXISTS should silently succeed when the index already exists."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        # Second creation with IF NOT EXISTS should not raise
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ON {table}(content) USING 'fulltext_index'")


def test_fulltext_index_in_system_schema(cql, test_keyspace):
    """Verify fulltext index metadata is stored in system_schema.indexes."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(content) USING 'fulltext_index' "
            f"WITH OPTIONS = {{'analyzer': 'standard'}}"
        )
        result = cql.execute(
            f"SELECT * FROM system_schema.indexes "
            f"WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table.split('.')[1]}' "
            f"AND index_name = '{index_name}'"
        )
        rows = result.current_rows
        assert len(rows) == 1
        options = rows[0].options
        assert options['class_name'] == 'fulltext_index'
        assert options.get('analyzer') == 'standard'


def test_create_fulltext_index_requires_tablets(cql, this_dc):
    """Fulltext index creation must fail when the keyspace does not use tablets."""
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks:
        with new_test_table(cql, ks, 'p int primary key, content text') as table:
            with pytest.raises(InvalidRequest, match="Creating a fulltext index requires the base table's keyspace to use tablets"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
