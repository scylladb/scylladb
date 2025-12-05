# -*- coding: utf-8 -*-
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#############################################################################
# Tests for GROUP0 BATCH operations
#############################################################################
from cassandra import InvalidRequest
import pytest


def test_group0_batch_syntax_error_for_non_system_table(cql, test_keyspace):
    """Verifies that GROUP0 BATCH can only be used with system keyspace tables"""
    with pytest.raises(InvalidRequest, match="GROUP0 BATCH can only modify system keyspace tables"):
        # Create a test table in a non-system keyspace
        table_name = f"{test_keyspace}.test_table"
        cql.execute(f"CREATE TABLE {table_name} (k int PRIMARY KEY, v int)")
        try:
            # Try to use GROUP0 BATCH with a non-system table
            cql.execute(f"""
                BEGIN GROUP0 BATCH
                INSERT INTO {table_name} (k, v) VALUES (1, 1)
                APPLY BATCH
            """)
        finally:
            cql.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_group0_batch_with_timestamp_error(cql):
    """Verifies that GROUP0 BATCH cannot have custom timestamp"""
    with pytest.raises(InvalidRequest, match="Cannot provide custom timestamp for GROUP0 BATCH"):
        cql.execute("""
            BEGIN GROUP0 BATCH USING TIMESTAMP 12345
            INSERT INTO system.topology (key) VALUES ('test')
            APPLY BATCH
        """)


def test_group0_batch_with_conditions_error(cql):
    """Verifies that GROUP0 BATCH cannot have conditions"""
    with pytest.raises(InvalidRequest, match="Cannot use conditions in GROUP0 BATCH"):
        cql.execute("""
            BEGIN GROUP0 BATCH
            INSERT INTO system.topology (key) VALUES ('test') IF NOT EXISTS
            APPLY BATCH
        """)


def test_group0_batch_basic_syntax(cql):
    """Verifies that GROUP0 BATCH has correct basic syntax"""
    # This test just checks that the syntax is recognized
    # The actual execution will fail if not properly set up with group0
    # but the syntax should be accepted
    try:
        cql.execute("""
            BEGIN GROUP0 BATCH
            APPLY BATCH
        """)
    except Exception as e:
        # Accept either success or group0-related errors, but not syntax errors
        error_msg = str(e).lower()
        assert "syntax" not in error_msg and "unexpected" not in error_msg, \
            f"GROUP0 BATCH should be valid syntax, but got: {e}"
