# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from contextlib import ExitStack
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

from .util import config_value_context, new_test_table, ScyllaMetrics

# All tests in this file are Scylla-only. There are at least two
# differences in how `write_consistency_levels_warned` works
# in ScyllaDB versus Cassandra. In ScyllaDB, guardrails apply to all
# roles (users), whereas in Cassandra, they are not applied to the
# system user or superuser. Moreover, in Cassandra, a query warning
# is added to each response of a warned query, whereas in ScyllaDB,
# only a metric is incremented. This avoids performance deterioration
# in existing clusters if write queries with CL=ONE or CL=LOCAL_ONE are
# used massively. Due to these differences, these tests will not pass
# with Cassandra, so let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

REGULAR_SCHEMA = "pk int PRIMARY KEY, v int"
COUNTER_SCHEMA = "pk int PRIMARY KEY, c counter"

LOGGED_BATCH = """
BEGIN BATCH
    INSERT INTO {table} (pk, v) VALUES (2, 2);
    UPDATE {table} SET v = 3 WHERE pk = 3;
APPLY BATCH
"""

UNLOGGED_BATCH = """
BEGIN UNLOGGED BATCH
    INSERT INTO {table} (pk, v) VALUES (4, 4);
    UPDATE {table} SET v = 5 WHERE pk = 5;
APPLY BATCH
"""

CONDITIONAL_BATCH = """
BEGIN BATCH
    INSERT INTO {table} (pk, v) VALUES (4, 4) IF NOT EXISTS;
APPLY BATCH
"""

WARNED_METRIC = "scylla_cql_write_consistency_levels_warned_violations"
DISALLOWED_METRIC = "scylla_cql_write_consistency_levels_disallowed_violations"

def get_metric(cql, name, cl=None):
    labels = {"consistency_level": ConsistencyLevel.value_to_name[cl]} if cl is not None else None
    return ScyllaMetrics.query(cql).get(name, labels=labels) or 0

def check_warned(cql, test_keyspace, query, cl=ConsistencyLevel.ONE, schema=REGULAR_SCHEMA):
    with new_test_table(cql, test_keyspace, schema=schema) as table:
        with config_value_context(cql, "write_consistency_levels_warned", ConsistencyLevel.value_to_name[cl]):
            before = get_metric(cql, WARNED_METRIC, cl)
            stmt = SimpleStatement(query.format(table=table), consistency_level=cl)
            cql.execute(stmt)
            after = get_metric(cql, WARNED_METRIC, cl)
            assert after > before, f"Expected {WARNED_METRIC} for {ConsistencyLevel.value_to_name[cl]} to increase, but got {before} -> {after}"

def check_disallowed(cql, test_keyspace, query, cl=ConsistencyLevel.ONE, schema=REGULAR_SCHEMA):
    with new_test_table(cql, test_keyspace, schema=schema) as table:
        with config_value_context(cql, "write_consistency_levels_disallowed", ConsistencyLevel.value_to_name[cl]):
            before = get_metric(cql, DISALLOWED_METRIC, cl)
            stmt = SimpleStatement(query.format(table=table), consistency_level=cl)
            with pytest.raises(InvalidRequest, match="(?i)not allowed"):
                cql.execute(stmt)
            after = get_metric(cql, DISALLOWED_METRIC, cl)
            assert after > before, f"Expected {DISALLOWED_METRIC} for {ConsistencyLevel.value_to_name[cl]} to increase, but got {before} -> {after}"

def check_no_warning(cql, test_keyspace, query, cl=ConsistencyLevel.ONE,
                     warned="", disallowed="", schema=REGULAR_SCHEMA):
    with new_test_table(cql, test_keyspace, schema=schema) as table:
        with ExitStack() as config:
            config.enter_context(config_value_context(cql, "write_consistency_levels_warned", warned))
            config.enter_context(config_value_context(cql, "write_consistency_levels_disallowed", disallowed))
            before_warned = get_metric(cql, WARNED_METRIC, cl)
            before_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)
            stmt = SimpleStatement(query.format(table=table), consistency_level=cl)
            cql.execute(stmt)
            after_warned = get_metric(cql, WARNED_METRIC, cl)
            after_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)
            assert after_warned == before_warned, f"Expected {WARNED_METRIC} for {ConsistencyLevel.value_to_name[cl]} unchanged, but got {before_warned} -> {after_warned}"
            assert after_disallowed == before_disallowed, f"Expected {DISALLOWED_METRIC} for {ConsistencyLevel.value_to_name[cl]} unchanged, but got {before_disallowed} -> {after_disallowed}"

def check_guardrailed(cql, test_keyspace, query, schema=REGULAR_SCHEMA):
    check_warned(cql, test_keyspace, query, schema=schema)
    check_disallowed(cql, test_keyspace, query, schema=schema)

def test_write_cl_insert(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, "INSERT INTO {table} (pk, v) VALUES (1, 1)")

def test_write_cl_update(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, "UPDATE {table} SET v = 2 WHERE pk = 1")

def test_write_cl_select_not_affected(cql, test_keyspace):
    check_no_warning(cql, test_keyspace, "SELECT * FROM {table} WHERE pk = 1", cl=ConsistencyLevel.QUORUM, warned="QUORUM")

def test_write_cl_delete(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, "DELETE FROM {table} WHERE pk = 1")

def test_write_cl_logged_batch(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, LOGGED_BATCH)

def test_write_cl_unlogged_batch(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, UNLOGGED_BATCH)

def test_write_cl_conditional_batch(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, CONDITIONAL_BATCH)

def test_write_cl_counter(cql, test_keyspace):
    check_guardrailed(cql, test_keyspace, "UPDATE {table} SET c = c + 1 WHERE pk = 1", schema=COUNTER_SCHEMA)

def test_write_cl_non_guardrailed_no_warning(cql, test_keyspace):
    check_no_warning(cql, test_keyspace, "INSERT INTO {table} (pk, v) VALUES (4, 4)", cl=ConsistencyLevel.ALL, warned="QUORUM", disallowed="ANY")

def test_write_cl_non_guardrailed_no_warning(cql, test_keyspace):
    check_no_warning(cql, test_keyspace, "INSERT INTO {table} (pk, v) VALUES (4, 4)", warned="", disallowed="")

def test_write_cl_case_insensitive(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, schema=REGULAR_SCHEMA) as table:
        for variant in ["any", "Any", "ANY"]:
            with config_value_context(cql, "write_consistency_levels_disallowed", variant):
                stmt = SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY)
                with pytest.raises(InvalidRequest):
                    cql.execute(stmt)

def test_write_cl_invalid_level_ignored(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, schema=REGULAR_SCHEMA) as table:
        with ExitStack() as config:
            config.enter_context(config_value_context(cql, "write_consistency_levels_disallowed", "INVALID_CL, ANY"))
            config.enter_context(config_value_context(cql, "write_consistency_levels_warned", ""))

            before_warned = get_metric(cql, WARNED_METRIC, ConsistencyLevel.ONE)
            before_disallowed = get_metric(cql, DISALLOWED_METRIC, ConsistencyLevel.ANY)

            stmt = SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ONE)
            cql.execute(stmt)
            assert get_metric(cql, WARNED_METRIC, ConsistencyLevel.ONE) == before_warned

            stmt = SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ANY)
            with pytest.raises(InvalidRequest):
                cql.execute(stmt)
            assert get_metric(cql, DISALLOWED_METRIC, ConsistencyLevel.ANY) > before_disallowed
