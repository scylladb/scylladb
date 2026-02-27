# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from contextlib import ExitStack
from cassandra import ConsistencyLevel, WriteFailure
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

from .util import config_value_context, new_test_table, ScyllaMetrics

# All tests in this file are Scylla-only. In ScyllaDB, guardrails apply to all
# roles (users), whereas in Cassandra, they are not applied to the
# system user or superuser. Due to this difference, these tests will
# not pass with Cassandra, so let's mark them all scylla_only with an
# autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

@pytest.fixture(scope="module")
def test_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        yield table

@pytest.fixture(scope="module")
def counter_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c counter") as table:
        yield table

WRITES_METRIC = "scylla_cql_writes_per_consistency_level"
WARNED_METRIC = "scylla_cql_write_consistency_levels_warned_violations"
DISALLOWED_METRIC = "scylla_cql_write_consistency_levels_disallowed_violations"

def get_metric(cql, name, cl=None):
    labels = {"consistency_level": ConsistencyLevel.value_to_name[cl]} if cl is not None else None
    return ScyllaMetrics.query(cql).get(name, labels=labels) or 0

def check_warned(cql, query, cl=ConsistencyLevel.ONE, config_value=None):
    with config_value_context(cql, "write_consistency_levels_warned", config_value or ConsistencyLevel.value_to_name[cl]):
        before_writes = get_metric(cql, WRITES_METRIC, cl)
        before_warned = get_metric(cql, WARNED_METRIC, cl)

        ret = cql.execute(SimpleStatement(query, consistency_level=cl))

        after_writes = get_metric(cql, WRITES_METRIC, cl)
        after_warned = get_metric(cql, WARNED_METRIC, cl)
        assert after_writes > before_writes
        assert after_warned > before_warned
        assert len(ret.response_future.warnings) > 0

def check_disallowed(cql, query, cl=ConsistencyLevel.ONE, config_value=None):
    with config_value_context(cql, "write_consistency_levels_disallowed", config_value or ConsistencyLevel.value_to_name[cl]):
        before_writes = get_metric(cql, WRITES_METRIC, cl)
        before_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)

        with pytest.raises(InvalidRequest, match="(?i)not allowed"):
            cql.execute(SimpleStatement(query, consistency_level=cl))

        after_writes = get_metric(cql, WRITES_METRIC, cl)
        after_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)
        assert after_writes > before_writes
        assert after_disallowed > before_disallowed

def check_no_warning(cql, query, cl=ConsistencyLevel.ONE,
                     warned="", disallowed="", is_write=True):
    with ExitStack() as config:
        config.enter_context(config_value_context(cql, "write_consistency_levels_warned", warned))
        config.enter_context(config_value_context(cql, "write_consistency_levels_disallowed", disallowed))
        before_writes = get_metric(cql, WRITES_METRIC, cl)
        before_warned = get_metric(cql, WARNED_METRIC, cl)
        before_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)

        ret = cql.execute(SimpleStatement(query, consistency_level=cl))

        after_writes = get_metric(cql, WRITES_METRIC, cl)
        after_warned = get_metric(cql, WARNED_METRIC, cl)
        after_disallowed = get_metric(cql, DISALLOWED_METRIC, cl)
        if is_write:
            assert after_writes > before_writes, f"Expected {WRITES_METRIC} for {ConsistencyLevel.value_to_name[cl]} to increase, but got {before_writes} -> {after_writes}"
        assert after_warned == before_warned
        assert after_disallowed == before_disallowed
        assert ret.response_future.warnings is None

def check_guardrailed(cql, query):
    check_warned(cql, query)
    check_disallowed(cql, query)

def test_write_cl_insert(cql, test_table):
    check_guardrailed(cql, f"INSERT INTO {test_table} (pk, v) VALUES (1, 1)")

def test_write_cl_update(cql, test_table):
    check_guardrailed(cql, f"UPDATE {test_table} SET v = 2 WHERE pk = 1")

def test_write_cl_delete(cql, test_table):
    check_guardrailed(cql, f"DELETE FROM {test_table} WHERE pk = 1")

def test_write_cl_conditional_insert(cql, test_table):
    check_guardrailed(cql, f"INSERT INTO {test_table} (pk, v) VALUES (5, 5) IF NOT EXISTS")

def test_write_cl_counter(cql, counter_table):
    check_guardrailed(cql, f"UPDATE {counter_table} SET c = c + 1 WHERE pk = 1")

def test_write_cl_select_not_affected(cql, test_table):
    check_no_warning(cql, f"SELECT * FROM {test_table} WHERE pk = 1", cl=ConsistencyLevel.QUORUM, warned="QUORUM", is_write=False)

def test_write_cl_logged_batch(cql, test_table):
    check_guardrailed(cql, f"""
        BEGIN BATCH
            INSERT INTO {test_table} (pk, v) VALUES (2, 2);
            UPDATE {test_table} SET v = 3 WHERE pk = 3;
        APPLY BATCH
    """)

def test_write_cl_unlogged_batch(cql, test_table):
    check_guardrailed(cql, f"""
        BEGIN UNLOGGED BATCH
            INSERT INTO {test_table} (pk, v) VALUES (4, 4);
            UPDATE {test_table} SET v = 5 WHERE pk = 5;
        APPLY BATCH
    """)

def test_write_cl_conditional_batch(cql, test_table):
    check_guardrailed(cql, f"""
        BEGIN BATCH
            INSERT INTO {test_table} (pk, v) VALUES (4, 4) IF NOT EXISTS;
        APPLY BATCH
    """)

def test_write_cl_no_warning(cql, test_table):
    check_no_warning(cql, f"INSERT INTO {test_table} (pk, v) VALUES (4, 4)", cl=ConsistencyLevel.ALL, warned="QUORUM", disallowed="ANY")

def test_write_cl_empty_config(cql, test_table):
    check_no_warning(cql, f"INSERT INTO {test_table} (pk, v) VALUES (4, 4)", warned="", disallowed="")

def test_write_cl_invalid_level_rejected(cql):
    with ExitStack() as config:
        with pytest.raises(WriteFailure):
            config.enter_context(config_value_context(cql, "write_consistency_levels_disallowed", "INVALID_CL, ANY"))

def test_write_cl_multiple_warned_levels(cql, test_table):
    """Verify that setting multiple consistency levels in the warned
    list works correctly: each listed CL triggers a warning metric
    while unlisted CLs are unaffected."""
    query = f"INSERT INTO {test_table} (pk, v) VALUES (1, 1)"
    config = "ONE,QUORUM"
    check_warned(cql, query, cl=ConsistencyLevel.ONE, config_value=config)
    check_warned(cql, query, cl=ConsistencyLevel.QUORUM, config_value=config)
    check_no_warning(cql, query, cl=ConsistencyLevel.LOCAL_ONE, warned=config)

def test_write_cl_multiple_disallowed_levels(cql, test_table):
    """Verify that setting multiple consistency levels in the disallowed
    list works correctly: each listed CL is rejected while unlisted CLs
    are unaffected."""
    query = f"INSERT INTO {test_table} (pk, v) VALUES (1, 1)"
    config = "ANY,ALL"
    check_disallowed(cql, query, cl=ConsistencyLevel.ANY, config_value=config)
    check_disallowed(cql, query, cl=ConsistencyLevel.ALL, config_value=config)
    check_no_warning(cql, query, cl=ConsistencyLevel.LOCAL_ONE, disallowed=config)
