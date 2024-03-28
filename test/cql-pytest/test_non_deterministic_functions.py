# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Non-deterministic CQL functions should be evaluated just before query
# execution.
# Check that these functions (e.g. `now()`, `uuid()` and `current*()`) work
# consistently both for un-prepared and prepared statements for LWT queries
# (e.g. execution order is correct and `bounce_to_shard` messages forward the
# same value for the execution on another shard).
#############################################################################

import pytest

from time import sleep
from util import new_test_table

def lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, pk_type, fn):
    with new_test_table(cql, test_keyspace, f"pk {pk_type} PRIMARY KEY") as table:
        # Test that both unprepared and prepared statements work exactly the
        # same way as described above.
        # The number of inserted rows should be exactly the same as the number
        # of query executions to show that each time non-deterministic function
        # call yields a different value (sleep 1ms to make sure time-dependent
        # functions demonstrate correct behavior).
        # Insert statements should not crash and handle `bounce_to_shard`
        # cross-shard request correctly by forwarding cached results
        # from the initial shard to the target (the test used to crash
        # before #8604 was resolved).

        insert_str = f"INSERT INTO {table} (pk) VALUES ({fn}()) IF NOT EXISTS"
        select_str = f"SELECT * FROM {table}"
        num_iterations = 10

        for i in range(num_iterations):
            cql.execute(insert_str)
            sleep(0.001)
        rows = list(cql.execute(select_str))
        assert len(rows) == num_iterations

        insert_stmt = cql.prepare(insert_str)
        for i in range(num_iterations):
            cql.execute(insert_stmt, [])
            sleep(0.001)
        rows = list(cql.execute(select_str))
        assert len(rows) == num_iterations * 2

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_lwt_uuid_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "uuid", "uuid")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_lwt_currenttimestamp_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timestamp", "currenttimestamp")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_lwt_currenttime_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "time", "currenttime")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_lwt_currenttimeuuid_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timeuuid", "currenttimeuuid")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_lwt_now_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timeuuid", "now")
