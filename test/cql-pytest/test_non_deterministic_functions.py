# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Non-deterministic CQL functions should be evaluated just before query
# execution.
# Check that these functions (e.g. `now()`, `uuid()` and `current*()`) work
# consistently both for un-prepared and prepared statements for LWT queries
# (e.g. execution order is correct and `bounce_to_shard` messages forward the
# same value for the execution on another shard).
#############################################################################

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

def test_lwt_uuid_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "uuid", "uuid")

def test_lwt_currenttimestamp_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timestamp", "currenttimestamp")

def test_lwt_currenttime_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "time", "currenttime")

def test_lwt_currenttimeuuid_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timeuuid", "currenttimeuuid")

def test_lwt_now_fn_pk_insert(cql, test_keyspace):
    lwt_nondeterm_fn_repeated_execute(cql, test_keyspace, "timeuuid", "now")
