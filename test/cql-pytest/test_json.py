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
# Various tests for JSON support in Scylla. Note that Cassandra also had
# extensive tests for JSON, which we ported in
# cassandra_tests/validation/entities/json_test.py. The tests here are either
# additional ones, or focusing on more esoteric issues or small tests aiming
# to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

from util import unique_name, new_test_table

from cassandra.protocol import FunctionFailure

import pytest
import random

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
    yield table
    cql.execute("DROP TABLE " + table)

# Test that failed fromJson() parsing an invalid JSON results in the expected
# error - FunctionFailure - and not some weird internal error.
# Reproduces issue #7911.
@pytest.mark.xfail(reason="issue #7911")
def test_failed_json_parsing(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('dog'))")
