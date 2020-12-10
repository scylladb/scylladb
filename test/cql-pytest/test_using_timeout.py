# Copyright 2020 ScyllaDB
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

# Tests for USING TIMEOUT extension

from util import new_test_keyspace, unique_name
import pytest
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadTimeout, WriteTimeout

def r(regex):
    return re.compile(regex, re.IGNORECASE)

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(p int, c int, v int, PRIMARY KEY (p,c))")
    for i in range(0, 3):
        for j in range(0, 3):
            cql.execute(f'INSERT INTO {table} (p, c, v) VALUES ({i}, {j}, {j})')
    everything = list(cql.execute('SELECT * FROM ' + table))
    yield (table, everything)
    cql.execute("DROP TABLE " + table)

# Performing operations with a small enough timeout is guaranteed to fail
def test_per_query_timeout_effective(scylla_only, cql, table1):
    table, everything = table1
    with pytest.raises(ReadTimeout):
        cql.execute(f"SELECT * FROM {table} USING TIMEOUT 0ms")
    with pytest.raises(WriteTimeout):
        cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (9,1,1) USING TIMEOUT 0ms")
    with pytest.raises(WriteTimeout):
        cql.execute(f"UPDATE {table} USING TIMEOUT 0ms SET v = 5 WHERE p = 9 AND c = 1")

# Performing operations with large enough timeout should succeed
def test_per_query_timeout_large_enough(scylla_only, cql, table1):
    table, everything = table1
    res = list(cql.execute(f"SELECT * FROM {table} USING TIMEOUT 24h"))
    assert res == everything
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (9,1,1) USING TIMEOUT 60m")
    cql.execute(f"UPDATE {table} USING TIMEOUT 48h SET v = 5 WHERE p = 9 AND c = 1")
    res = list(cql.execute(f"SELECT * FROM {table} USING TIMEOUT 24h"))
    assert res == list(cql.execute(f"SELECT * FROM {table}"))

# Mixing TIMEOUT parameter with other params from the USING clause is legal
def test_mix_per_query_timeout_with_other_params(scylla_only, cql, table1):
    table, everything = table1
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (42,1,1) USING TIMEOUT 60m AND TTL 1000000 AND TIMESTAMP 321")
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (42,2,1) USING TIMESTAMP 42 AND TIMEOUT 30m")
    res = list(cql.execute(f"SELECT ttl(v), writetime(v) FROM {table} WHERE p = 42 and c = 1"))
    assert len(res) == 1 and res[0].ttl_v == 1000000 and res[0].writetime_v == 321
    res = list(cql.execute(f"SELECT ttl(v), writetime(v) FROM {table} WHERE p = 42 and c = 2"))
    assert len(res) == 1 and not res[0].ttl_v and res[0].writetime_v == 42

# Only valid timeout durations are allowed to be specified
def test_invalid_timeout(scylla_only, cql, table1):
    table, everything = table1
    def invalid(stmt):
        with pytest.raises(InvalidRequest):
            cql.execute(stmt)
    invalid(f"SELECT * FROM {table} USING TIMEOUT 'hey'")
    invalid(f"SELECT * FROM {table} USING TIMEOUT 3mo")
    invalid(f"SELECT * FROM {table} USING TIMEOUT 40y")
    invalid(f"SELECT * FROM {table} USING TIMEOUT 917")
    invalid(f"SELECT * FROM {table} USING TIMEOUT null")
    # Scylla only supports ms granularity for timeouts
    invalid(f"SELECT * FROM {table} USING TIMEOUT 60s5ns")
    invalid(f"SELECT * FROM {table} USING TIMEOUT -10ms")
    # For select statements, it's not allowed to specify timestamp or ttl,
    # since they bear no meaning
    invalid(f"SELECT * FROM {table} USING TIMEOUT 60s AND TIMESTAMP 42")
    invalid(f"SELECT * FROM {table} USING TIMEOUT 60s AND TTL 10000")
    invalid(f"SELECT * FROM {table} USING TIMEOUT 60s AND TTL 123 AND TIMESTAMP 911")
