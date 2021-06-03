# Copyright 2020-present ScyllaDB
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
import random
from cassandra.protocol import InvalidRequest, ReadTimeout, WriteTimeout
from cassandra.util import Duration

def r(regex):
    return re.compile(regex, re.IGNORECASE)

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(p bigint, c int, v int, PRIMARY KEY (p,c))")
    yield table
    cql.execute("DROP TABLE " + table)

# Performing operations with a small enough timeout is guaranteed to fail
def test_per_query_timeout_effective(scylla_only, cql, table1):
    table = table1
    key = random.randint(3, 2**60)
    with pytest.raises(ReadTimeout):
        cql.execute(f"SELECT * FROM {table} USING TIMEOUT 0ms")
    with pytest.raises(WriteTimeout):
        cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ({key},1,1) USING TIMEOUT 0ms")
    with pytest.raises(WriteTimeout):
        cql.execute(f"UPDATE {table} USING TIMEOUT 0ms SET v = 5 WHERE p = {key} AND c = 1")

# Performing operations with large enough timeout should succeed
def test_per_query_timeout_large_enough(scylla_only, cql, table1):
    table = table1
    key = random.randint(3, 2**60)
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ({key},1,1) USING TIMEOUT 60m")
    cql.execute(f"UPDATE {table} USING TIMEOUT 48h SET v = 5 WHERE p = {key} AND c = 1")
    res = list(cql.execute(f"SELECT * FROM {table} WHERE p IN (0,1,2,{key}) USING TIMEOUT 24h"))
    assert set(res) == set(cql.execute(f"SELECT * FROM {table} WHERE p IN (0,1,2,{key})"))

# Preparing a statement with timeout should work - both by explicitly setting
# the timeout and by using a marker.
def test_prepared_statements(scylla_only, cql, table1):
    table = table1
    key = random.randint(3, 2**60)
    prep = cql.prepare(f"INSERT INTO {table} (p,c,v) VALUES ({key},6,7) USING TIMEOUT ?")
    with pytest.raises(WriteTimeout):
        cql.execute(prep, (Duration(nanoseconds=0),))
    cql.execute(prep, (Duration(nanoseconds=10**15),))
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key}"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (6, 7)
    prep = cql.prepare(f"SELECT * FROM {table} USING TIMEOUT ?");
    with pytest.raises(ReadTimeout):
        cql.execute(prep, (Duration(nanoseconds=0),))
    cql.execute(prep, (Duration(nanoseconds=10**15),))
    prep = cql.prepare(f"UPDATE {table} USING TIMEOUT ? AND TIMESTAMP ? SET v = ? WHERE p = {key} and c = 1")
    with pytest.raises(WriteTimeout):
        cql.execute(prep, (Duration(nanoseconds=0), 3, 42))
    cql.execute(prep, (Duration(nanoseconds=10**15), 3, 42))
    prep_named = cql.prepare(f"UPDATE {table} USING TIMEOUT :timeout AND TIMESTAMP :ts SET v = :v WHERE p = {key} and c = 1")
    # Timeout cannot be left unbound
    with pytest.raises(InvalidRequest):
        cql.execute(prep_named, {'timestamp': 42, 'v': 3})
    cql.execute(prep_named, {'timestamp': 42, 'v': 3, 'timeout': Duration(nanoseconds=10**15)})
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key} AND c = 1"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (1, 3)

def test_batch(scylla_only, cql, table1):
    table = table1
    key = random.randint(3, 2**60)
    cql.execute(f"""BEGIN BATCH USING TIMEOUT 48h
        INSERT INTO {table} (p,c,v) VALUES ({key},7,8);
        INSERT INTO {table} (p,c,v) VALUES ({key+1},8,9);
        APPLY BATCH
    """)
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p  = {key} and c = 7"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (7, 8)
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p  = {key+1} and c = 8"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (8, 9)
    prep1 = cql.prepare(f"""BEGIN BATCH USING TIMEOUT ?
        INSERT INTO {table} (p,c,v) VALUES ({key},7,10);
        INSERT INTO {table} (p,c,v) VALUES ({key+1},8,11);
        APPLY BATCH
    """)
    prep2 = cql.prepare(f"""BEGIN BATCH USING TIMEOUT 48h
        INSERT INTO {table} (p,c,v) VALUES (?,7,2);
        INSERT INTO {table} (p,c,v) VALUES (?,?,14);
        APPLY BATCH
    """)
    prep_named = cql.prepare(f"""BEGIN BATCH USING TIMEOUT :timeout
        INSERT INTO {table} (p,c,v) VALUES (:key,7,8);
        INSERT INTO {table} (p,c,v) VALUES ({key+1},8,:nine);
        APPLY BATCH
    """)
    cql.execute(prep1, (Duration(nanoseconds=10**15),))
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key} and c = 7"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (7, 10)
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key+1} and c = 8"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (8, 11)
    cql.execute(prep2, (key, key+1, 8))
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key} and c = 7"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (7, 2)
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key+1} and c = 8"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (8, 14)
    cql.execute(prep_named, {'timeout': Duration(nanoseconds=10**15), 'key': key, 'nine': 9})
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key} and c = 7"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (7,8)
    result = list(cql.execute(f"SELECT * FROM {table} WHERE p = {key+1} and c = 8"))
    assert len(result) == 1 and (result[0].c, result[0].v) == (8, 9)
    with pytest.raises(WriteTimeout):
        cql.execute(prep1, (Duration(nanoseconds=0),))
    with pytest.raises(WriteTimeout):
        cql.execute(prep_named, {'timeout': Duration(nanoseconds=0), 'key': key, 'nine': 9})


# Mixing TIMEOUT parameter with other params from the USING clause is legal
def test_mix_per_query_timeout_with_other_params(scylla_only, cql, table1):
    table = table1
    key = random.randint(3, 2**60)
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ({key},1,1) USING TIMEOUT 60m AND TTL 1000000 AND TIMESTAMP 321")
    cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ({key},2,1) USING TIMESTAMP 42 AND TIMEOUT 30m")
    res = list(cql.execute(f"SELECT ttl(v), writetime(v) FROM {table} WHERE p = {key} and c = 1"))
    assert len(res) == 1 and res[0].ttl_v > 0 and res[0].writetime_v == 321
    res = list(cql.execute(f"SELECT ttl(v), writetime(v) FROM {table} WHERE p = {key} and c = 2"))
    assert len(res) == 1 and not res[0].ttl_v and res[0].writetime_v == 42

# Only valid timeout durations are allowed to be specified
def test_invalid_timeout(scylla_only, cql, table1):
    table = table1
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
