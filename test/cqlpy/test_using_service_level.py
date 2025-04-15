# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for USING SERVICE LEVEL extension

from .util import new_test_keyspace, unique_name, unique_key_int
import pytest
from cassandra.protocol import InvalidRequest, ReadTimeout, WriteTimeout, SyntaxException
from cassandra.cluster import NoHostAvailable
from cassandra.util import Duration

# yields list of (service level name, bool if non-zero timeout)
@pytest.fixture(scope="module")
def service_levels(cql):
    sls = [
        (unique_name(), True),
        (unique_name(), False),
        (f'"{unique_name()}_esCaPeD"', True)
    ]
    for sl, should_fail in sls:
        timeout = "0ns" if should_fail else "1h"
        cql.execute(f"CREATE SERVICE LEVEL {sl} WITH timeout = {timeout}")
    yield sls
    for sl, _ in sls:
        cql.execute(f"DROP SERVICE LEVEL {sl}")

@pytest.fixture(scope="module")
def test_table(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(p bigint, c int, v int, PRIMARY KEY (p,c))")
    yield table
    cql.execute("DROP TABLE " + table)

def test_select_using_service_level(scylla_only, cql, service_levels, test_table):
    for sl, should_fail in service_levels:
        stmt = f"SELECT * FROM {test_table} USING SERVICE LEVEL {sl}"
        if should_fail:
            with pytest.raises(ReadTimeout):
                cql.execute(stmt)
        else:
            cql.execute(stmt)

def test_select_using_non_existing_service_level(scylla_only, cql, test_table):
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT * FROM {test_table} USING SERVICE LEVEL {unique_name()}")
