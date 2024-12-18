# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Test involving the "decimal" column type.
# There are additional tests involving decimals in specific contexts in other
# files - e.g., aggregating decimals in test_aggregate.py, casting decimals
# in test_cast_data.py, and decimals in JSON in test_json.py
#############################################################################

from decimal import Decimal
import pytest

from . import nodetool
from .util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c decimal, PRIMARY KEY (p, c)") as table:
        yield table

# Test that if we have clustering keys of wildly different scales, sorting
# them works. Reproduces issue #21716 where comparing two decimals of
# very different scales needed to expand the digits of one of them and
# would take huge amounts of CPU and also run out of memory.
# See also test_json.py::test_json_decimal_high_mantissa where printing
# a decimal runs into a similar problem.
def test_decimal_clustering_key_high_exponent(cql, table1):
    # The two numbers "low" and "high" have wildly different exponents.
    # Any algorithm that attempts to expand the digits of one to have the
    # same exponent as the other will run out of memory (#21716).
    low = Decimal('19866597869857659876855e-1000000000')
    high = Decimal('19866597869857659876855e1000000000')
    minuslow = Decimal('-19866597869857659876855e-1000000000')
    minushigh = Decimal('-19866597869857659876855e1000000000')
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, c) VALUES ({p}, ?)")
    cql.execute(stmt, [high])
    cql.execute(stmt, [low])
    cql.execute(stmt, [minushigh])
    cql.execute(stmt, [minuslow])
    assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [(minushigh,),(minuslow,),(low,),(high,)]
    # Do a memtable flush, just to make sure the memtable-flushing or sstable
    # writing code also doesn't use problematic decimal-handling algorithms.
    nodetool.flush(cql, table1)
