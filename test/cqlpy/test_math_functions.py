# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
#
# Tests for Cassandra 5 math functions (abs, exp, log, log10, round).
# See https://github.com/scylladb/scylladb/issues/24276 and CASSANDRA-17221.

import math
import pytest
from decimal import Decimal
from .util import new_test_table, unique_key_int


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
                        "p int PRIMARY KEY, i int, b bigint, t tinyint, s smallint,"
                        "f float, d double, v varint, e decimal") as table:
        yield table


def test_abs_int_types(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i, b, t, s) VALUES ({p}, -3, -3, -3, -3)")
    row = list(cql.execute(
        f"SELECT abs(i), abs(b), abs(t), abs(s) FROM {table1} WHERE p={p}"))[0]
    assert row == (3, 3, 3, 3)


def test_abs_float_double(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, f, d) VALUES ({p}, -3.7, -3.7)")
    row = list(cql.execute(f"SELECT abs(f), abs(d) FROM {table1} WHERE p={p}"))[0]
    assert abs(row[0] - 3.7) < 1e-6
    assert abs(row[1] - 3.7) < 1e-9


def test_abs_varint_decimal(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, v, e) VALUES ({p}, -3, -3.7)")
    row = list(cql.execute(f"SELECT abs(v), abs(e) FROM {table1} WHERE p={p}"))[0]
    assert row[0] == 3
    assert Decimal(str(row[1])) == Decimal("3.7")


def test_abs_null(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i) VALUES ({p}, null)")
    row = list(cql.execute(f"SELECT abs(i) FROM {table1} WHERE p={p}"))[0]
    assert row[0] is None


def test_abs_constant(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p) VALUES ({p})")
    assert list(cql.execute(f"SELECT abs(-5) FROM {table1} WHERE p={p}")) == [(5,)]


def test_exp_int(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i) VALUES ({p}, 1)")
    row = list(cql.execute(f"SELECT exp(i), exp(0) FROM {table1} WHERE p={p}"))[0]
    assert row[0] == int(math.e)
    assert row[1] == 1


def test_exp_float(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, f) VALUES ({p}, 1)")
    row = list(cql.execute(f"SELECT exp(f) FROM {table1} WHERE p={p}"))[0]
    assert abs(row[0] - math.e) < 1e-5


def test_log_log10(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i, d) VALUES ({p}, 1, 10.0)")
    row = list(cql.execute(
        f"SELECT log(i), log10(d), log10(i) FROM {table1} WHERE p={p}"))[0]
    assert row[0] == 0
    assert abs(row[1] - 1.0) < 1e-9
    assert row[2] == 0


def test_round_integral_identity(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i, b) VALUES ({p}, 5, -1)")
    row = list(cql.execute(f"SELECT round(i), round(b) FROM {table1} WHERE p={p}"))[0]
    assert row == (5, -1)


def test_round_float_java_semantics(cql, table1):
    # Cassandra uses Java Math.round for float/double (not BigDecimal HALF_UP).
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, f, d) VALUES ({p}, 5.5, -1.5)")
    row = list(cql.execute(f"SELECT round(f), round(d) FROM {table1} WHERE p={p}"))[0]
    assert row[0] == 6.0
    assert row[1] == -1.0


def test_round_decimal_half_up(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, e) VALUES ({p}, 5.5)")
    row = list(cql.execute(f"SELECT round(e) FROM {table1} WHERE p={p}"))[0]
    assert Decimal(str(row[0])) == Decimal("6")
    cql.execute(f"INSERT INTO {table1} (p, e) VALUES ({p}, -1.5)")
    row = list(cql.execute(f"SELECT round(e) FROM {table1} WHERE p={p}"))[0]
    assert Decimal(str(row[0])) == Decimal("-2")


def test_decimal_exp_log_approx(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, e) VALUES ({p}, 1)")
    row = list(cql.execute(f"SELECT exp(e), log(e) FROM {table1} WHERE p={p}"))[0]
    assert abs(float(row[0]) - math.e) < 1e-10
    assert abs(float(row[1])) < 1e-10
