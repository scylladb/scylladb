# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Translation of AbstractFormatFctTest.java, the base class of
# FormatBytesFctTest.java and FormatTimeFctTest.java (translated in
# format_bytes_fct_test.py and format_time_fct_test.py).

from ..porting import *
from contextlib import contextmanager
from decimal import Decimal, ROUND_HALF_UP

# createTable(List<CQL3Type.Native> columnTypes, Object[][] rows)
@contextmanager
def create_format_table(cql, test_keyspace, columnTypes, rows):
    columns = [["pk", "int"]]
    for i in range(1, len(columnTypes) + 1):
        columns.append(["col" + str(i), columnTypes[i - 1]])
    with create_format_table_columns(cql, test_keyspace, columns, rows) as table:
        yield table

# createDefaultTable(Object[][] rows)
@contextmanager
def create_default_format_table(cql, test_keyspace, rows):
    with create_format_table_columns(cql, test_keyspace, [["pk", "int"], ["col1", "int"], ["col2", "int"]], rows) as table:
        yield table

# createTable(String[][] columns, Object[][] rows). Like in the Java test,
# the values in rows are inserted as CQL literals, written with str().
@contextmanager
def create_format_table_columns(cql, test_keyspace, columns, rows):
    columnsDefinition = ", ".join(c[0] + " " + c[1] + (" primary key" if i == 0 else "") for i, c in enumerate(columns))
    with create_table(cql, test_keyspace, "(" + columnsDefinition + ")") as table:
        cols = ", ".join(c[0] for c in columns)
        for row in rows:
            vals = ", ".join("null" if v is None else str(v) for v in row)
            execute(cql, table, "INSERT INTO %s (" + cols + ") values (" + vals + ")")
        yield table

# Cassandra's FormatFcts.format(double), which uses Java's
# DecimalFormat("#.##") with RoundingMode.HALF_UP. Java rounds according to
# the exact binary value of the double, as Decimal(value) does. This is only
# used for values much smaller than 10^15, where Java prints all the digits.
def format_double(value):
    s = format(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), 'f')
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s
