# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *
import random

def testManyClusterings(cql, test_keyspace):
    table = "(a TEXT"
    cols = ""
    args = "?"
    vals = ["a"]
    for i in range(40):
        table += f", c{i} text"
        cols += f", c{i}"
        if random.choice([True, False]):
            vals.append(str(i))
        else:
            vals.append("")
        args += ",?"
    args += ",?"
    vals.append("value")
    table += ", v text, PRIMARY KEY ((a)" + cols + "))"
    with create_table(cql, test_keyspace, table) as table:
        execute(cql, table, "INSERT INTO %s (a" + cols + ", v) VALUES (" + args + ")", *vals)
        flush(cql, table)
        row = execute(cql, table, "SELECT * FROM %s").one()
        for i in range(len(row)):
            assert vals[i] == getattr(row, "a" if i == 0 else f"c{i - 1}" if i < 41 else "v")
