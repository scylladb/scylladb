# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cassandra_tests.porting import *

# ALTER ... DROP COMPACT STORAGE was recently dropped (unless a special
# flag is used) by Cassandra, and it was never implemented in Scylla, so
# let's skip its test.
# See issue #3882
@pytest.mark.skip
def testDropCompactStorage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, PRIMARY KEY(pk)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck) VALUES (1, 1)")
        execute(cql, table, "ALTER TABLE %s DROP COMPACT STORAGE")
        assertRows(execute(cql, table,  "SELECT * FROM %s"),
                   row(1, null, 1, null))
    with create_table(cql, test_keyspace, "(pk int, ck int, PRIMARY KEY(pk, ck)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck) VALUES (1, 1)")
        execute(cql, table, "ALTER TABLE %s DROP COMPACT STORAGE")
        assertRows(execute(cql, table,  "SELECT * FROM %s"),
                   row(1, 1, null))
    with create_table(cql, test_keyspace, "(pk int, ck int, v int, PRIMARY KEY(pk, ck)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)")
        execute(cql, table, "ALTER TABLE %s DROP COMPACT STORAGE")
        assertRows(execute(cql, table,  "SELECT * FROM %s"),
                   row(1, 1, 1))

def testCompactStorageSemantics(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, PRIMARY KEY(pk, ck)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, 1)
        execute(cql, table, "DELETE FROM %s WHERE pk = ? AND ck = ?", 1, 1)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1))

    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY(pk, ck1, ck2)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck1, v) VALUES (?, ?, ?)", 2, 2, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?",2),
                   row(2, 2, null, 2))

def testColumnDeletionWithCompactTableWithMultipleColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (1, 1, 1) USING TIMESTAMP 1000")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, v1) VALUES (1, 2) USING TIMESTAMP 2000")
        flush(cql, table)
        execute(cql, table, "DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1")
        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk=1"), row(1, null, 1))
        assertRows(execute(cql, table, "SELECT v1, v2 FROM %s WHERE pk=1"), row(null, 1))
        assertRows(execute(cql, table, "SELECT v1 FROM %s WHERE pk=1"), row(null))
