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

def testTruncate(cql, test_keyspace):
    for arg in ["", "TABLE"]:
        with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY(a,b))") as table:
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)

            flush(cql, table)

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2)
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3)

            assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, 0, 2), row(1, 1, 3), row(0, 0, 0), row(0, 1, 1))

            execute(cql, table, "TRUNCATE " + arg + " %s")

            assertEmpty(execute(cql, table, "SELECT * FROM %s"))
