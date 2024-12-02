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

def testNonExistingOnes(cql, test_keyspace):
    # The specific error message that Scylla and Cassandra print in these
    # error cases are different. But they both list the non-existent table
    # or keyspace.
    assert_invalid_message(cql, test_keyspace, "table_does_not_exist",  "DROP TABLE " + test_keyspace + ".table_does_not_exist")
    assert_invalid_message(cql, test_keyspace, "keyspace_does_not_exist", "DROP TABLE keyspace_does_not_exist.table_does_not_exist")
    execute(cql, test_keyspace, "DROP TABLE IF EXISTS " + test_keyspace + ".table_does_not_exist")
    execute(cql, test_keyspace, "DROP TABLE IF EXISTS keyspace_does_not_exist.table_does_not_exist")

def testDropTableWithDroppedColumns(cql, test_keyspace):
    # CASSANDRA-13730: entry should be removed from dropped_columns table when table is dropped
    with create_table(cql, test_keyspace, "(k1 int, c1 int, v1 int, v2 int, PRIMARY KEY(k1, c1))") as table:
        execute(cql, table, "ALTER TABLE %s DROP v2")
        cf = table.split('.')[1]
    assertRowsIgnoringOrder(execute(cql, table, "select * from system_schema.dropped_columns where keyspace_name = '"
                + test_keyspace
                + "' and table_name = '" + cf + "'"))
