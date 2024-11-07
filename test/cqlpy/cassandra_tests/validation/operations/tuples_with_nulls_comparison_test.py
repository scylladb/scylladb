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

def testAddUDTField(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(foo text)") as typename:
        with create_table(cql, test_keyspace, f"(pk int, ck frozen<{typename}>, , v int, PRIMARY KEY(pk, ck))") as table:
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 0);")
            execute(cql, table, "ALTER TYPE " + typename + " ADD bar text;")
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);")
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": null}'), 2);")
            flush(cql, table)
            compact(cql, table)
            assertRows(execute(cql, table, "select v from %s where pk = 0 and ck=system.fromjson('{\"foo\": \"foo\"}')"),
                   row(2))
            assertRows(execute(cql, table, "select v from %s where pk = 0"),
                   row(2))

def testFieldWithData(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(foo text)") as typename:
        with create_table(cql, test_keyspace, f"(pk int, ck frozen<{typename}>, , v int, PRIMARY KEY(pk, ck))") as table:
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);")
            execute(cql, table, "ALTER TYPE " + typename + " ADD bar text;")
            # this row becomes inaccessible by primary key but remains visible through select *
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": \"bar\"}'), 2);")
            flush(cql, table)
            compact(cql, table)
            assertRows(execute(cql, table, "select v from %s where pk = 0"),
                   row(1),
                   row(2))

def testAddUDTFields(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(foo text)") as typename:
        with create_table(cql, test_keyspace, f"(pk int, ck frozen<{typename}>, , v int, PRIMARY KEY(pk, ck))") as table:
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 0);")
            execute(cql, table, "ALTER TYPE " + typename + " ADD bar text;")
            execute(cql, table, "ALTER TYPE " + typename + " ADD bar2 text;")
            execute(cql, table, "ALTER TYPE " + typename + " ADD bar3 text;")
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);")
            execute(cql, table, "insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": null, \"bar2\": null, \"bar3\": null}'), 2);")
            flush(cql, table)
            compact(cql, table)
            assertRows(execute(cql, table, "select v from %s where pk = 0 and ck=system.fromjson('{\"foo\": \"foo\"}')"),
                   row(2))
            assertRows(execute(cql, table, "select v from %s where pk = 0"),
                   row(2))
