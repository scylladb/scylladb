# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
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

@pytest.mark.xfail(reason="Issue #9082")
def testNonExistingOnes(cql, test_keyspace):
    # The Scylla and Cassandra error messages are slightly different, but both
    # include the type's full name and the word "exist":
    assert_invalid_message_re(cql, test_keyspace, f"{test_keyspace}.type_does_not_exist.* exist", "DROP TYPE " + test_keyspace + ".type_does_not_exist")
    assert_invalid_message(cql, test_keyspace, "keyspace_does_not_exist", "DROP TYPE keyspace_does_not_exist.type_does_not_exist")
    execute(cql, test_keyspace, "DROP TYPE IF EXISTS " + test_keyspace + ".type_does_not_exist")
    # Reproduces issue #9082 - if the keyspace doesn't exist, an error was
    # reported instead of just doing nothing:
    execute(cql, test_keyspace, "DROP TYPE IF EXISTS keyspace_does_not_exist.type_does_not_exist")

def testNowToUUIDCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b uuid, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, now())")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b < now()"))) == 1

def testDateCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b timestamp, c bigint, d varint, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (0, toUnixTimestamp(now()), toTimestamp(now()), toTimestamp(now()))")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b <= toUnixTimestamp(now())"))) == 1
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, unixTimestampOf(now()), dateOf(now()), dateOf(now()))")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=1 AND b <= toUnixTimestamp(now())"))) == 1

def testReversedTypeCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b timeuuid, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, now())")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b < now()"))) == 1
