# This file was translated from the original Java test from the Apache
# Cassandra source repository, commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
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

from cassandra_tests.porting import create_table, execute, assert_invalid

# Check dates are correctly recognized and validated,
# migrated from cql_tests.py:TestCQL.date_test()
def testDate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t timestamp)") as table:
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (0, '2011-02-03')");
        assert_invalid(cql, table, "INSERT INTO %s (k, t) VALUES (0, '2011-42-42')");
