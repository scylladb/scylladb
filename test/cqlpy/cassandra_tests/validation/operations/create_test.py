# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d
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
from cassandra.util import Duration
from cassandra.protocol import SyntaxException, InvalidRequest, ConfigurationException
from uuid import UUID

def testCreateTableWithSmallintColumns(cql, test_keyspace):
    short_max_value = 2**15-1
    short_min_value = -2**15
    with create_table(cql, test_keyspace, "(a text, b smallint, c smallint, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", short_max_value, short_min_value)

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   ["2", short_max_value, short_min_value],
                   ["1", 1, 2])

        # This cannot be tested in Python, it doesn't send the wrong types...
        #assertInvalidMessage(cql, table, "Expected 2 bytes for a smallint (4)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2)
        #assertInvalidMessage(cql, table, "Expected 2 bytes for a smallint (0)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, b"")

def testCreateTinyintColumns(cql, test_keyspace):
    byte_max_value = 127
    byte_min_value = -128
    with create_table(cql, test_keyspace, "(a text, b tinyint, c tinyint, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", byte_max_value, byte_min_value)

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   ["2", byte_max_value, byte_min_value],
                   ["1", 1, 2])

        # This cannot be tested in Python, it doesn't send the wrong types...
        #assertInvalidMessage(cql, table, "Expected 1 byte for a tinyint (4)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2)
        #assertInvalidMessage(cql, table, "Expected 1 byte for a tinyint (0)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", (byte) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER)

@pytest.mark.xfail(reason="Issue #8001")
def testCreateTableWithDurationColumns(cql, test_keyspace):
    t = unique_name()
    # Messages in Scylla and Cassandra are slightly different - Cassandra
    # refers to "column 'a'", Scylla to "part a".
    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY.*a",
                             f"CREATE TABLE %s.{t} (a duration PRIMARY KEY, b int);")
    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY.*b",
                             f"CREATE TABLE %s.{t} (a text, b duration, c duration, primary key (a, b));")
    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY.*b",
                             f"CREATE TABLE %s.{t} (a text, b duration, c duration, primary key (a, b)) with clustering order by (b DESC);")

    with create_table(cql, test_keyspace, "(a int, b int, c duration, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 1y2mo)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, -1y2mo)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 1Y2MO)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, 2w)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 5, 2d10h)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 6, 30h20m)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 7, 20m)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 8, 567ms)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 9, 1950us)")
        # Reproduces #8001:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 10, 1950µs)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 11, 1950000NS)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 12, -1950000ns)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 13, 1y3mo2h10m)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 14, -P1Y2M)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 15, P2D)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 16, PT20M)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 17, P2W)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 18, P1Y3MT2H10M)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 19, P0000-00-00T30:20:00)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 20, P0001-03-00T02:10:00)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 21, Duration(12, 10, 0))
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 22, Duration(-12, -10, 0))

        NANOS_PER_MICRO = 1000
        NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO
        NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI
        NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND
        NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE
        MONTHS_PER_YEAR = 12
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [1, 1, Duration(14, 0, 0)],
                   [1, 2, Duration(-14, 0, 0)],
                   [1, 3, Duration(14, 0, 0)],
                   [1, 4, Duration(0, 14, 0)],
                   [1, 5, Duration(0, 2, 10 * NANOS_PER_HOUR)],
                   [1, 6, Duration(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE)],
                   [1, 7, Duration(0, 0, 20 * NANOS_PER_MINUTE)],
                   [1, 8, Duration(0, 0, 567 * NANOS_PER_MILLI)],
                   [1, 9, Duration(0, 0, 1950 * NANOS_PER_MICRO)],
                   #Reproduces #8001:
                   [1, 10, Duration(0, 0, 1950 * NANOS_PER_MICRO)],
                   [1, 11, Duration(0, 0, 1950000)],
                   [1, 12, Duration(0, 0, -1950000)],
                   [1, 13, Duration(15, 0, 130 * NANOS_PER_MINUTE)],
                   [1, 14, Duration(-14, 0, 0)],
                   [1, 15, Duration(0, 2, 0)],
                   [1, 16, Duration(0, 0, 20 * NANOS_PER_MINUTE)],
                   [1, 17, Duration(0, 14, 0)],
                   [1, 18, Duration(15, 0, 130 * NANOS_PER_MINUTE)],
                   [1, 19, Duration(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE)],
                   [1, 20, Duration(15, 0, 130 * NANOS_PER_MINUTE)],
                   [1, 21, Duration(12, 10, 0)],
                   [1, 22, Duration(-12, -10, 0)])

        # Cassandra and Scylla print different messages: Cassandra prints
        # "Slice restrictions are not supported on duration columns", Scylla
        # prints "Duration type is unordered for c".
        assertInvalidMessageRE(cql, table, "Slice restrictions are not supported on duration columns|Duration type is unordered for c",
                             "SELECT * FROM %s WHERE c > 1y ALLOW FILTERING")

        assertInvalidMessageRE(cql, table, "Slice restrictions are not supported on duration columns|Duration type is unordered for c",
                             "SELECT * FROM %s WHERE c <= 1y ALLOW FILTERING")

        # Cannot be tested in Python
        #assertInvalidMessage(cql, table, "Expected at least 3 bytes for a duration (1)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, 1)
        #assertInvalidMessage(cql, table, "Expected at least 3 bytes for a duration (0)",
        #                     "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, b"")
        assertInvalidMessageRE(cql, table, "Invalid duration. The.*number of days must be less.*or equal to 2147483647",
                             "INSERT INTO %s (a, b, c) VALUES (1, 2, " + str(2**63-1) + "d)")

        assertInvalidMessageRE(cql, table, "The duration months, days.*and nanoseconds must be all of the same sign \\(2, -2, 0\\)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, Duration(2, -2, 0))

        assertInvalidMessageRE(cql, table, "The duration months, days.*and nanoseconds must be all of the same sign \\(-2, 0, 2000000\\)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, Duration(-2, 0, 2000000))

        assertInvalidMessageRE(cql, table, "The duration months.*must be a 32 bit.*integer",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, Duration(9223372036854775807, 1, 0))

        assertInvalidMessageRE(cql, table, "The duration days.*must be a 32 bit.*integer",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, Duration(0, 9223372036854775807, 0))

    # Test with duration column name
    with create_table(cql, test_keyspace, "(a text PRIMARY KEY, duration duration)") as table:
        pass
    # Test duration within Map
    assertInvalidMessage(cql, test_keyspace, "Durations are not allowed as map keys: map<duration, text>",
                             f"CREATE TABLE %s.{t}(pk int PRIMARY KEY, m map<duration, text>)")
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, m map<text, duration>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, m) VALUES (1, {'one month' : 1mo, '60 days' : 60d})")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [1, {"one month": Duration(1,0,0), "60 days": Duration(0,60,0)}])

    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 'm'|part m)",
                f"CREATE TABLE %s.{t}(m frozen<map<text, duration>> PRIMARY KEY, v int)")

    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 'm'|part m)",
                         f"CREATE TABLE %s.{t}(pk int, m frozen<map<text, duration>>, v int, PRIMARY KEY (pk, m))")

    # Test duration within Set
    assertInvalidMessage(cql, test_keyspace, "Durations are not allowed inside sets: set<duration>",
                         f"CREATE TABLE %s.{t}(pk int PRIMARY KEY, s set<duration>)")
    assertInvalidMessage(cql, test_keyspace, "Durations are not allowed inside sets: frozen<set<duration>>",
                         f"CREATE TABLE %s.{t}(s frozen<set<duration>> PRIMARY KEY, v int)")

    # Test duration within List
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, l list<duration>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, l) VALUES (1, [1mo, 60d])")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [1, [Duration(1,0,0), Duration(0,60,0)]])

    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 'l'|part l)",
         f"CREATE TABLE %s.{t}(l frozen<list<duration>> PRIMARY KEY, v int)")

    # Test duration within Tuple
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, t tuple<int, duration>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, t) VALUES (1, (1, 1mo))")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [1, (1, Duration(1,0,0))])

    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 't'|part t)",
         f"CREATE TABLE %s.{t}(t frozen<tuple<int, duration>> PRIMARY KEY, v int)")

    # Test duration within UDT
    with create_type(cql, test_keyspace, "(a duration)") as myType:
        with create_table(cql, test_keyspace, f"(pk int PRIMARY KEY, u {myType})") as table:
            execute(cql, table, "INSERT INTO %s (pk, u) VALUES (1, {a : 1mo})")
            assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [1, userType("a", Duration(1,0,0))])

        assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 'u'|part u)",
             f"CREATE TABLE %s.{t}(pk int, u frozen<{myType}>, v int, PRIMARY KEY(pk, u))")

    # Test duration with several level of depth
    assertInvalidMessageRE(cql, test_keyspace, "duration type is not supported for PRIMARY KEY (column 'm'|part m)",
        f"CREATE TABLE %s.{t}(pk int, m frozen<map<text, list<tuple<int, duration>>>>, v int, PRIMARY KEY (pk, m))")

# Creation and basic operations on a static table,
# migrated from cql_tests.py:TestCQL.static_cf_test()
def testStaticTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid PRIMARY KEY, firstname text, lastname text, age int)") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

        execute(cql, table, "INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32)
        execute(cql, table, "UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2)

        assertRows(execute(cql, table, "SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   ["Frodo", "Baggins"])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE userid = ?", id1),
                   [id1, 32, "Frodo", "Baggins"])

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [id2, 33, "Samwise", "Gamgee"],
                   [id1, 32, "Frodo", "Baggins"]
        )

        batch = "BEGIN BATCH " + \
                "INSERT INTO %s (userid, age) VALUES (?, ?) " + \
                "UPDATE %s SET age = ? WHERE userid = ? " + \
                "DELETE firstname, lastname FROM %s WHERE userid = ? " + \
                "DELETE firstname, lastname FROM %s WHERE userid = ? " + \
                "APPLY BATCH"

        execute(cql, table, batch, id1, 36, 37, id2, id1, id2)

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   [id2, 37, None, None],
                   [id1, 36, None, None])

# Creation and basic operations on a composite table,
# migrated from cql_tests.py:TestCQL.sparse_cf_test()
def testSparseCompositeTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid, posted_month int, posted_day int, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_day))") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

        execute(cql, table, "INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 12, 'Something else', 'Frodo Baggins')", id1)
        execute(cql, table, "INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 24, 'Something something', 'Frodo Baggins')", id1)
        execute(cql, table, "UPDATE %s SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = ? AND posted_month = 1 AND posted_day = 3", id2)
        execute(cql, table, "UPDATE %s SET body = 'Yet one more message' WHERE userid = ? AND posted_month = 1 and posted_day = 30", id1)

        assertRows(execute(cql, table, "SELECT body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day = 24", id1),
                   ["Something something", "Frodo Baggins"])

        assertRows(execute(cql, table, "SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day > 12", id1),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null))

        assertRows(execute(cql, table, "SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1", id1),
                   row(12, "Something else", "Frodo Baggins"),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null))

# Check invalid create table statements,
# migrated from cql_tests.py:TestCQL.create_invalid_test()
def testInvalidCreateTableStatements(cql, test_keyspace):
        assertInvalidThrow(cql, test_keyspace, SyntaxException, "CREATE TABLE %s.test ()")

        assertInvalid(cql, test_keyspace, "CREATE TABLE %s.test (c1 text, c2 text, c3 text)")
        assertInvalid(cql, test_keyspace, "CREATE TABLE %s.test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assertInvalid(cql, test_keyspace, "CREATE TABLE %s.test (key text PRIMARY KEY, key int)")
        assertInvalid(cql, test_keyspace, "CREATE TABLE %s.test (key text PRIMARY KEY, c int, c text)")

# Check obsolete properties from CQL2 are rejected
# migrated from cql_tests.py:TestCQL.invalid_old_property_test()
def testObsoleteTableProperties(cql, test_keyspace):
    assertInvalidThrow(cql, test_keyspace, SyntaxException, "CREATE TABLE %s.table0 (foo text PRIMARY KEY, c int) WITH default_validation=timestamp")

    with create_table(cql, test_keyspace, "(foo text PRIMARY KEY, c int)") as table:
        assertInvalidThrow(cql, table, SyntaxException, "ALTER TABLE %s WITH default_validation=int")

# Test create and drop keyspace
# migrated from cql_tests.py:TestCQL.keyspace_test()
def testKeyspace(cql):
    n = unique_name()
    assertInvalidThrow(cql, n, SyntaxException, "CREATE KEYSPACE %s testXYZ ")

    execute(cql, n, "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    execute(cql, n, "DROP KEYSPACE %s")
    assertInvalid(cql, "", 
         "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    # FIXME: Cassandra throws InvalidRequest here, but Scylla uses
    # ConfigurationException. We shouldn't have done that... But I consider
    # this difference to be so trivial I didn't want to consider this a bug.
    # Maybe we should reconsider, and not allow ConfigurationException...
    assertInvalidThrow(cql, "", (InvalidRequest, ConfigurationException), "DROP KEYSPACE non_existing")

    execute(cql, n, "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    # clean-up
    execute(cql, n, "DROP KEYSPACE %s")

#  Test {@link ConfigurationException} is thrown on create keyspace with invalid DC option in replication configuration .
def testCreateKeyspaceWithNTSOnlyAcceptsConfiguredDataCenterNames(cql, this_dc):
    n = unique_name()
    assertInvalidThrow(cql, n, ConfigurationException, "CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', 'INVALID_DC' : 2 }")
    execute(cql, n, "CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 }")
    execute(cql, n, "DROP KEYSPACE IF EXISTS %s")

    # Mix valid and invalid, should throw an exception
    assertInvalidThrow(cql, n, ConfigurationException, "CREATE KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 , 'INVALID_DC': 1}")
    execute(cql, n, "DROP KEYSPACE IF EXISTS %s")

# Test {@link ConfigurationException} is not thrown on create NetworkTopologyStrategy keyspace without any options.
@pytest.mark.xfail(reason="Issue #16028")
def testCreateKeyspaceWithNetworkTopologyStrategyNoOptions(cql):
    n = unique_name()
    execute(cql, n, "CREATE KEYSPACE %s with replication = { 'class': 'NetworkTopologyStrategy' }")
    # clean-up
    execute(cql, n, "DROP KEYSPACE IF EXISTS %s")

# Test {@link ConfigurationException} is not thrown on create SimpleStrategy keyspace without any options.
# Reproduces one aspect of #8892 (a default_keyspace_rf allows creating
# a keyspace without specifying a replication factor).
@pytest.mark.xfail(reason="Issue #8892")
def testCreateKeyspaceWithSimpleStrategyNoOptions(cql):
    n = unique_name()
    execute(cql, n, "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy' }")
    # clean-up
    execute(cql, n, "DROP KEYSPACE IF EXISTS %s")

def testCreateKeyspaceWithMultipleInstancesOfSameDCThrowsException(cql, this_dc):
    n = unique_name()
    assertInvalidThrow(cql, n, SyntaxException, "CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2, '" + this_dc + "' : 3 }")
    execute(cql, n, "DROP KEYSPACE IF EXISTS %s")

# Test create and drop table
# migrated from cql_tests.py:TestCQL.table_test()
def testTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, PRIMARY KEY (k),)") as table:
        pass
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c int)") as table1:
        execute(cql, table1, f"DROP TABLE %s")
        execute(cql, table1, f"CREATE TABLE %s ( k int PRIMARY KEY, c1 int, c2 int, ) ")

    table4 = unique_name()
    # repeated column
    # Different messages in Cassandra and Scylla
    assertInvalidMessageRE(cql, test_keyspace, "Duplicate column 'k' declaration for table|Multiple definition of identifier k", f"CREATE TABLE %s.{table4} (k int PRIMARY KEY, c int, k text)")

# Migrated from cql_tests.py:TestCQL.multiordering_validation_test()
def testTable(cql, test_keyspace):
    tableName = test_keyspace + "." + unique_name()
    assertInvalid(cql, tableName, "CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)")

    tableName = test_keyspace + "." + unique_name()
    assertInvalid(cql, tableName, "CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)")

    tableName = test_keyspace + "." + unique_name()
    assertInvalid(cql, tableName, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)")

    execute(cql, tableName, "CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)")
    execute(cql, tableName, "DROP TABLE %s")
    execute(cql, tableName, "CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)")
    execute(cql, tableName, "DROP TABLE %s")

# Tests for triggers (see issue #2205) are commented out, because using
# them in Cassandra requires adding a Java class, which we can't do.
# It is also unlikely that in the present form, they will ever be added
# to Scylla.
#    @Test
#    public void testCreateTrigger() throws Throwable
#    {
#        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))")
#        execute(cql, table, "CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#        execute(cql, table, "CREATE TRIGGER trigger_2 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_2")
#        assertInvalid(cql, table, "CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        execute(cql, table, "CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("Trigger 3")
#    }
#
#    @Test
#    public void testCreateTriggerIfNotExists() throws Throwable
#    {
#        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))")
#
#        execute(cql, table, "CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#
#        execute(cql, table, "CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#    }
#
#    @Test
#    public void testDropTrigger() throws Throwable
#    {
#        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))")
#
#        execute(cql, table, "CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#
#        execute(cql, table, "DROP TRIGGER trigger_1 ON %s")
#        assertTriggerDoesNotExists("trigger_1")
#
#        execute(cql, table, "CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#
#        assertInvalid(cql, table, "DROP TRIGGER trigger_2 ON %s")
#
#        execute(cql, table, "CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("Trigger 3")
#
#        execute(cql, table, "DROP TRIGGER \"Trigger 3\" ON %s")
#        assertTriggerDoesNotExists("Trigger 3")
#    }
#
#    @Test
#    public void testDropTriggerIfExists() throws Throwable
#    {
#        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))")
#
#        execute(cql, table, "DROP TRIGGER IF EXISTS trigger_1 ON %s")
#        assertTriggerDoesNotExists("trigger_1")
#
#        execute(cql, table, "CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'")
#        assertTriggerExists("trigger_1")
#
#        execute(cql, table, "DROP TRIGGER IF EXISTS trigger_1 ON %s")
#        assertTriggerDoesNotExists("trigger_1")
#    }
#
#    private void assertTriggerExists(String name)
#    {
#        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace(), currentTable())
#        assertTrue("the trigger does not exist", metadata.triggers.get(name).isPresent())
#    }
#
#    private void assertTriggerDoesNotExists(String name)
#    {
#        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace(), currentTable())
#        assertFalse("the trigger exists", metadata.triggers.get(name).isPresent())
#    }
#
#    public static class TestTrigger implements ITrigger
#    {
#        public TestTrigger() { }
#        public Collection<Mutation> augment(Partition update)
#        {
#            return Collections.emptyList()
#        }
#    }
#
#}

# Test commented out because it uses internal Cassandra Java APIs, not CQL
#    @Test
#    // tests CASSANDRA-4278
#    public void testHyphenDatacenters() throws Throwable
#    {
#        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch()
#
#        // Register an EndpointSnitch which returns fixed values for test.
#        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
#        {
#            @Override
#            public String getRack(InetAddressAndPort endpoint) { return RACK1; }
#
#            @Override
#            public String getDatacenter(InetAddressAndPort endpoint) { return "us-east-1"; }
#
#            @Override
#            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2) { return 0; }
#        })
#
#        // this forces the dc above to be added to the list of known datacenters (fixes static init problem
#        // with this group of tests), ok to remove at some point if doing so doesn't break the test
#        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.255"))
#        execute(cql, table, "CREATE KEYSPACE Foo WITH replication = { 'class' : 'NetworkTopologyStrategy', 'us-east-1' : 1 };")
#
#        // Restore the previous EndpointSnitch
#        DatabaseDescriptor.setEndpointSnitch(snitch)
#
#        // clean up
#        execute(cql, table, "DROP KEYSPACE IF EXISTS Foo")
#    }

# tests CASSANDRA-9565
def testDoubleWith(cql, test_keyspace):
    for stmt in ["CREATE KEYSPACE WITH WITH DURABLE_WRITES = true",
                 "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true"]:
        assertInvalidSyntaxMessage(cql, test_keyspace, "no viable alternative at input 'WITH'", stmt)

# Scylla does not support CEP-11 (Pluggable memtable implementations),
# and is unlikely to ever support it in a way compatible with Cassandra.
# Also, the tests for it use some internal Java APIs instead of CQL.
# So they are commented out.
#    public static class InvalidMemtableFactoryMethod
#    {
#        @SuppressWarnings("unused")
#        public static String factory(Map<String, String> options)
#        {
#            return "invalid"
#        }
#    }
#
#    public static class InvalidMemtableFactoryField
#    {
#        @SuppressWarnings("unused")
#        public static String FACTORY = "invalid"
#    }
#
#    @Test
#    public void testCreateTableWithMemtable() throws Throwable
#    {
#        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))")
#        assertSame(MemtableParams.DEFAULT.factory(), getCurrentColumnFamilyStore().metadata().params.memtable.factory())
#
#        assertSchemaOption("memtable", null)
#
#        testMemtableConfig("skiplist", SkipListMemtable.FACTORY, SkipListMemtable.class)
#        testMemtableConfig("skiplist_remapped", SkipListMemtable.FACTORY, SkipListMemtable.class)
#        testMemtableConfig("test_fullname", TestMemtable.FACTORY, SkipListMemtable.class)
#        testMemtableConfig("test_shortname", SkipListMemtable.FACTORY, SkipListMemtable.class)
#        testMemtableConfig("default", MemtableParams.DEFAULT.factory(), SkipListMemtable.class)
#
#        assertThrowsConfigurationException("The 'class_name' option must be specified.",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_empty_class';")
#
#        assertThrowsConfigurationException("The 'class_name' option must be specified.",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_missing_class';")
#
#        assertThrowsConfigurationException("Memtable class org.apache.cassandra.db.memtable.SkipListMemtable does not accept any further parameters, but {invalid=throw} were given.",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_invalid_param';")
#
#        assertThrowsConfigurationException("Could not create memtable factory for class NotExisting",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_unknown_class';")
#
#        assertThrowsConfigurationException("Memtable class org.apache.cassandra.db.memtable.TestMemtable does not accept any further parameters, but {invalid=throw} were given.",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_invalid_extra_param';")
#
#        assertThrowsConfigurationException("Could not create memtable factory for class " + InvalidMemtableFactoryMethod.class.getName(),
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_invalid_factory_method';")
#
#        assertThrowsConfigurationException("Could not create memtable factory for class " + InvalidMemtableFactoryField.class.getName(),
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'test_invalid_factory_field';")
#
#        assertThrowsConfigurationException("Memtable configuration \"unknown\" not found.",
#                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                                           + " WITH memtable = 'unknown';")
#    }
#
#    private void testMemtableConfig(String memtableConfig, Memtable.Factory factoryInstance, Class<? extends Memtable> memtableClass) throws Throwable
#    {
#        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
#                    + " WITH memtable = '" + memtableConfig + "';")
#        assertSame(factoryInstance, getCurrentColumnFamilyStore().metadata().params.memtable.factory())
#        Assert.assertTrue(memtableClass.isInstance(getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable()))
#
#        assertSchemaOption("memtable", MemtableParams.DEFAULT.configurationKey().equals(memtableConfig) ? null : memtableConfig)
#    }
#

def assertSchemaOption(cql, table, option, expected):
    [ks, cf] = table.split('.')
    assertRows(execute(cql, table, "SELECT " + option + " FROM system_schema.tables WHERE keyspace_name = '" + ks + "' and table_name = '" + cf + "';"), row(expected))

@pytest.mark.xfail(reason="Issue #8948, #6442")
def testCreateTableWithCompression(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b))") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "32", "class": "org.apache.cassandra.io.compress.SnappyCompressor"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled': true };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "32", "class": "org.apache.cassandra.io.compress.SnappyCompressor"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "32", "class": "org.apache.cassandra.io.compress.SnappyCompressor"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 2 };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio": "2.0"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 1 };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio": "1.0"})
#
    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 0 };") as table:
        assertSchemaOption(cql, table, "compression", {"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.SnappyCompressor"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'sstable_compression' : '', 'chunk_length_kb' : 32 };") as table:
        assertSchemaOption(cql, table, "compression", {"enabled": "false"})

    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b)) WITH compression = { 'enabled' : 'false' };") as table:
        assertSchemaOption(cql, table, "compression", {"enabled": "false"})

    table = test_keyspace + '.' + unique_name()
    assertThrowsConfigurationException(cql, table, "Missing sub-option 'class' for the 'compression' option.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = {'chunk_length_in_kb' : 32};")

    assertThrowsConfigurationException(cql, table, "The 'class' option must not be empty. To disable compression use 'enabled' : false",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : ''};")

    assertThrowsConfigurationException(cql, table, "If the 'enabled' option is set to false no other options must be specified",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'enabled' : 'false', 'class' : 'SnappyCompressor'};")

    assertThrowsConfigurationException(cql, table, "If the 'enabled' option is set to false no other options must be specified",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'enabled' : 'false', 'chunk_length_in_kb' : 32};")

    assertThrowsConfigurationException(cql, table, "The 'sstable_compression' option must not be used if the compression algorithm is already specified by the 'class' option",
                                       "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'class' : 'SnappyCompressor'};")

    assertThrowsConfigurationException(cql, table, "The 'chunk_length_kb' option must not be used if the chunk length is already specified by the 'chunk_length_in_kb' option",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_kb' : 32 , 'chunk_length_in_kb' : 32 };")

    assertThrowsConfigurationException(cql, table, "chunk_length_in_kb must be a power of 2",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 31 };")

    assertThrowsConfigurationException(cql, table, "Invalid negative or null chunk_length_in_kb",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : -1 };")

    assertThrowsConfigurationException(cql, table, "Invalid negative min_compress_ratio",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                            + " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : -1 };")

    assertThrowsConfigurationException(cql, table, "Unknown compression options unknownOption",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                            + " WITH compression = { 'class' : 'SnappyCompressor', 'unknownOption' : 32 };")

def assertThrowsConfigurationException(cql, table, message, cmd, *args):
    with pytest.raises(ConfigurationException, match=re.escape(message)):
        execute(cql, table, cmd, *args)
