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

def testDropColumnAsPreparedStatement(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int PRIMARY KEY, value int)") as table:
        prepared = cql.prepare(f"ALTER TABLE {table} DROP value")
        cql.execute(f"INSERT INTO {table} (key, value) VALUES (1, 1)")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, 1])
        cql.execute(prepared)
        cql.execute(f"ALTER TABLE {table} ADD value int")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, None])

def testAddList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text)") as table:
        execute(cql, table, "ALTER TABLE %s ADD myCollection list<text>")
        execute(cql, table, "INSERT INTO %s (id, content, myCollection) VALUES ('test', 'first test', ['first element'])")
        assert_rows(execute(cql, table, "SELECT * FROM %s"), ["test", "first test", ["first element"]])

def testDropList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text, myCollection list<text>)") as table:
        execute(cql, table, "INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);")
        execute(cql, table, "ALTER TABLE %s DROP myCollection")
        assert_rows(execute(cql, table, "SELECT * FROM %s;"), ["test", "first test"])

def testAddMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text)") as table:
        execute(cql, table, "ALTER TABLE %s ADD myCollection map<text, text>;")
        execute(cql, table, "INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'});")
        assert_rows(execute(cql, table, "SELECT * FROM %s;"), ["test", "first test", {"1": "first element"}])

def testDropMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text, myCollection map<text, text>)") as table:
        execute(cql, table, "INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'})")
        execute(cql, table, "ALTER TABLE %s DROP myCollection")
        assert_rows(execute(cql, table, "SELECT * FROM %s;"), ["test", "first test"])

def testDropListAndAddListWithSameName(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text, myCollection list<text>)") as table:
        execute(cql, table, "INSERT INTO %s (id, content, myCollection) VALUES ('test', 'first test', ['first element'])")
        execute(cql, table, "ALTER TABLE %s DROP myCollection")
        execute(cql, table, "ALTER TABLE %s ADD myCollection list<text>")
        assert_rows(execute(cql, table, "SELECT * FROM %s;"), ["test", "first test", None])
        execute(cql, table, "UPDATE %s set myCollection = ['second element'] WHERE id = 'test'")
        assert_rows(execute(cql, table, "SELECT * FROM %s;"), ["test", "first test", ["second element"]])

def testDropListAndAddMapWithSameName(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, content text, myCollection list<text>)") as table:
        execute(cql, table, "INSERT INTO %s (id, content, myCollection) VALUES ('test', 'first test', ['first element'])")
        execute(cql, table, "ALTER TABLE %s DROP myCollection")
        assert_invalid(cql, table, "ALTER TABLE %s ADD myCollection map<int, int>")

# Reproduces #9929
def testDropWithTimestamp(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, c1 int, v1 int, todrop int,  PRIMARY KEY (id, c1))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000 * i)

        # flush is necessary since otherwise the values of `todrop` will get discarded during
        # alter statement
        flush(cql, table)
        execute(cql, table, "ALTER TABLE %s DROP todrop USING TIMESTAMP 20000")
        execute(cql, table, "ALTER TABLE %s ADD todrop int")
        execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000)
        assert_rows(execute(cql, table, "SELECT id, c1, v1, todrop FROM %s"),
               [1, 0, 0, None],
               [1, 1, 1, None],
               [1, 2, 2, None],
               [1, 3, 3, 3],
               [1, 4, 4, 4],
               [1, 100, 100, 100])

@pytest.mark.xfail(reason="Issue #9930")
def testDropAddWithDifferentKind(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int static,  PRIMARY KEY (a, b))") as table:
        execute(cql, table, "ALTER TABLE %s DROP c")
        execute(cql, table, "ALTER TABLE %s DROP d")

        assert_invalid_message(cql, table, "Cannot re-add previously dropped column 'c' of kind STATIC, incompatible with previous kind REGULAR",
                         "ALTER TABLE %s ADD c int static")

        assert_invalid_message(cql, table, "Cannot re-add previously dropped column 'd' of kind REGULAR, incompatible with previous kind STATIC",
                         "ALTER TABLE %s ADD d int")

# Reproduces #9929
def testDropStaticWithTimestamp(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, c1 int, v1 int, todrop int static,  PRIMARY KEY (id, c1))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000 * i)

        # flush is necessary since otherwise the values of `todrop` will get discarded during
        # alter statement
        flush(cql, table)
        execute(cql, table, "ALTER TABLE %s DROP todrop USING TIMESTAMP 20000")
        execute(cql, table, "ALTER TABLE %s ADD todrop int static")
        execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000)
        # static column value with largest timestamp will be available again
        assert_rows(execute(cql, table, "SELECT id, c1, v1, todrop FROM %s"),
               [1, 0, 0, 4],
               [1, 1, 1, 4],
               [1, 2, 2, 4],
               [1, 3, 3, 4],
               [1, 4, 4, 4],
               [1, 100, 100, 4])

# Reproduces #9929
def testDropMultipleWithTimestamp(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, c1 int, v1 int, todrop1 int, todrop2 int,  PRIMARY KEY (id, c1))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, i, 10000 * i)
        # flush is necessary since otherwise the values of `todrop1` and `todrop2` will get discarded during
        # alter statement
        flush(cql, table)
        execute(cql, table, "ALTER TABLE %s DROP (todrop1, todrop2) USING TIMESTAMP 20000;")
        execute(cql, table, "ALTER TABLE %s ADD todrop1 int;")
        execute(cql, table, "ALTER TABLE %s ADD todrop2 int;")

        execute(cql, table, "INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 100, 40000)
        assert_rows(execute(cql, table, "SELECT id, c1, v1, todrop1, todrop2 FROM %s"),
               [1, 0, 0, None, None],
               [1, 1, 1, None, None],
               [1, 2, 2, None, None],
               [1, 3, 3, 3, 3],
               [1, 4, 4, 4, 4],
               [1, 100, 100, 100, 100])

def testChangeStrategyWithUnquotedAgrument(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY)") as table:
        assert_invalid_syntax(cql, table,
            "ALTER TABLE %s WITH caching = {'keys' : 'all', 'rows_per_partition' : ALL};")

# tests CASSANDRA-7976
def testAlterIndexInterval(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id uuid, album text, atrist text, data blob, PRIMARY KEY (id))") as table:
        execute(cql, table, "ALTER TABLE %s WITH min_index_interval=256 AND max_index_interval=512")
        [ks, cf] = table.split('.')
        options = cql.cluster.metadata.keyspaces[ks].tables[cf].options
        assert options['min_index_interval'] == 256
        assert options['max_index_interval'] == 512

        execute(cql, table, "ALTER TABLE %s WITH caching = {}")
        options = cql.cluster.metadata.keyspaces[ks].tables[cf].options
        assert options['min_index_interval'] == 256
        assert options['max_index_interval'] == 512

# Migrated from cql_tests.py:TestCQL.create_alter_options_test()
# Reproduces #9929
@pytest.mark.xfail(reason="Issue #9935")
def testCreateAlterKeyspaces(cql, test_keyspace, this_dc):
    assert_invalid_throw(cql, test_keyspace, SyntaxException, "CREATE KEYSPACE ks1")
    assert_invalid_throw(cql, test_keyspace, ConfigurationException, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }")

    with create_keyspace(cql, "replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as ks1:
        with create_keyspace(cql, "replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false") as ks2:
            assert_rows_ignoring_order_and_extra(execute(cql, ks1, "SELECT keyspace_name, durable_writes FROM system_schema.keyspaces"),
               [ks1, True],
               [ks2, False])

            execute(cql, ks1, "ALTER KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND durable_writes=False")
            execute(cql, ks2, "ALTER KEYSPACE %s WITH durable_writes=true")

            assert_rows_ignoring_order_and_extra(execute(cql, ks1, "SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
               [ks1, False, {"class": "org.apache.cassandra.locator.NetworkTopologyStrategy", this_dc: "1"}],
               [ks2, True, {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"}])

            assert_invalid_throw(cql, ks1, ConfigurationException, "CREATE TABLE %s.cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }")

            with create_table(cql, ks1, "(a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }") as table:
                [ks, cf] = table.split('.')
                # There's a minor difference here between Scylla and Cassandra-
                # Cassandra expands the name SizeTieredCompactionStrategy to
                # org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy,
                # and stores that in system_schema.tables. Scylla doesn't -
                # it stores the original short name given in the table creation
                # command. See issue #9935.
                assert_rows(execute(cql, ks, "SELECT table_name, compaction FROM system_schema.tables WHERE keyspace_name='%s'"),
                   [cf, {"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                              "min_threshold": "7",
                              "max_threshold": "32"}])

# NOTE: The two tests, testCreateAlterNetworkTopologyWithDefaults() and
# testCreateSimpleAlterNTSDefaults() were not translated to this test
# framework because it doesn't create more than one DC - which these
# two tests try to test.

# Test {@link ConfigurationException} thrown on alter keyspace to no DC
# option in replication configuration.
# Reproduces CASSANDRA-12681 and Scylla #10036
def testAlterKeyspaceWithNoOptionThrowsConfigurationException(cql, test_keyspace, this_dc, has_tablets):
    if has_tablets:
        extra_opts = " AND TABLETS = {'enabled': false}"
    else:
        extra_opts = ""
    # Create keyspaces
    with create_keyspace(cql, "replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 3 }" + extra_opts) as abc:
        with create_keyspace(cql, "replication={ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }" + extra_opts) as xyz:
            # Try to alter the created keyspace without any option
            assert_invalid_throw(cql, xyz, ConfigurationException, "ALTER KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy' }")
            assert_invalid_throw(cql, abc, ConfigurationException, "ALTER KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy' }")
            # Make sure that the alter works as expected
            execute(cql, abc, "ALTER KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 }")
            execute(cql, xyz, "ALTER KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }")

# Test {@link ConfigurationException} thrown when altering a keyspace to
# invalid DC option in replication configuration.
def testAlterKeyspaceWithNTSOnlyAcceptsConfiguredDataCenterNames(cql, test_keyspace, this_dc):
    # Create a keyspace with expected DC name.
    with create_keyspace(cql, "replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 }") as ks:
        # try modifying the keyspace
        assert_invalid_throw_message_re(cql, ks, "Unrecognized strategy option {INVALID_DC} passed to .*NetworkTopologyStrategy for keyspace " + ks, ConfigurationException, "ALTER KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy', 'INVALID_DC' : 2 }")
        execute(cql, ks, "ALTER KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 3 }")
        # Mix valid and invalid, should throw an exception
        assert_invalid_throw_message_re(cql, ks, "Unrecognized strategy option {INVALID_DC} passed to .*NetworkTopologyStrategy for keyspace " + ks, ConfigurationException, "ALTER KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 , 'INVALID_DC': 1}")

def testAlterKeyspaceWithMultipleInstancesOfSameDCThrowsSyntaxException(cql, test_keyspace, this_dc):
    # Create a keyspace
    with create_keyspace(cql, "replication={ 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 }") as ks:
        # try modifying the keyspace
        assert_invalid_throw(cql, ks, SyntaxException, "ALTER KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2, '" + this_dc + "' : 3 }")
        execute(cql, ks, "ALTER KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 3}")

# Test for bug of CASSANDRA-5232,
# migrated from cql_tests.py:TestCQL.alter_bug_test()
def testAlterStatementWithAdd(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int PRIMARY KEY, t text)") as table:
        execute(cql, table, "UPDATE %s SET t = '111' WHERE id = 1")
        execute(cql, table, "ALTER TABLE %s ADD l list<text>")
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
               [1, None, "111"])
        execute(cql, table, "ALTER TABLE %s ADD m map<int, text>")
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
               [1, None, None, "111"])

# Test for CASSANDRA-7744,
# migrated from cql_tests.py:TestCQL.downgrade_to_compact_bug_test()
def testDowngradeToCompact(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v set<text>)") as table:
        execute(cql, table, "insert into %s (k, v) VALUES (0, {'f'})")
        flush(cql, table)
        execute(cql, table, "alter table %s drop v")
        execute(cql, table, "alter table %s add v1 int")

# tests CASSANDRA-9565
def testDoubleWith(cql, test_keyspace):
    stmts = [ "ALTER KEYSPACE WITH WITH DURABLE_WRITES = true",
              f"ALTER KEYSPACE {test_keyspace} WITH WITH DURABLE_WRITES = true" ]
    for stmt in stmts:
        assert_invalid_throw(cql, test_keyspace, SyntaxException, stmt)

@pytest.mark.xfail(reason="Issue #8948")
def testAlterTableWithCompression(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, primary key (a, b))") as table:
        [ks, cf] = table.split('.')
        # This first assert already reproduces #8948
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "32", "class": "org.apache.cassandra.io.compress.SnappyCompressor"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'LZ4Compressor', 'chunk_length_in_kb' : 64 };")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "64", "class": "org.apache.cassandra.io.compress.LZ4Compressor"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'LZ4Compressor', 'min_compress_ratio' : 2 };")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor", "min_compress_ratio": "2.0"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'LZ4Compressor', 'min_compress_ratio' : 1 };")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor", "min_compress_ratio": "1.0"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'LZ4Compressor', 'min_compress_ratio' : 0 };")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor"}])

        execute(cql, table, "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };")
        execute(cql, table, "ALTER TABLE %s WITH compression = { 'enabled' : 'false'};")
        assert_rows(execute(cql, table, "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?;", ks, cf),
               [{"enabled": "false"}])

        assert_invalid_throw_message(cql, table, "Missing sub-option 'class' for the 'compression' option.", ConfigurationException, "ALTER TABLE %s WITH  compression = {'chunk_length_in_kb' : 32};")

        assert_invalid_throw_message(cql, table, "The 'class' option must not be empty. To disable compression use 'enabled' : false", ConfigurationException, "ALTER TABLE %s WITH  compression = { 'class' : ''};");

        assert_invalid_throw_message(cql, table, "If the 'enabled' option is set to false no other options must be specified", ConfigurationException, "ALTER TABLE %s WITH compression = { 'enabled' : 'false', 'class' : 'SnappyCompressor'};")

        assert_invalid_throw_message(cql, table, "The 'sstable_compression' option must not be used if the compression algorithm is already specified by the 'class' option", ConfigurationException, "ALTER TABLE %s WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'class' : 'SnappyCompressor'};")

        assert_invalid_throw_message(cql, table, "The 'chunk_length_kb' option must not be used if the chunk length is already specified by the 'chunk_length_in_kb' option", ConfigurationException, "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_kb' : 32 , 'chunk_length_in_kb' : 32 };")

        assert_invalid_throw_message(cql, table, "Invalid negative min_compress_ratio", ConfigurationException, "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : -1 };")

        assert_invalid_throw_message(cql, table, "min_compress_ratio can either be 0 or greater than or equal to 1", ConfigurationException, "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 0.5 };")

# Test for CASSANDRA-13337. Checks that dropping a column when a sstable contains only data for that column
# works properly.
def testAlterDropEmptySSTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, x int, y int)") as table:
        execute(cql, table, "UPDATE %s SET x = 1 WHERE k = 0")
        flush(cql, table)
        execute(cql, table, "UPDATE %s SET x = 1, y = 1 WHERE k = 0")
        flush(cql, table)
        execute(cql, table, "ALTER TABLE %s DROP x")
        compact(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1])

# Similarly to testAlterDropEmptySSTable, checks we don't return empty rows from queries (testAlterDropEmptySSTable
# tests the compaction case).
def testAlterOnlyColumnBehaviorWithFlush(cql, test_keyspace):
    for flushAfterInsert in [True, False]:
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, x int, y int)") as table:
            execute(cql, table, "UPDATE %s SET x = 1 WHERE k = 0")
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, None])
            if flushAfterInsert:
                flush(cql, table)
            execute(cql, table, "ALTER TABLE %s DROP x")
            assert_empty(execute(cql, table, "SELECT * FROM %s"))

# This test was modified from the original Cassandra test. Cassandra lists
# in the error message all the tables which need the UDT, but Scylla doesn't
# so we need a more direct test that doesn't rely on the error message.
# Reproduces CASSANDRA-15933
def testAlterTypeUsedInPartitionKey(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(v1 int)") as type1:
        with create_type(cql, test_keyspace, f"(v1 frozen<{type1}>, v2 frozen<{type1}>)") as type2:
            # frozen UDT used directly in a partition key
            with create_table(cql, test_keyspace, f"(pk frozen<{type1}>, val int, PRIMARY KEY(pk))") as table1:
                assert_invalid_message(cql, type1, table1, "ALTER TYPE %s ADD v2 int;")
            # frozen UDT used in a frozen UDT used in a partition key
            with create_table(cql, test_keyspace, f"(pk frozen<{type2}>, val int, PRIMARY KEY(pk))") as table2:
                assert_invalid_message(cql, type1, table2, "ALTER TYPE %s ADD v2 int;")
            # frozen UDT used in a frozen collection used in a partition key
            with create_table(cql, test_keyspace, f"(pk frozen<list<frozen<{type1}>>>, val int, PRIMARY KEY(pk))") as table3:
                assert_invalid_message(cql, type1, table3, "ALTER TYPE %s ADD v2 int;")
