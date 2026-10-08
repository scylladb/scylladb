# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# The tests in this file test the "CREATE TABLE ... LIKE" statement, which
# Cassandra added in Cassandra 6.0.

from ...porting import *
from cassandra.protocol import ConfigurationException, AlreadyExists
from cassandra import Unauthorized
from ....util import is_scylla
from test.pylib.skip_types import skip_env
from cassandra.util import Duration
from decimal import Decimal
from uuid import UUID, uuid4

uuid1 = UUID("62c3e96f-55cd-493b-8c8e-5a18883a1698")
uuid2 = UUID("52c3e96f-55cd-493b-8c8e-5a18883a1698")
timeUuid1 = UUID("00346642-2d2f-11ed-a261-0242ac120002")
timeUuid2 = UUID("10346642-2d2f-11ed-a261-0242ac120002")
duration1 = Duration(1, 2, 3)
duration2 = Duration(1, 2, 4)
d1 = 1.1
d2 = 2.2
f1 = to_float(3.33)
f2 = to_float(4.44)
decimal1 = Decimal("1.1")
decimal2 = Decimal("2.2")
vector1 = [1, 2]
vector2 = [3, 4]

# The original test creates its keyspaces with SimpleStrategy, but Scylla
# doesn't allow SimpleStrategy when tablets are enabled, so we use
# NetworkTopologyStrategy, with the same replication factor, instead.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

# The original Java test is parameterized by differentKs - whether the
# target table is created in the same keyspace as the source table or in a
# different keyspace. This fixture yields (sourceKs, targetKs, differentKs).
@pytest.fixture(params=[False, True], ids=["differentKs=False", "differentKs=True"])
def keyspaces(request, cql, new_to_cassandra_6):
    differentKs = request.param
    with create_keyspace(cql, REPLICATION) as keyspace1:
        with create_keyspace(cql, REPLICATION) as keyspace2:
            yield keyspace1, (keyspace2 if differentKs else keyspace1), differentKs

# Python versions of the CQLTester functions used by this test.
# createTable() creates a table in the given keyspace (replacing the "%s" in
# the query by the table's full name), and returns its name - a unique name
# unless a name is given.
def createTable(cql, keyspace, query, tableName=None):
    tableName = tableName or unique_name()
    cql.execute(query.replace("%s", keyspace + "." + tableName, 1))
    return tableName

# createTableLike() replaces the first "%s" in the query with the target
# table and the second (if any) with the source table, and returns the name
# of the target table - a unique name unless a name is given.
def createTableLike(cql, query, sourceTable, sourceKeyspace, targetKeyspace, targetTable=None):
    targetTable = targetTable or unique_name()
    query = query.replace("%s", targetKeyspace + "." + targetTable, 1)
    query = query.replace("%s", sourceKeyspace + "." + sourceTable, 1)
    cql.execute(query)
    return targetTable

def createType(cql, keyspace, query):
    typeName = unique_name()
    cql.execute(query.replace("%s", keyspace + "." + typeName, 1))
    return typeName

def indexNames(cql, keyspace, table):
    return {row.index_name for row in cql.execute("SELECT index_name FROM system_schema.indexes WHERE keyspace_name = %s AND table_name = %s", [keyspace, table])}

# createIndex() creates an index on the given table (replacing the "%s" in
# the query by the table's full name), and returns the new index's name.
def createIndex(cql, keyspace, table, query):
    before = indexNames(cql, keyspace, table)
    cql.execute(query.replace("%s", keyspace + "." + table, 1))
    added = indexNames(cql, keyspace, table) - before
    assert len(added) == 1
    return added.pop()

# The original Java test compares the two tables' internal TableMetadata
# objects, ignoring their keyspace, name and dropped columns. Here we compare
# what Cassandra exposes of the same metadata through CQL - the tables' rows
# in system_schema.columns, system_schema.tables (the table's parameters)
# and system_schema.indexes.
def getTableMetadata(cql, keyspace, table):
    params = cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s", [keyspace, table]).one()
    if params is None:
        return None
    params = params._asdict()
    id = params.pop("id")
    del params["keyspace_name"]
    del params["table_name"]
    columns = sorted((row.column_name, row.kind, row.position, row.clustering_order, row.type) for row in
        cql.execute("SELECT * FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s", [keyspace, table]))
    indexes = sorted((row.index_name, row.kind, row.options) for row in
        cql.execute("SELECT * FROM system_schema.indexes WHERE keyspace_name = %s AND table_name = %s", [keyspace, table]))
    # Column masks (Cassandra's "dynamic data masking") are part of the
    # column metadata that the original test compares. Cassandra keeps them
    # in system_schema.column_masks, which doesn't exist in Scylla.
    try:
        masks = sorted((row.column_name, row.function_keyspace, row.function_name, row.function_argument_types, row.function_argument_values) for row in
            cql.execute("SELECT * FROM system_schema.column_masks WHERE keyspace_name = %s AND table_name = %s", [keyspace, table]))
    except InvalidRequest:
        masks = []
    return {"id": id, "params": params, "columns": columns, "indexes": indexes, "masks": masks}

# Cassandra writes a table's "cdc" parameter to system_schema.tables only if
# CDC is enabled in its configuration, so we read it from DESCRIBE instead.
def getCdc(cql, keyspace, table):
    description = cql.execute(f"DESCRIBE TABLE {keyspace}.{table}").one().create_statement
    match = re.search(r"cdc = (true|false)", description)
    return match is not None and match.group(1) == "true"

def assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb, compareParams=True, compareIndexes=True, compareIndexWithOutName=False):
    left = getTableMetadata(cql, sourceKs, sourceTb)
    right = getTableMetadata(cql, targetKs, targetTb)
    assert left is not None
    assert right is not None
    assert left["columns"] == right["columns"]
    assert left["masks"] == right["masks"]
    if compareParams:
        assert left["params"] == right["params"]
    if compareIndexes:
        if compareIndexWithOutName:
            # The Java test sorts the indexes by name and then compares them
            # ignoring the name. This only works when both tables' generated
            # index names sort in the same order, so instead we compare the
            # indexes' (kind, options) as a multiset.
            def withoutName(indexes):
                return sorted((kind, sorted(options.items())) for name, kind, options in indexes)
            assert withoutName(left["indexes"]) == withoutName(right["indexes"])
        else:
            assert left["indexes"] == right["indexes"]
    assert left["id"] != right["id"]
    assert sourceTb != targetTb

# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE).
@pytest.mark.xfail(reason="SCYLLADB-5147")
def testTableSchemaCopy(cql, keyspaces):
    sourceKs, targetKs, differentKs = keyspaces
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c) VALUES (?, ?, ?)", 1, duration1, "1")
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c) VALUES (?, ?, ?)", 2, duration2, "2")
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, duration1, "1"))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, duration2, "2"))

    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int PRIMARY KEY);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (1)")
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (2)")
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2))

    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a frozen<map<text, text>> PRIMARY KEY);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (?)", {"k": "v"})
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (?)", {"nk": "nv"})
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row({"k": "v"}))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row({"nk": "nv"}))

    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b set<frozen<list<text>>>, c map<text, int>, d smallint, e duration, f tinyint);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
            1, {("1", "2"), ("3", "4")}, {"k": 1}, 2, duration1, 4)
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
            2, {("5", "6"), ("7", "8")}, {"nk": 2}, 3, duration2, 5)
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, {("1", "2"), ("3", "4")}, {"k": 1}, 2, duration1, 4))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, {("5", "6"), ("7", "8")}, {"nk": 2}, 3, duration2, 5))

    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int , b double, c tinyint, d float, e list<text>, f map<text, int>, g duration, PRIMARY KEY((a, b, c), d));")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
            1, d1, 4, f1, ["a", "b"], {"k": 1}, duration1)
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
            2, d2, 5, f2, ["c", "d"], {"nk": 2}, duration2)
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, d1, 4, f1, ["a", "b"], {"k": 1}, duration1))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, d2, 5, f2, ["c", "d"], {"nk": 2}, duration2))

    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int , " +
                                     "b text, " +
                                     "c bigint, " +
                                     "d decimal, " +
                                     "e set<text>, " +
                                     "f uuid, " +
                                     "g vector<int, 2>, " +
                                     "h list<float>, " +
                                     "i timeuuid, " +
                                     "j map<text, frozen<set<int>>>, " +
                                     "PRIMARY KEY((a, b), c, d)) " +
                                     "WITH CLUSTERING ORDER BY (c DESC, d ASC);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g, h, i, j)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            1, "b", 100, decimal1, {"1", "2"}, uuid1, vector1, [to_float(1.1), to_float(2.2)], timeUuid1, {"k": {1, 2}})
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g, h, i, j) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            2, "nb", 200, decimal2, {"3", "4"}, uuid2, vector2, [to_float(3.3), to_float(4.4)], timeUuid2, {"nk": {3, 4}})
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, "b", 100, decimal1, {"1", "2"}, uuid1, vector1, [to_float(1.1), to_float(2.2)], timeUuid1, {"k": {1, 2}}))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, "nb", 200, decimal2, {"3", "4"}, uuid2, vector2, [to_float(3.3), to_float(4.4)], timeUuid2, {"nk": {3, 4}}))

    # test that can create a copy of a copied table
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    createTableLike(cql, "CREATE TABLE %s LIKE %s", targetTb, targetKs, sourceKs, "newtargettb")

# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE).
@pytest.mark.xfail(reason="SCYLLADB-5147")
def testIfNotExists(cql, keyspaces):
    sourceKs, targetKs, differentKs = keyspaces
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));")
    targetTb = createTableLike(cql, "CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    createTableLike(cql, "CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetKs, targetTb)
    # The Python driver replaces the server's message for AlreadyExists errors
    # with its own message, so we check for the driver's message instead of
    # Cassandra's "Cannot add already existing table ...".
    assert_invalid_throw_message(cql, "", "Table '" + targetKs + "." + targetTb + "' already exists", AlreadyExists,
                              "CREATE TABLE " + targetKs + "." + targetTb + " LIKE " + sourceKs + "." + sourceTb)

# This test fails on Cassandra because of a Cassandra bug: after
# "ALTER TABLE DROP f USING TIMESTAMP 20000", where column f has data newer
# than that timestamp, reading the table through the CQL protocol fails with
# "IllegalStateException: [c, e, f] is not a subset of [c e]" (while
# serializing the read response). The original Java test reads the table
# internally, without this serialization, so it doesn't notice. This is
# CASSANDRA-21733.
# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE) and SCYLLADB-5142 (LCS's
# fanout_size option).
@pytest.mark.xfail(reason="SCYLLADB-5147, SCYLLADB-5142")
def testCopyAfterAlterTable(cql, new_to_cassandra_6, keyspaces, cassandra_bug):
    sourceKs, targetKs, differentKs = keyspaces
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " DROP d")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD e uuid")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD f float")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 1, "1", duration1, uuid1, f1)
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 2, "2", duration2, uuid2, f2)
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, "1", duration1, uuid1, f1))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, "2", duration2, uuid2, f2))

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " DROP f USING TIMESTAMP 20000")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " RENAME b TO bb ")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10, 'fanout_size' : 16} ")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)

    execute(cql, "", "INSERT INTO " + sourceKs + "." + sourceTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 1, "1", duration1, uuid1)
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 2, "2", duration2, uuid2)
    assert_rows(execute(cql, "", "SELECT * FROM " + sourceKs + "." + sourceTb),
               row(1, "1", duration1, uuid1))
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb),
               row(2, "2", duration2, uuid2))

# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE), #8948 (compression options
# "class" and "enabled"), SCYLLADB-5142 (LCS's fanout_size option), #24029
# (UnifiedCompactionStrategy) and #9859 (Cassandra's table options cdc = true,
# additional_write_policy, read_repair, memtable, incremental_backups and
# speculative_retry = '95p').
@pytest.mark.xfail(reason="SCYLLADB-5147, #8948, SCYLLADB-5142, #24029, #9859")
def testTableOptionsCopy(cql, keyspaces):
    sourceKs, targetKs, differentKs = keyspaces
    # compression
    tbCompressionDefault1 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))")
    tbCompressionDefault2 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                         " WITH compression = { 'enabled' : 'false'};")
    tbCompressionSnappy1 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };")
    tbCompressionSnappy2 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled' : true };")
    tbCompressionSnappy3 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 2 };")
    tbCompressionSnappy4 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 1 };")
    tbCompressionSnappy5 = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 0 };")

    # memtable
    # The "skiplist" and "trie" memtable configurations exist in Cassandra's
    # unit-test configuration (test/conf/cassandra.yaml), but not in the
    # default configuration we run Cassandra with, so these two tables are
    # commented out.
    #tableMemtableSkipList = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
    #                                                     " WITH memtable = 'skiplist';")
    #tableMemtableTrie = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
    #                                                 " WITH memtable = 'trie';")
    tableMemtableDefault = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH memtable = 'default';")

    # compaction
    tableCompactionStcs = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 2, 'enabled' : false};")
    tableCompactionLcs = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 1, 'fanout_size' : 5};")
    tableCompactionTwcs = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'TimeWindowCompactionStrategy', 'min_threshold' : 2};")
    tableCompactionUcs = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'UnifiedCompactionStrategy'};")

    # other options are all different from default
    tableOtherOptions = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                     " additional_write_policy = '95p' " +
                                                     " AND bloom_filter_fp_chance = 0.1 " +
                                                     " AND caching = {'keys' : 'ALL', 'rows_per_partition' : '100'}" +
                                                     " AND cdc = true " +
                                                     " AND comment = 'test for create like'" +
                                                     " AND crc_check_chance = 0.1" +
                                                     " AND default_time_to_live = 10" +
                                                     " AND compaction = {'class' : 'UnifiedCompactionStrategy'} " +
                                                     " AND compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 }" +
                                                     " AND gc_grace_seconds = 100" +
                                                     " AND incremental_backups = false" +
                                                     " AND max_index_interval = 1024" +
                                                     " AND min_index_interval = 64" +
                                                     " AND speculative_retry = '95p'" +
                                                     " AND read_repair = 'NONE'" +
                                                     " AND memtable_flush_period_in_ms = 360000" +
                                                     " AND memtable = 'default';")

    tbLikeCompressionDefault1 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionDefault1, sourceKs, targetKs)
    tbLikeCompressionDefault2 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionDefault2, sourceKs, targetKs)
    tbLikeCompressionSp1 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionSnappy1, sourceKs, targetKs)
    tbLikeCompressionSp2 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionSnappy2, sourceKs, targetKs)
    tbLikeCompressionSp3 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionSnappy3, sourceKs, targetKs)
    tbLikeCompressionSp4 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionSnappy4, sourceKs, targetKs)
    tbLikeCompressionSp5 = createTableLike(cql, "CREATE TABLE %s LIKE %s", tbCompressionSnappy5, sourceKs, targetKs)
    #tbLikeMemtableSkipList = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableMemtableSkipList, sourceKs, targetKs)
    #tbLikeMemtableTrie = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableMemtableTrie, sourceKs, targetKs)
    tbLikeMemtableDefault = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableMemtableDefault, sourceKs, targetKs)
    tbLikeCompactionStcs = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableCompactionStcs, sourceKs, targetKs)
    tbLikeCompactionLcs = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableCompactionLcs, sourceKs, targetKs)
    tbLikeCompactionTwcs = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableCompactionTwcs, sourceKs, targetKs)
    tbLikeCompactionUcs = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableCompactionUcs, sourceKs, targetKs)
    tbLikeCompactionOthers = createTableLike(cql, "CREATE TABLE %s LIKE %s", tableOtherOptions, sourceKs, targetKs)

    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionDefault1, tbLikeCompressionDefault1)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionDefault2, tbLikeCompressionDefault2)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionSnappy1, tbLikeCompressionSp1)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionSnappy2, tbLikeCompressionSp2)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionSnappy3, tbLikeCompressionSp3)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionSnappy4, tbLikeCompressionSp4)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionSnappy5, tbLikeCompressionSp5)
    #assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableMemtableSkipList, tbLikeMemtableSkipList)
    #assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableMemtableTrie, tbLikeMemtableTrie)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableMemtableDefault, tbLikeMemtableDefault)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableCompactionStcs, tbLikeCompactionStcs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableCompactionLcs, tbLikeCompactionLcs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableCompactionTwcs, tbLikeCompactionTwcs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableCompactionUcs, tbLikeCompactionUcs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableOtherOptions, tbLikeCompactionOthers)

    # a copy of the table with the table parameters set
    tableCopyAndSetCompression = createTableLike(cql, "CREATE TABLE %s LIKE %s WITH compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64 };",
                                                        tbCompressionSnappy1, sourceKs, targetKs)
    tableCopyAndSetLCSCompaction = createTableLike(cql, "CREATE TABLE %s LIKE %s WITH compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10, 'fanout_size' : 16};",
                                                          tableCompactionLcs, sourceKs, targetKs)
    tableCopyAndSetAllParams = createTableLike(cql, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                      " bloom_filter_fp_chance = 0.75 " +
                                                      " AND caching = {'keys' : 'NONE', 'rows_per_partition' : '10'}" +
                                                      " AND cdc = true " +
                                                      " AND comment = 'test for create like and set params'" +
                                                      " AND crc_check_chance = 0.8" +
                                                      " AND default_time_to_live = 100" +
                                                      " AND compaction = {'class' : 'SizeTieredCompactionStrategy'} " +
                                                      " AND compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64}" +
                                                      " AND gc_grace_seconds = 1000" +
                                                      " AND incremental_backups = true" +
                                                      " AND max_index_interval = 128" +
                                                      " AND min_index_interval = 16" +
                                                      " AND speculative_retry = '96p'" +
                                                      " AND read_repair = 'NONE'" +
                                                      " AND memtable_flush_period_in_ms = 3600;",
                                                      tableOtherOptions, sourceKs, targetKs)

    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tbCompressionDefault1, tableCopyAndSetCompression, False, False, False)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableCompactionLcs, tableCopyAndSetLCSCompaction, False, False, False)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, tableOtherOptions, tableCopyAndSetAllParams, False, False, False)
    paramsSetCompression = getTableMetadata(cql, targetKs, tableCopyAndSetCompression)["params"]
    paramsSetLCSCompaction = getTableMetadata(cql, targetKs, tableCopyAndSetLCSCompaction)["params"]
    paramsSetAllParams = getTableMetadata(cql, targetKs, tableCopyAndSetAllParams)["params"]

    # The original Java test compares these parameters to TableParams objects
    # built with the default parameters except the explicitly listed ones.
    # Here we take the default parameters from a table created without any
    # options, and override the same explicitly listed parameters, in the
    # form in which they appear in system_schema.tables.
    defaultParams = getTableMetadata(cql, sourceKs, tbCompressionDefault1)["params"]
    assert paramsSetCompression == defaultParams | {
        "compression": {"class": "org.apache.cassandra.io.compress.SnappyCompressor", "chunk_length_in_kb": "64"}}
    # (When bloom_filter_fp_chance isn't set explicitly, its default depends
    # on the compaction strategy - it is 0.1 for LeveledCompactionStrategy.)
    assert paramsSetLCSCompaction == defaultParams | {
        "bloom_filter_fp_chance": 0.1,
        "compaction": {"class": "org.apache.cassandra.db.compaction.LeveledCompactionStrategy",
                       "sstable_size_in_mb": "10", "fanout_size": "16", "max_threshold": "32", "min_threshold": "4"}}
    assert paramsSetAllParams == defaultParams | {
        "bloom_filter_fp_chance": 0.75,
        "caching": {"keys": "NONE", "rows_per_partition": "10"},
        "comment": "test for create like and set params",
        "crc_check_chance": 0.8,
        "default_time_to_live": 100,
        "compaction": {"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold": "32", "min_threshold": "4"},
        "compression": {"class": "org.apache.cassandra.io.compress.SnappyCompressor", "chunk_length_in_kb": "64"},
        "gc_grace_seconds": 1000,
        # incremental_backups = true is the default, which Cassandra doesn't
        # write to system_schema.tables.
        "max_index_interval": 128,
        "min_index_interval": 16,
        "speculative_retry": "96p",
        "read_repair": "NONE",
        "memtable_flush_period_in_ms": 3600}
    assert getCdc(cql, targetKs, tableCopyAndSetAllParams)

    # table id
    id = uuid4()
    tbNormal = createTable(cql, sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))")
    assert_invalid_throw_message(cql, "", "Cannot alter table id.", ConfigurationException,
                              "CREATE TABLE " + targetKs + ".targetnormal LIKE " + sourceKs + "." + tbNormal + " WITH ID = " + str(id))

# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE).
@pytest.mark.xfail(reason="SCYLLADB-5147")
def testStaticColumnCopy(cql, keyspaces):
    sourceKs, targetKs, differentKs = keyspaces
    # create with static column
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int , b int , c int static, d int, e list<text>, PRIMARY KEY(a, b));", "tb1")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
    execute(cql, "", "INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e) VALUES (0, 1, 2, 3, ?)", ["1", "2", "3", "4"])
    assert_rows(execute(cql, "", "SELECT * FROM " + targetKs + "." + targetTb), row(0, 1, 2, 3, ["1", "2", "3", "4"]))

    # add static column
    sourceTb = createTable(cql, sourceKs, "CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))")
    cql.execute("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD d int static")
    targetTb = createTableLike(cql, "CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs)
    assertTableMetaEqualsWithoutKs(cql, sourceKs, targetKs, sourceTb, targetTb)
