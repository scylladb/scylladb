# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit bb66561142788270ab450c02de836b3952ed37b4
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from .porting import *

# public class VectorInvalidQueryTest extends SAITester
# {
#     @BeforeClass
#     public static void setupClass()
#     {
#         requireNetwork();
#     }

#     @Test
#     public void cannotCreateEmptyVectorColumn()
#     {
#         assertThatThrownBy(() -> execute(String.format("CREATE TABLE %s.%s (pk int, str_val text, val vector<float, 0>, PRIMARY KEY(pk))",
#                                                        KEYSPACE, createTableName())))
#         .isInstanceOf(InvalidRequestException.class)
#         .hasMessage("vectors may only have positive dimensions; given 0");
#     }

#     @Test
#     public void cannotQueryEmptyVectorColumn()
#     {
#         createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
#         assertThatThrownBy(() -> execute("SELECT similarity_cosine((vector<float, 0>) [], []) FROM %s"))
#         .isInstanceOf(InvalidRequestException.class)
#         .hasMessage("vectors may only have positive dimensions; given 0");
#     }

#     @Test
#     public void cannotIndex1DWithCosine()
#     {
#         createTable("CREATE TABLE %s (pk int, v vector<float, 1>, PRIMARY KEY(pk))");
#         assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'cosine'}"))
#         .isInstanceOf(InvalidRequestException.class)
#         .hasRootCauseMessage(StorageAttachedIndex.VECTOR_1_DIMENSION_COSINE_ERROR);
#     }

#     @Test
#     public void cannotInsertWrongNumberOfDimensions()
#     {
#         createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0])"))
#         .isInstanceOf(InvalidRequestException.class)
#         .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
#     }

#     @Test
#     public void cannotQueryWrongNumberOfDimensions()
#     {
#         createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
#         createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

#         execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
#         execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
#         execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
#         execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

#         assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5] LIMIT 5"))
#         .isInstanceOf(InvalidRequestException.class)
#         .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
#     }

#     @Test
#     public void testMultiVectorOrderingsNotAllowed() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk int, str_val text, val1 vector<float, 3>, val2 vector<float, 3>, PRIMARY KEY(pk))");
#         createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
#         createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
#         createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");

#         assertInvalidMessage("Cannot specify more than one ANN ordering",
#                              "SELECT * FROM %s ORDER BY val1 ann of [2.5, 3.5, 4.5], val2 ann of [2.1, 3.2, 4.0] LIMIT 2");
#     }

#     @Test
#     public void testDescendingVectorOrderingIsNotAllowed() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk int, val vector<float, 3>, PRIMARY KEY(pk))");
#         createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

#         assertInvalidMessage("Descending ANN ordering is not supported",
#                              "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] DESC LIMIT 2");
#     }

#     @Test
#     public void testVectorOrderingIsNotAllowedWithClusteringOrdering() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
#         createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

#         assertInvalidMessage("ANN ordering does not support any other ordering",
#                              "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5], ck ASC LIMIT 2");
#     }

#     @Test
#     public void testVectorOrderingIsNotAllowedWithoutIndex() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
#                              "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
#                              "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5 ALLOW FILTERING");
#     }

#     @Test
#     public void testInvalidColumnNameWithAnn() throws Throwable
#     {
#         String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v int, primary key (k, c))");
#         assertInvalidMessage(String.format("Undefined column name bad_col in table %s", KEYSPACE + '.' + table),
#                              "SELECT k from %s ORDER BY bad_col ANN OF [1.0] LIMIT 1");
#     }

#     @Test
#     public void cannotPerformNonANNQueryOnVectorIndex() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
#         createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

#         assertInvalidMessage(StatementRestrictions.VECTOR_INDEXES_ANN_ONLY_MESSAGE,
#                              "SELECT * FROM %s WHERE val = [1.0, 2.0, 3.0]");
#     }

#     @Test
#     public void cannotOrderWithAnnOnNonVectorColumn() throws Throwable
#     {
#         createTable("CREATE TABLE %s (k int, v int, primary key(k))");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

#         assertInvalidMessage(StatementRestrictions.ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE,
#                              "SELECT * FROM %s ORDER BY v ANN OF 1 LIMIT 1");
#     }

#     @Test
#     public void disallowZeroVectorsWithCosineSimilarity()
#     {
#         createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
#         createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'cosine'}");

#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, [0.0, 0.0])")).isInstanceOf(InvalidRequestException.class);
#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(0.0f, 0.0f))).isInstanceOf(InvalidRequestException.class);
#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1.0f, Float.NaN))).isInstanceOf(InvalidRequestException.class);
#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1.0f, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
#         assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(Float.NEGATIVE_INFINITY, 1.0f))).isInstanceOf(InvalidRequestException.class);
#         assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of [0.0, 0.0] LIMIT 2")).isInstanceOf(InvalidRequestException.class);
#     }

#     @Test
#     public void mustHaveLimitSpecifiedAndWithinMaxAllowed()
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>)");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [0]"))
#         .isInstanceOf(InvalidQueryException.class).hasMessage(SelectStatement.TOPK_LIMIT_ERROR);

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 1001"))
#         .isInstanceOf(InvalidQueryException.class).hasMessage(String.format(StorageAttachedIndex.ANN_LIMIT_ERROR, IndexWriterConfig.MAX_TOP_K, 1001));
#     }

#     @Test
#     public void mustHaveLimitWithinPageSize()
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

#         execute("INSERT INTO %s (k, v) VALUES (1, [1.0, 2.0, 3.0])");
#         execute("INSERT INTO %s (k, v) VALUES (2, [2.0, 3.0, 4.0])");
#         execute("INSERT INTO %s (k, v) VALUES (3, [3.0, 4.0, 5.0])");

#         ClientWarn.instance.captureWarnings();
#         ResultSet result = execute("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [2.0, 3.0, 4.0] LIMIT 10", 9);
#         assertEquals(1, ClientWarn.instance.getWarnings().size());
#         assertEquals(String.format(SelectStatement.TOPK_PAGE_SIZE_WARNING, 9, 10, 10), ClientWarn.instance.getWarnings().get(0));
#         assertEquals(1, result.size());

#         ClientWarn.instance.captureWarnings();
#         result = execute("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [2.0, 3.0, 4.0] LIMIT 10", 11);
#         assertNull(ClientWarn.instance.getWarnings());
#         assertEquals(1, result.size());
#     }

#     @Test
#     public void cannotHaveAggregationOnANNQuery()
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>, c int)");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         execute("INSERT INTO %s (k, v, c) VALUES (1, [4], 1)");
#         execute("INSERT INTO %s (k, v, c) VALUES (2, [3], 10)");
#         execute("INSERT INTO %s (k, v, c) VALUES (3, [2], 100)");
#         execute("INSERT INTO %s (k, v, c) VALUES (4, [1], 1000)");

#         assertThatThrownBy(() -> executeNet("SELECT sum(c) FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 4"))
#         .isInstanceOf(InvalidQueryException.class).hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
#     }

#     @Test
#     public void multipleVectorColumnsInQueryFailCorrectlyTest() throws Throwable
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 vector<float, 1>, v2 vector<float, 1>)");
#         createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         execute("INSERT INTO %s (k, v1, v2) VALUES (1, [1], [2])");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
#                              "SELECT * FROM %s WHERE v1 = [1] ORDER BY v2 ANN OF [2] ALLOW FILTERING");
#     }

#     @Test
#     public void annOrderingIsNotAllowedWithoutIndexWhereIndexedColumnExistsInQueryTest() throws Throwable
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>, c int)");
#         createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");

#         execute("INSERT INTO %s (k, v, c) VALUES (1, [4], 1)");
#         execute("INSERT INTO %s (k, v, c) VALUES (2, [3], 10)");
#         execute("INSERT INTO %s (k, v, c) VALUES (3, [2], 100)");
#         execute("INSERT INTO %s (k, v, c) VALUES (4, [1], 1000)");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
#                              "SELECT * FROM %s WHERE c >= 100 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");
#     }

#     @Test
#     public void cannotPostFilterOnNonIndexedColumnWithAnnOrdering() throws Throwable
#     {
#         createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, v vector<float, 1>, c int, primary key ((pk1, pk2), ck1, ck2))");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (1, 1, 1, 1, [4], 1)");
#         execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (2, 2, 1, 1, [3], 10)");
#         execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (3, 3, 1, 1, [2], 100)");
#         execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (4, 4, 1, 1, [1], 1000)");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE c >= 100 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE ck1 >= 0 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE ck2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE pk1 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE pk2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE pk1 = 1 AND pk2 = 1 AND ck2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

#         assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
#                              "SELECT * FROM %s WHERE token(pk1, pk2) = token(1, 1) AND ck2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");
#     }

#     @Test
#     public void cannotHavePerPartitionLimitWithAnnOrdering()
#     {
#         createTable("CREATE TABLE %s (k int, c int, v vector<float, 1>, PRIMARY KEY(k, c))");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         execute("INSERT INTO %s (k, c, v) VALUES (1, 1, [1])");
#         execute("INSERT INTO %s (k, c, v) VALUES (1, 2, [2])");
#         execute("INSERT INTO %s (k, c, v) VALUES (1, 3, [3])");

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s ORDER BY v ANN OF [2] PER PARTITION LIMIT 1 LIMIT 3"))
#             .isInstanceOf(InvalidQueryException.class).hasMessage(SelectStatement.TOPK_PARTITION_LIMIT_ERROR);
#     }

#     @Test
#     public void cannotCreateIndexOnNonFloatVector()
#     {
#         createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 1>)");

#         assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'"))
#             .isInstanceOf(InvalidRequestException.class).hasRootCauseMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
#     }

#     @Test
#     public void canOrderWithWhereOnPrimaryColumns() throws Throwable
#     {
#         createTable("CREATE TABLE %s (a int, b int, c int, d int, v vector<float, 2>, PRIMARY KEY ((a,b),c,d))");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

#         execute("INSERT INTO %s (a, b, c, d, v) VALUES (1, 2, 1, 2, [6.0,1.0])");

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE v > [5.0,1.0] ORDER BY v ANN OF [2.0,1.0] LIMIT 1"))
#             .isInstanceOf(InvalidQueryException.class).hasMessage("v cannot be restricted by more than one relation in an ANN ordering");

#         ResultSet result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 1 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE a = 1 AND b = 2 AND d = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1"))
#             .isInstanceOf(InvalidQueryException.class).hasMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);

#         createIndex("CREATE CUSTOM INDEX c_idx ON %s(c) USING 'StorageAttachedIndex'");

#         assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE a = 1 AND b = 2 AND d = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1"))
#             .isInstanceOf(InvalidQueryException.class).hasMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);

#         dropIndex("DROP INDEX %s.c_idx");
#         createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");

#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 1 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND d = 2 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#         result = execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c > 0 ORDER BY v ANN OF [2.0,1.0] LIMIT 1", ConsistencyLevel.ONE);
#         assertEquals(1, result.size());
#     }

#     @Test
#     public void canOnlyExecuteWithCorrectConsistencyLevel()
#     {
#         createTable("CREATE TABLE %s (k int, c int, v vector<float, 1>, PRIMARY KEY(k, c))");
#         createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

#         execute("INSERT INTO %s (k, c, v) VALUES (1, 1, [1])");
#         execute("INSERT INTO %s (k, c, v) VALUES (1, 2, [2])");
#         execute("INSERT INTO %s (k, c, v) VALUES (1, 3, [3])");

#         ClientWarn.instance.captureWarnings();
#         ResultSet result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.ONE);
#         assertEquals(3, result.size());
#         assertNull(ClientWarn.instance.getWarnings());

#         result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_ONE);
#         assertEquals(3, result.size());
#         assertNull(ClientWarn.instance.getWarnings());

#         result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.QUORUM);
#         assertEquals(3, result.size());
#         assertEquals(1, ClientWarn.instance.getWarnings().size());
#         assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
#                      ClientWarn.instance.getWarnings().get(0));

#         ClientWarn.instance.captureWarnings();
#         result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_QUORUM);
#         assertEquals(3, result.size());
#         assertEquals(1, ClientWarn.instance.getWarnings().size());
#         assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
#                      ClientWarn.instance.getWarnings().get(0));

#         assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.SERIAL))
#             .isInstanceOf(InvalidRequestException.class)
#             .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.SERIAL));

#         assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_SERIAL))
#             .isInstanceOf(InvalidRequestException.class)
#             .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.LOCAL_SERIAL));
#     }

#     protected ResultSet execute(String query, ConsistencyLevel consistencyLevel)
#     {
#         return execute(query, consistencyLevel, -1);
#     }

#     protected ResultSet execute(String query, int pageSize)
#     {
#         return execute(query, ConsistencyLevel.ONE, pageSize);
#     }

#     protected ResultSet execute(String query, ConsistencyLevel consistencyLevel, int pageSize)
#     {
#         ClientState state = ClientState.forInternalCalls();
#         QueryState queryState = new QueryState(state);

#         CQLStatement statement = QueryProcessor.parseStatement(formatQuery(query), queryState.getClientState());
#         statement.validate(state);

#         QueryOptions options = QueryOptions.withConsistencyLevel(QueryOptions.forInternalCalls(Collections.emptyList()), consistencyLevel);
#         options = QueryOptions.withPageSize(options, pageSize);

#         return ((ResultMessage.Rows)statement.execute(queryState, options, Dispatcher.RequestTime.forImmediateExecution())).result;
#     }
# }
