# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE

# Test batch statements
# migrated from cql_tests.py:TestCQL.batch_test()
def testBatch(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid text PRIMARY KEY, name text, password text)") as table:
        execute(cql, table, "BEGIN BATCH\n" +
                "INSERT INTO %s (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');\n" + 
                "UPDATE %s SET password = 'ps22dhds' WHERE userid = 'user3';\n" + 
                "INSERT INTO %s (userid, password) VALUES ('user4', 'ch@ngem3c');\n" + 
                "DELETE name FROM %s WHERE userid = 'user1';\n" + 
                "APPLY BATCH;")
        assert_rows_ignoring_order(execute(cql, table, "SELECT userid FROM %s"),
                   ["user2"],
                   ["user4"],
                   ["user3"])

# Migrated from cql_tests.py:TestCQL.batch_and_list_test()
def testBatchAndList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<int>)") as table:
        execute(cql, table, "BEGIN BATCH " +
                "UPDATE %s SET l = l + [1] WHERE k = 0; " +
                "UPDATE %s SET l = l + [2] WHERE k = 0; " +
                "UPDATE %s SET l = l + [3] WHERE k = 0; " +
                "APPLY BATCH")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"),
                   [[1, 2, 3]])

        execute(cql, table, "BEGIN BATCH " +
                "UPDATE %s SET l = [1] + l WHERE k = 1; " +
                "UPDATE %s SET l = [2] + l WHERE k = 1; " +
                "UPDATE %s SET l = [3] + l WHERE k = 1; " +
                "APPLY BATCH ")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 1"),
                   [[3, 2, 1]])

def testBatchAndMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<int, int>)") as table:
        execute(cql, table, "BEGIN BATCH " +
                "UPDATE %s SET m[1] = 2 WHERE k = 0; " +
                "UPDATE %s SET m[3] = 4 WHERE k = 0; " +
                "APPLY BATCH")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{1: 2, 3: 4}])

def testBatchAndSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>)") as table:
        execute(cql, table, "BEGIN BATCH " +
                "UPDATE %s SET s = s + {1} WHERE k = 0; " +
                "UPDATE %s SET s = s + {2} WHERE k = 0; " +
                "UPDATE %s SET s = s + {3} WHERE k = 0; " +
                "APPLY BATCH")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{1, 2, 3}])

# Migrated from cql_tests.py:TestCQL.bug_6115_test()
def testBatchDeleteInsert(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, 1)")
        execute(cql, table, "BEGIN BATCH DELETE FROM %s WHERE k=0 AND v=1; INSERT INTO %s (k, v) VALUES (0, 2); APPLY BATCH")
        assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 2])

def testBatchWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s text, i int)") as table:
        # test batch and update
        execute(cql, table, "BEGIN BATCH " +
                "INSERT INTO %s (k, s, i) VALUES (100, 'batchtext', 7); " +
                "INSERT INTO %s (k, s, i) VALUES (111, 'batchtext', 7); " +
                "UPDATE %s SET s=?, i=? WHERE k = 100; " +
                "UPDATE %s SET s=?, i=? WHERE k=111; " +
                "APPLY BATCH;", None, UNSET_VALUE, UNSET_VALUE, None)
        assert_rows_ignoring_order(execute(cql, table, "SELECT k, s, i FROM %s where k in (100,111)"),
                   [100, None, 7],
                   [111, "batchtext", None])

def testBatchUpdate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, PRIMARY KEY (partitionKey, clustering_1))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 4, 4)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 5, 5)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 6, 6)")

        execute(cql, table, "BEGIN BATCH " +
                "UPDATE %s SET value = 7 WHERE partitionKey = 0 AND clustering_1 = 1;" +
                "UPDATE %s SET value = 8 WHERE partitionKey = 0 AND (clustering_1) = (2);" +
                "UPDATE %s SET value = 10 WHERE partitionKey = 0 AND clustering_1 IN (3, 4);" +
                "UPDATE %s SET value = 20 WHERE partitionKey = 0 AND (clustering_1) IN ((5), (6));" +
                "APPLY BATCH;")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   [0, 0, 0],
                   [0, 1, 7],
                   [0, 2, 8],
                   [0, 3, 10],
                   [0, 4, 10],
                   [0, 5, 20],
                   [0, 6, 20])

def testBatchEmpty(cql, test_keyspace):
    assert_empty(execute(cql, test_keyspace, "BEGIN BATCH APPLY BATCH;"))

def testBatchMultipleTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int PRIMARY KEY, v11 int, v12 int)") as tbl1:
        with create_table(cql, test_keyspace, "(k2 int PRIMARY KEY, v21 int, v22 int)") as tbl2:
            execute(cql, test_keyspace, "BEGIN BATCH " +
                f"UPDATE {tbl1} SET v11 = 1 WHERE k1 = 0;" +
                f"UPDATE {tbl1} SET v12 = 2 WHERE k1 = 0;" +
                f"UPDATE {tbl2} SET v21 = 3 WHERE k2 = 0;" +
                f"UPDATE {tbl2} SET v22 = 4 WHERE k2 = 0;" +
                "APPLY BATCH;")

            assert_rows(execute(cql, tbl1, "SELECT * FROM %s"), [0, 1, 2])
            assert_rows(execute(cql, tbl2, "SELECT * FROM %s"), [0, 3, 4])

            flush(cql, tbl1)
            flush(cql, tbl2)

            assert_rows(execute(cql, tbl1, "SELECT * FROM %s"), [0, 1, 2])
            assert_rows(execute(cql, tbl2, "SELECT * FROM %s"), [0, 3, 4])

def testBatchMultipleTablePrepare(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int PRIMARY KEY, v1 int)") as tbl1:
        with create_table(cql, test_keyspace, "(k2 int PRIMARY KEY, v2 int)") as tbl2:
            query = f"BEGIN BATCH UPDATE {tbl1} SET v1 = 1 WHERE k1 = ?; UPDATE {tbl2} SET v2 = 2 WHERE k2 = ?; APPLY BATCH;"
            execute(cql, test_keyspace, query, 0, 1)

            assert_rows(execute(cql, tbl1, "SELECT * FROM %s"), [0, 1])
            assert_rows(execute(cql, tbl2, "SELECT * FROM %s"), [1, 2])

def testBatchWithInRestriction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a,b))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,1,1)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,2,2)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,3,3)

        for inClause in "()", "(1, 2)":
            assert_invalid_message(cql, table, "IN on the clustering key columns is not supported with conditional updates",
                                 "BEGIN BATCH " +
                                 "UPDATE %s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "UPDATE %s SET c = 200 WHERE a = 1 AND b IN " + inClause + " IF c = 1;" +
                                 "APPLY BATCH")

            assert_invalid_message(cql, table, "IN on the clustering key columns is not supported with conditional deletions",
                                 "BEGIN BATCH " +
                                 "UPDATE %s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "DELETE FROM %s WHERE a = 1 AND b IN " + inClause + " IF c = 1;" +
                                 "APPLY BATCH")

            # Cassandra throws NoHostAvailable here instead of InvalidRequest.
            # Also the message is different in Cassandra and Scylla -
            #  "Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)"
            #  "IN on the partition key is not supported with conditional updates"
            assert_invalid_throw_message(cql, table, "IN on the partition key", Exception,
                                 "BEGIN BATCH " +
                                 "UPDATE %s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "UPDATE %s SET c = 200 WHERE a IN " + inClause + " AND b = 1 IF c = 1;" +
                                 "APPLY BATCH")

            # Cassandra throws NoHostAvailable here instead of InvalidRequest:
            assert_invalid_throw_message(cql, table, "IN on the partition key", Exception,
                                 "BEGIN BATCH " +
                                 "UPDATE %s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "DELETE FROM %s WHERE a IN " + inClause + " AND b = 1 IF c = 1;" +
                                 "APPLY BATCH")
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [1,1,1],
                   [1,2,2],
                   [1,3,3])

# This test is marked "veryslow" because it has multi-second sleeps to check
# TTLs. These sleeps also make it dependent on timing - in the very unlikely
# case that the test code is delayed by a large fraction of a second, we can
# end up with the wrong TTL.
@pytest.mark.skip(reason="slow test, remove skip to try it anyway")
def testBatchTTLConditionalInteraction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, clustering1 int, clustering2 int, clustering3 int, val int, PRIMARY KEY (id, clustering1, clustering2, clustering3))") as clustering:
        execute(cql, clustering, "DELETE FROM %s WHERE id=1")

        clusteringInsert = "INSERT INTO " + clustering + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s); "
        clusteringTTLInsert = "INSERT INTO " + clustering + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) USING TTL %s; "
        clusteringConditionalInsert = "INSERT INTO " + clustering + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) IF NOT EXISTS; "
        clusteringConditionalTTLInsert = "INSERT INTO " + clustering + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s)  IF NOT EXISTS USING TTL %s; "
        clusteringUpdate = "UPDATE " + clustering + " SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringTTLUpdate = "UPDATE " + clustering + " USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringConditionalUpdate = "UPDATE " + clustering + " SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;"
        clusteringConditionalTTLUpdate = "UPDATE " + clustering + " USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;"
        clusteringDelete = "DELETE FROM " + clustering + " WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringRangeDelete = "DELETE FROM " + clustering + " WHERE id=%s AND clustering1=%s ;"
        clusteringConditionalDelete = "DELETE FROM " + clustering + " WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ; "

        execute(cql, clustering, "BEGIN BATCH " + clusteringInsert % (1, 1, 1, 1, 1) + " APPLY BATCH")
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"), [1, 1, 1, 1, 1])

        cmd2 = "BEGIN BATCH "
        cmd2 += clusteringInsert % (1, 1, 1, 2, 2)
        cmd2 += clusteringConditionalUpdate % (11, 1, 1, 1, 1, 1)
        cmd2 += "APPLY BATCH "
        execute(cql, clustering, cmd2)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 1, 11],
                [1, 1, 1, 2, 2])

        cmd3 = "BEGIN BATCH "
        cmd3 += clusteringInsert % (1, 1, 2, 3, 23)
        cmd3 += clusteringConditionalUpdate % (22, 1, 1, 1, 2, 2)
        cmd3 += clusteringDelete % (1, 1, 1, 1)
        cmd3 += "APPLY BATCH "
        execute(cql, clustering, cmd3)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22],
                [1, 1, 2, 3, 23])

        cmd4 = "BEGIN BATCH "
        cmd4 += clusteringInsert % (1, 2, 3, 4, 1234)
        cmd4 += clusteringConditionalUpdate % (234, 1, 1, 1, 2, 22)
        cmd4 += "APPLY BATCH "
        execute(cql, clustering, cmd4)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 234],
                [1, 1, 2, 3, 23],
                [1, 2, 3, 4, 1234])

        cmd5 = "BEGIN BATCH "
        cmd5 += clusteringRangeDelete % (1, 2)
        cmd5 += clusteringConditionalUpdate % (1234, 1, 1, 1, 2, 234)
        cmd5 += "APPLY BATCH "
        execute(cql, clustering, cmd5)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 1234],
                [1, 1, 2, 3, 23])

        cmd6 = "BEGIN BATCH "
        cmd6 += clusteringUpdate % (345, 1, 3, 4, 5)
        cmd6 += clusteringConditionalUpdate % (1, 1, 1, 1, 2, 1234)
        cmd6 += "APPLY BATCH "
        execute(cql, clustering, cmd6)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 1],
                [1, 1, 2, 3, 23],
                [1, 3, 4, 5, 345])

        cmd7 = "BEGIN BATCH "
        cmd7 += clusteringDelete % (1, 3, 4, 5)
        cmd7 += clusteringConditionalUpdate % (2300, 1, 1, 2, 3, 1) # SHOULD NOT MATCH
        cmd7 += "APPLY BATCH "
        execute(cql, clustering, cmd7)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 1],
                [1, 1, 2, 3, 23],
                [1, 3, 4, 5, 345])

        cmd8 = "BEGIN BATCH "
        cmd8 += clusteringConditionalDelete % (1, 3, 4, 5, 345)
        cmd8 += clusteringRangeDelete % (1, 1)
        cmd8 += clusteringInsert % (1, 2, 3, 4, 5)
        cmd8 += "APPLY BATCH "
        execute(cql, clustering, cmd8)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 5])

        cmd9 = "BEGIN BATCH "
        cmd9 += clusteringConditionalInsert % (1, 3, 4, 5, 345)
        cmd9 += clusteringDelete % (1, 2, 3, 4)
        cmd9 += "APPLY BATCH "
        execute(cql, clustering, cmd9)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 345])

        cmd10 = "BEGIN BATCH "
        cmd10 += clusteringTTLInsert % (1, 2, 3, 4, 5, 5)
        cmd10 += clusteringConditionalTTLUpdate % (10, 5, 1, 3, 4, 5, 345)
        cmd10 += "APPLY BATCH"
        execute(cql, clustering, cmd10)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 5], # 5 second TTL
                [1, 3, 4, 5, 5]  # 10 second TTL
        )

        time.sleep(6)

        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 5] # now 4 second TTL
        )

        cmd11 = "BEGIN BATCH "
        cmd11 += clusteringConditionalTTLInsert % (1, 2, 3, 4, 5, 5)
        cmd11 += clusteringInsert % (1, 4, 5, 6, 7)
        cmd11 += "APPLY BATCH"
        execute(cql, clustering, cmd11)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 5], # This one has 5 seconds left
                [1, 3, 4, 5, 5], # This one should have 4 seconds left
                [1, 4, 5, 6, 7]  # This one has no TTL
        )

        time.sleep(6)

        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, None], # We had a row here before from cmd9, but we've ttl'd out the value in cmd11
                [1, 4, 5, 6, 7]
        )

        cmd12 = "BEGIN BATCH "
        cmd12 += clusteringConditionalTTLUpdate % (5, 5, 1, 3, 4, 5, "null")
        cmd12 += clusteringTTLUpdate % (5, 8, 1, 4, 5, 6)
        cmd12 += "APPLY BATCH"
        execute(cql, clustering, cmd12)
        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 5],
                [1, 4, 5, 6, 8])

        time.sleep(6)

        assert_rows(execute(cql, clustering, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, None],
                [1, 4, 5, 6, None])


# This test is marked "veryslow" because it has multi-second sleeps to check
# TTLs. These sleeps also make it dependent on timing - in the very unlikely
# case that the test code is delayed by a large fraction of a second, we can
# end up with the wrong TTL.
@pytest.mark.skip(reason="slow test, remove skip to try it anyway")
def testBatchStaticTTLConditionalInteraction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, clustering1 int, clustering2 int, clustering3 int, sval int static, val int, PRIMARY KEY (id, clustering1, clustering2, clustering3))") as clustering_static:
        execute(cql, clustering_static, f"DELETE FROM {clustering_static} WHERE id=1")

        clusteringInsert = "INSERT INTO " + clustering_static + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s); "
        clusteringTTLInsert = "INSERT INTO " + clustering_static + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) USING TTL %s; "
        clusteringStaticInsert = "INSERT INTO " + clustering_static + "(id, clustering1, clustering2, clustering3, sval, val) VALUES(%s, %s, %s, %s, %s, %s); "
        clusteringConditionalInsert = "INSERT INTO " + clustering_static + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) IF NOT EXISTS; "
        clusteringConditionalTTLInsert = "INSERT INTO " + clustering_static + "(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s)  IF NOT EXISTS USING TTL %s; "
        clusteringUpdate = "UPDATE " + clustering_static + " SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringStaticUpdate = "UPDATE " + clustering_static + " SET sval=%s WHERE id=%s ;"
        clusteringTTLUpdate = "UPDATE " + clustering_static + " USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringStaticConditionalUpdate = "UPDATE " + clustering_static + " SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;"
        clusteringConditionalTTLUpdate = "UPDATE " + clustering_static + " USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;"
        clusteringStaticConditionalTTLUpdate = "UPDATE " + clustering_static + " USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;"
        clusteringStaticConditionalStaticUpdate = "UPDATE " + clustering_static + " SET sval=%s WHERE id=%s IF sval=%s; "
        clusteringDelete = "DELETE FROM " + clustering_static + " WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;"
        clusteringRangeDelete = "DELETE FROM " + clustering_static + " WHERE id=%s AND clustering1=%s ;"
        clusteringConditionalDelete = "DELETE FROM " + clustering_static + " WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ; "
        clusteringStaticConditionalDelete = "DELETE FROM " + clustering_static + " WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ; "

        execute(cql, clustering_static, "BEGIN BATCH " + clusteringStaticInsert % (1, 1, 1, 1, 1, 1) + " APPLY BATCH")

        assert_rows(execute(cql, clustering_static, f"SELECT * FROM %s WHERE id=1"), [1, 1, 1, 1, 1, 1])

        cmd2 = "BEGIN BATCH "
        cmd2 += clusteringInsert % (1, 1, 1, 2, 2)
        cmd2 += clusteringStaticConditionalUpdate % (11, 1, 1, 1, 1, 1)
        cmd2 += "APPLY BATCH "
        execute(cql, clustering_static, cmd2)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 1, 1, 11],
                [1, 1, 1, 2, 1, 2])

        cmd3 = "BEGIN BATCH "
        cmd3 += clusteringInsert % (1, 1, 2, 3, 23)
        cmd3 += clusteringStaticUpdate % (22, 1)
        cmd3 += clusteringDelete % (1, 1, 1, 1)
        cmd3 += "APPLY BATCH "
        execute(cql, clustering_static, cmd3)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, 2],
                [1, 1, 2, 3, 22, 23])

        cmd4 = "BEGIN BATCH "
        cmd4 += clusteringInsert % (1, 2, 3, 4, 1234)
        cmd4 += clusteringStaticConditionalTTLUpdate % (5, 234, 1, 1, 1, 2, 22)
        cmd4 += "APPLY BATCH "
        execute(cql, clustering_static, cmd4)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, 234],
                [1, 1, 2, 3, 22, 23],
                [1, 2, 3, 4, 22, 1234])

        time.sleep(6)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, None],
                [1, 1, 2, 3, 22, 23],
                [1, 2, 3, 4, 22, 1234])

        cmd5 = "BEGIN BATCH "
        cmd5 += clusteringRangeDelete % (1, 2)
        cmd5 += clusteringStaticConditionalUpdate % (1234, 1, 1, 1, 2, 22)
        cmd5 += "APPLY BATCH "
        execute(cql, clustering_static, cmd5)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, 1234],
                [1, 1, 2, 3, 22, 23])

        cmd6 = "BEGIN BATCH "
        cmd6 += clusteringUpdate % (345, 1, 3, 4, 5)
        cmd6 += clusteringStaticConditionalUpdate % (1, 1, 1, 1, 2, 22)
        cmd6 += "APPLY BATCH "
        execute(cql, clustering_static, cmd6)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, 1],
                [1, 1, 2, 3, 22, 23],
                [1, 3, 4, 5, 22, 345])

        cmd7 = "BEGIN BATCH "
        cmd7 += clusteringDelete % (1, 3, 4, 5)
        cmd7 += clusteringStaticConditionalUpdate % (2300, 1, 1, 2, 3, 1) # SHOULD NOT MATCH
        cmd7 += "APPLY BATCH "
        execute(cql, clustering_static, cmd7)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 1, 1, 2, 22, 1],
                [1, 1, 2, 3, 22, 23],
                [1, 3, 4, 5, 22, 345])

        cmd8 = "BEGIN BATCH "
        cmd8 += clusteringConditionalDelete % (1, 3, 4, 5, 345)
        cmd8 += clusteringRangeDelete % (1, 1)
        cmd8 += clusteringInsert % (1, 2, 3, 4, 5)
        cmd8 += "APPLY BATCH "
        execute(cql, clustering_static, cmd8)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 22, 5])

        cmd9 = "BEGIN BATCH "
        cmd9 += clusteringConditionalInsert % (1, 3, 4, 5, 345)
        cmd9 += clusteringDelete % (1, 2, 3, 4)
        cmd9 += "APPLY BATCH "
        execute(cql, clustering_static, cmd9)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 22, 345])

        cmd10 = "BEGIN BATCH "
        cmd10 += clusteringTTLInsert % (1, 2, 3, 4, 5, 5)
        cmd10 += clusteringConditionalTTLUpdate % (10, 5, 1, 3, 4, 5, 345)
        cmd10 += "APPLY BATCH "
        execute(cql, clustering_static, cmd10)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 22, 5], # 5 second TTL
                [1, 3, 4, 5, 22, 5]  # 10 second TTL
        )

        time.sleep(6)

        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 22, 5] # now 4 second TTL
        )

        cmd11 = "BEGIN BATCH "
        cmd11 += clusteringConditionalTTLInsert % (1, 2, 3, 4, 5, 5)
        cmd11 += clusteringInsert % (1, 4, 5, 6, 7)
        cmd11 += "APPLY BATCH "
        execute(cql, clustering_static, cmd11)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 22, 5], # This one has 5 seconds left
                [1, 3, 4, 5, 22, 5], # This one should have 4 seconds left
                [1, 4, 5, 6, 22, 7]  # This one has no TTL
        )

        time.sleep(6)

        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 22, None], # We had a row here before from cmd9, but we've ttl'd out the value in cmd11
                [1, 4, 5, 6, 22, 7])

        cmd12 = "BEGIN BATCH "
        cmd12 += clusteringConditionalTTLUpdate % (5, 5, 1, 3, 4, 5, "null")
        cmd12 += clusteringTTLUpdate % (5, 8, 1, 4, 5, 6)
        cmd12 += "APPLY BATCH "
        execute(cql, clustering_static, cmd12)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 22, 5],
                [1, 4, 5, 6, 22, 8])

        time.sleep(6)

        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 3, 4, 5, 22, None],
                [1, 4, 5, 6, 22, None])

        cmd13 = "BEGIN BATCH "
        cmd13 += clusteringStaticConditionalDelete % (1, 3, 4, 5, 22)
        cmd13 += clusteringInsert % (1, 2, 3, 4, 5)
        cmd13 += "APPLY BATCH "
        execute(cql, clustering_static, cmd13)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 22, 5],
                [1, 4, 5, 6, 22, None])

        cmd14 = "BEGIN BATCH "
        cmd14 += clusteringStaticConditionalStaticUpdate % (23, 1, 22)
        cmd14 += clusteringDelete % (1, 4, 5, 6)
        cmd14 += "APPLY BATCH "
        execute(cql, clustering_static, cmd14)
        assert_rows(execute(cql, clustering_static, "SELECT * FROM %s WHERE id=1"),
                [1, 2, 3, 4, 23, 5])
