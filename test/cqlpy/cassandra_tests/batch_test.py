# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

from cassandra.query import BatchStatement, BatchType
from cassandra.protocol import InvalidRequest

def sendBatch(cql, test_keyspace, t, addCounter, addNonCounter, addClustering):
    assert addCounter or addNonCounter or addClustering
    with create_table(cql, test_keyspace, "(id int primary key, val text)") as table_noncounter:
        with create_table(cql, test_keyspace, "(id int primary key, val counter)") as table_counter:
            with create_table(cql, test_keyspace, "(id int, clustering1 int, clustering2 int, clustering3 int, val text, primary key (id, clustering1, clustering2, clustering3))") as table_clustering:
                noncounter = cql.prepare(f"insert into {table_noncounter}(id, val)values(?,?)")
                counter = cql.prepare(f"update {table_counter} set val = val + ? where id = ?")
                clustering = cql.prepare(f"insert into {table_clustering}(id, clustering1, clustering2, clustering3, val) values(?,?,?,?,?)")
                b = BatchStatement(batch_type = t)
                for i in range(10):
                    if addNonCounter:
                        b.add(noncounter.bind([i, "foo"]))
                    if addCounter:
                        b.add(counter.bind([i, i]))
                    if addClustering:
                        b.add(clustering.bind([i, i, i, i, "foo"]))
                cql.execute(b)

def testMixedInCounterBatch(cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        sendBatch(cql, test_keyspace, BatchType.COUNTER, True, True, False)

def testMixedInLoggedBatch(cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        sendBatch(cql, test_keyspace, BatchType.LOGGED, True, True, False)

def testMixedInUnLoggedBatch(cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        sendBatch(cql, test_keyspace, BatchType.UNLOGGED, True, True, False)

def testNonCounterInCounterBatch(cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        sendBatch(cql, test_keyspace, BatchType.COUNTER, False, True, False)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testNonCounterInLoggedBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.LOGGED, False, True, False)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testNonCounterInUnLoggedBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.UNLOGGED, False, True, False)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterInCounterBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.COUNTER, True, False, False)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterInUnLoggedBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.UNLOGGED, True, False, False)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testTableWithClusteringInLoggedBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.LOGGED, False, False, True)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testTableWithClusteringInUnLoggedBatch(cql, test_keyspace):
    sendBatch(cql, test_keyspace, BatchType.UNLOGGED, False, False, True)

def testEmptyBatch(cql, test_keyspace):
    cql.execute("BEGIN BATCH APPLY BATCH")
    cql.execute("BEGIN UNLOGGED BATCH APPLY BATCH")

def testCounterInLoggedBatch(cql, test_keyspace):
    with pytest.raises(InvalidRequest):
        sendBatch(cql, test_keyspace, BatchType.LOGGED, True, False, False)

def testOversizedBatch(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int primary key, val text)") as table_noncounter:
        noncounter = cql.prepare(f"insert into {table_noncounter}(id, val)values(?,?)")
        with pytest.raises(InvalidRequest):
            # In Scylla, the default batch_size_fail_threshold_in_kb is bigger
            # so I increased the size of the string s
            SIZE_FOR_FAILURE = 2500
            s = "foobar" * 30
            b = BatchStatement(batch_type=BatchType.UNLOGGED)
            for i in range(SIZE_FOR_FAILURE):
                b.add(noncounter.bind([i, s]))
            cql.execute(b)
