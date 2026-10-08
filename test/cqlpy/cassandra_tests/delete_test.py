# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *
from contextlib import ExitStack

# The original Java test creates its own keyspace "junit" with
# replication_factor 2, and reads and writes with consistency level ONE.
# We use the usual test keyspace instead, which on a single node is
# equivalent.
def testlostDeletesTest(cql, test_keyspace):
    with ExitStack() as stack:
        tpc_base = stack.enter_context(create_table(cql, test_keyspace, "(\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");"))
        tpc_inherit_a = stack.enter_context(create_table(cql, test_keyspace, "(\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_a text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");"))
        tpc_inherit_b = stack.enter_context(create_table(cql, test_keyspace, "(\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_b text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");"))
        tpc_inherit_b2 = stack.enter_context(create_table(cql, test_keyspace, "(\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_b text ,\n" +
                "  inh_b2 text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");"))
        tpc_inherit_c = stack.enter_context(create_table(cql, test_keyspace, "(\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_c text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");"))

        pstmtI = cql.prepare(f"insert into {tpc_inherit_b} ( id, cid, inh_b, val) values (?, ?, ?, ?)")
        pstmtU = cql.prepare(f"update {tpc_inherit_b} set inh_b=?, val=? where id=? and cid=?")
        pstmtD = cql.prepare(f"delete from {tpc_inherit_b} where id=? and cid=?")
        pstmt1 = cql.prepare(f"select id, cid, val from {tpc_base} where id=? and cid=?")
        pstmt2 = cql.prepare(f"select id, cid, inh_a, val from {tpc_inherit_a} where id=? and cid=?")
        pstmt3 = cql.prepare(f"select id, cid, inh_b, val from {tpc_inherit_b} where id=? and cid=?")
        pstmt4 = cql.prepare(f"select id, cid, inh_b, inh_b2, val from {tpc_inherit_b2} where id=? and cid=?")
        pstmt5 = cql.prepare(f"select id, cid, inh_c, val from {tpc_inherit_c} where id=? and cid=?")

        def load():
            futures = [cql.execute_async(p, [1, 1]) for p in [pstmt1, pstmt2, pstmt3, pstmt4, pstmt5]]
            return [list(f.result()) for f in futures]

        # The Java test repeats the following 500 times, to catch the bug in
        # Cassandra's memtable which it was written for (CASSANDRA-7371).
        # On Scylla, 500 iterations take more than 2 seconds, so on Scylla we
        # do just 50.
        for i in range(50 if is_scylla(cql) else 500):
            cql.execute(pstmtI, [1, 1, "inhB", "valB"])

            results = load()
            assert results[0] == []
            assert results[1] == []
            assert results[2] != []
            assert results[3] == []
            assert results[4] == []

            cql.execute(pstmtU, ["inhBu", "valBu", 1, 1])

            results = load()
            assert results[0] == []
            assert results[1] == []
            assert results[2] != []
            assert results[3] == []
            assert results[4] == []

            cql.execute(pstmtD, [1, 1])

            results = load()
            assert results[0] == []
            assert results[1] == []
            assert results[2] == []
            assert results[3] == []
            assert results[4] == []
