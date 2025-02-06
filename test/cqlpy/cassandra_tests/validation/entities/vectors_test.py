# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 54e46880690bd5effb31116986292c1bdc9e891e
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from ...porting import *


def testDescendingOrderingOfVectorIsSupported(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v vector<int, 3>, PRIMARY KEY (k, v)) WITH CLUSTERING ORDER BY (v DESC)") as table:
        execute(cql, table, "INSERT INTO %s(k, v) VALUES (1, [1,2,3])")
        
        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 1 and v = [1,2,3]"), [[1, 2, 3]])
