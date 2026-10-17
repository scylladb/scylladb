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
from cassandra.query import SimpleStatement
from ..test_materialized_view_old import clock

# The original Java test starts its own Cassandra node and creates its own
# keyspace with replication_factor 2. We use the usual test keyspace instead,
# which on a single node is equivalent.

# Makes sure that we don't drop any live rows when paging with DISTINCT queries
#
# * We need to have more rows than fetch_size
# * The node must have a token within the first page (so that the range gets split up in StorageProxy#getRestrictedRanges)
#   - This means that the second read in the second range will read back too many rows
# * The extra rows are dropped (so that we only return fetch_size rows to client)
# * This means that the last row recorded in AbstractQueryPager#recordLast is a non-live one
# * For the next page, the first row returned will be the same non-live row as above
# * The bug in CASSANDRA-14956 caused us to drop that non-live row + the first live row in the next page
def testPaging(cql, test_keyspace, clock):
    # The Java test installs a custom NodeProximity implementation, through
    # Cassandra's internal configuration API, to avoid merging ranges back
    # together after StorageProxy#getRestrictedRanges splits them up. We
    # can't do this through CQL, but the test still checks the same results.
    with create_table(cql, test_keyspace, "(id int, id2 int, id3 int, val text, PRIMARY KEY ((id, id2), id3))") as table:
        for i in range(110):
            # removing row with idx 10 causes the last row in the first page read to be empty
            ttlClause = "USING TTL 1" if i == 10 else ""
            cql.execute(f"INSERT INTO {table} (id, id2, id3, val) VALUES ({i}, {i}, {i}, '{i}') {ttlClause}")

        # The Java test sleeps 1.5 seconds here, to let the TTL expire.
        clock.jump(2)

        stmt = SimpleStatement(f"SELECT DISTINCT token(id, id2), id, id2 FROM {table}", fetch_size=100)
        res = cql.execute(stmt)
        stmt = SimpleStatement(f"SELECT DISTINCT token(id, id2), id, id2 FROM {table}", fetch_size=200)
        res2 = cql.execute(stmt)

        iter1 = iter(res)
        iter2 = iter(res2)
        while True:
            row1 = next(iter1, None)
            row2 = next(iter2, None)
            if row1 is None or row2 is None:
                break
            assert row1.id == row2.id
        assert row1 is None
        assert row2 is None
