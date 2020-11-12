# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for secondary indexes

import time
import pytest
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure

from util import new_test_table

# A reproducer for issue #7443: Normally, when the entire table is SELECTed,
# the partitions are returned sorted by the partitions' token. When there
# is filtering, this order is not expected to change. Furthermore, when this
# filtering happens to use a secondary index, again the order is not expected
# to change.
def test_partition_order_with_si(cql, test_keyspace):
    schema = 'pk int, x int, PRIMARY KEY ((pk))'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Insert 20 partitions, all of them with x=1 so that filtering by x=1
        # will yield the same 20 partitions:
        N = 20
        stmt = cql.prepare('INSERT INTO '+table+' (pk, x) VALUES (?, ?)')
        for i in range(N):
            cql.execute(stmt, [i, 1])
        # SELECT all the rows, and verify they are returned in increasing
        # partition token order (note that the token is a *signed* number):
        tokens = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table)]
        assert len(tokens) == N
        assert sorted(tokens) == tokens
        # Now select all the partitions with filtering of x=1. Since all
        # rows have x=1, this shouldn't change the list of matching rows, and
        # also shouldn't check their order:
        tokens1 = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table+' WHERE x=1 ALLOW FILTERING')]
        assert tokens1 == tokens
        # Now add an index on x, which allows implementing the "x=1"
        # restriction differently. With the index, "ALLOW FILTERING" is
        # no longer necessary. But the order of the results should
        # still not change. Issue #7443 is about the order changing here.
        cql.execute('CREATE INDEX ON '+table+'(x)')
        # "CREATE INDEX" does not wait until the index is actually available
        # for use. Reads immediately after the CREATE INDEX may fail or return
        # partial results. So let's retry until reads resume working:
        for i in range(100):
            try:
                tokens2 = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table+' WHERE x=1')]
                if len(tokens2) == N:
                    break
            except ReadFailure:
                pass
            time.sleep(0.1)
        assert tokens2 == tokens
