# Copyright 2020-present ScyllaDB
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

from util import new_test_table

import requests
import nodetool

def test_create_large_static_cells_and_rows(cql, test_keyspace):
    '''Test that `large_data_handler` successfully reports large static cells
    and static rows and this doesn't cause a crash of Scylla server.

    This is a regression test for https://github.com/scylladb/scylla/issues/6780'''
    schema = "pk int, ck int, user_ids set<text> static, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, test_keyspace, schema) as table:
        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, user_ids) VALUES (?, ?, ?)")
        # Default large data threshold for cells is 1 mb, for rows it is 10 mb.
        # Take 10 mb cell to trigger large data reporting code both for
        # static cells and static rows simultaneously.
        large_set = {'x' * 1024 * 1024 * 10}
        cql.execute(insert_stmt, [1, 1, large_set])

        nodetool.flush(cql, table)
        # No need to check that the Scylla server is running here, since the test will
        # fail automatically in case Scylla crashes.
