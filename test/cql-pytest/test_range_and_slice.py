# Copyright 2021-present ScyllaDB
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

#############################################################################
# Tests the calculation of partition range and slice.

import pytest
from util import new_test_table

def test_delete_where_empty_IN(cql, test_keyspace):
    """Tests that DELETE FROM t WHERE p IN () is allowed.  See #9311."""
    with new_test_table(cql, test_keyspace, "p int, PRIMARY KEY (p)") as table:
        cql.execute(f"DELETE FROM {table} WHERE p IN ()")
