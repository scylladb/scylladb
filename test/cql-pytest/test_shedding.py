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
# Tests for the shedding mechanisms in the CQL layer
#############################################################################

import pytest
import re
from cassandra.protocol import InvalidRequest
from util import unique_name, random_string


@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int primary key, t1 text, t2 text, t3 text, t4 text, t5 text, t6 text)")
    yield table
    cql.execute("DROP TABLE " + table)

# When a too large request comes, it should be rejected in full.
# That means that first of all a client receives an error after sending
# such a request, but also that following correct requests can be successfully
# processed.
# This test depends on the current configuration. The assumptions are:
# 1. Scylla has 1GB memory total
# 2. The memory is split among 2 shards
# 3. Total memory reserved for CQL requests is 10% of the total - 50MiB
# 4. The memory estimate for a request is 2*(raw size) + 8KiB
# 5. Hence, a 30MiB request will be estimated to take around 60MiB RAM,
#    which is enough to trigger shedding.
# See also #8193.
@pytest.mark.skip(reason="highly depends on configuration")
def test_shed_too_large_request(cql, table1, scylla_only):
    prepared = cql.prepare(f"INSERT INTO {table1} (p,t1,t2,t3,t4,t5,t6) VALUES (42,?,?,?,?,?,?)")
    a_5mb_string = 'x'*5*1024*1024
    with pytest.raises(InvalidRequest, match=re.compile('large', re.IGNORECASE)):
        cql.execute(prepared, [a_5mb_string]*6)
    cql.execute(prepared, ["small_string"]*6)
    res = [row for row in cql.execute(f"SELECT p, t3 FROM {table1}")]
    assert len(res) == 1 and res[0].p == 42 and res[0].t3 == "small_string"


