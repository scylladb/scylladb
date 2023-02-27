import pytest
from util import new_test_table, unique_key_int
from cassandra.query import UNSET_VALUE
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table

# Although UNSET_VALUE is designed to skip part of a SET, it is not designed
# to skip an entire write which uses a an UNSET_VALUE in its WHERE clause -
# this should be treated as an error, not a silent skip.
#
# As in test_unset_where_clustering() above (the SELECT version of this test)
# we need to check the UNSET_VALUE on the clustering key because if we try
# an UNSET_VALUE on the partition key, the Python driver will refuse to
# send the request (it uses the partition key to decide which node to send
# the request).
def test_unset_insert_where(cql, table2):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c) VALUES ({p}, ?)')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# Similar to test_unset_insert_where() above, just use an LWT write ("IF
# NOT EXISTS"). Test that using an UNSET_VALUE in an LWT condtion causes
# a clear error, not silent skip and not a crash as in issue #13001.
def test_unset_insert_where_lwt(cql, table2):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c) VALUES ({p}, ?) IF NOT EXISTS')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])
