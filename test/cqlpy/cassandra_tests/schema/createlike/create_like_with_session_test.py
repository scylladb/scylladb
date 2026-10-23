# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


from ...porting import *
from ....util import new_cql

# The original test creates its keyspaces with SimpleStrategy, but Scylla
# doesn't allow SimpleStrategy when tablets are enabled, so we use
# NetworkTopologyStrategy, with the same replication factor, instead.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

def tableExists(cql, keyspace, table):
    return cql.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s", [keyspace, table]).one() is not None

# Reproduces SCYLLADB-5147 (CREATE TABLE LIKE).
@pytest.mark.xfail(reason="SCYLLADB-5147")
def testCreateLikeWithSession(cql, new_to_cassandra_6):
    tb1 = "tb1"
    tb2 = "tb2"
    # create two keyspaces and tables
    with create_keyspace(cql, REPLICATION) as ks1, create_keyspace(cql, REPLICATION) as ks2:
        cql.execute("CREATE TABLE " + ks1 + "." + tb1 + " (id int PRIMARY KEY, age int);")
        cql.execute("CREATE TABLE " + ks2 + "." + tb2 + " (name text PRIMARY KEY, address text);")

        # "USE" cannot be undone, so we do it on a separate connection
        with new_cql(cql) as ncql:
            # use ks1
            ncql.execute("use " + ks1)
            ncql.execute("CREATE TABLE tb3 LIKE " + tb1)
            ncql.execute("CREATE TABLE " + ks1 + ".tb4 LIKE " + tb1)
            ncql.execute("CREATE TABLE tb5 like " + ks1 + "." + tb1)

            ncql.execute("CREATE TABLE " + ks2 + ".tb6 LIKE " + tb1)

            with pytest.raises(InvalidRequest, match=re.escape("Source Table '" + ks2 + ".tb1' doesn't exist")):
                ncql.execute("CREATE TABLE tb7 LIKE " + ks2 + "." + tb1)

        assert tableExists(cql, ks1, tb1)
        assert tableExists(cql, ks1, "tb3")
        assert tableExists(cql, ks1, "tb4")
        assert tableExists(cql, ks1, "tb5")
        assert tableExists(cql, ks2, "tb6")
        assert not tableExists(cql, ks2, "tb7")
