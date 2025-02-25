import pytest
from cassandra.protocol import ConfigurationException
from cassandra_tests.porting import *
from cassandra.util import Duration
from cassandra.cluster import Cluster
# This test checks that keyspace can still be created with user defined replication parameters after the patch.
def testCreateKeyspaceWithUserDefinedParameters(cql):
    n = unique_name()
    execute(cql, n, "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    cluster = cql.cluster
    cluster.refresh_schema_metadata()  
    keyspaces = cluster.metadata.keyspaces  
    assert n in keyspaces  
    params = cluster.metadata.keyspaces[n]

    assert params.replication_strategy.__class__.__name__ == "SimpleStrategy"

    assert params.replication_strategy.replication_factor == 1

    execute(cql, n, "DROP KEYSPACE %s")
# This test checks if user can now use a new approach to create a keyspace without defining the replication parameters
def testCreateKeyspaceWithBasicParameters(cql):
    n = unique_name()
    execute(cql, n, "CREATE KEYSPACE %s")
    cluster = cql.cluster
    cluster.refresh_schema_metadata()
    keyspaces = cluster.metadata.keyspaces
    assert n in keyspaces
    params = cluster.metadata.keyspaces[n]

    assert params.replication_strategy.__class__.__name__ == "NetworkTopologyStrategy"

    assert params.replication_strategy.dc_replication_factors['datacenter1'] == 3  
    execute(cql, n, "DROP KEYSPACE %s")
