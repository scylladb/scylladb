-- Error messages contain a keyspace name. Make the output stable.
-- CDC and tablets are not working together yet, turn them off.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND
    tablets = {'enabled': false};

create table ks.test_scylla_cdc_log (pk int primary key);
-- Should be possible to drop it
drop table ks.test_scylla_cdc_log;

-- cleanup
DROP KEYSPACE ks;
