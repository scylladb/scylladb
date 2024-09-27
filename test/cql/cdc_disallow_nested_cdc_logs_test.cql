-- Error messages contain a keyspace name. Make the output stable.
-- CDC and tablets are not working together yet, turn them off.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND
    tablets = {'enabled': false};

create table ks.tbl (pk int primary key) with cdc = {'enabled': true};
alter table ks.tbl_scylla_cdc_log with cdc = {'enabled': true};
DROP KEYSPACE ks;
