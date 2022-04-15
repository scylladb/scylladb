-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
create table ks.test_scylla_cdc_log (pk int primary key);
-- Should be possible to drop it
drop table ks.test_scylla_cdc_log;
DROP KEYSPACE ks;
