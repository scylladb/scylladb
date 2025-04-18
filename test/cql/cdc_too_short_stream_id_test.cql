CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
USE ks;

create table tb (pk int primary key) with cdc = {'enabled': true};
insert into tb (pk) VALUES (0);

-- Key of length != 128 b should return empty result set (issue #6570)
select * from tb_scylla_cdc_log where "cdc$stream_id" = 0x00;

select * from tb_scylla_cdc_log where "cdc$stream_id" = 0x;

drop table tb;

-- cleanup
DROP KEYSPACE ks;
