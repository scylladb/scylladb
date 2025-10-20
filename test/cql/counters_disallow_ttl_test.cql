-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
USE ks;

create table tb1 (pk int primary key, c1 counter) with default_time_to_live = 100;

create table tb2 (pk int primary key, c1 counter);
alter table tb2 with default_time_to_live = 100;

create table tb3 (pk int primary key) with default_time_to_live = 100;
alter table tb3 add (c1 counter);

create table tb4 (pk int, ck int, cs counter static, primary KEY (pk, ck)) with default_time_to_live = 100;

-- cleanup
DROP KEYSPACE ks;
