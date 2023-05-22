-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
create table ks.tb1 (pk int primary key, c1 counter) with cdc = {'enabled': true};

create table ks.tb2 (pk int primary key, c1 counter);
alter table ks.tb2 with cdc = {'enabled': true};

create table ks.tb3 (pk int primary key) with cdc = {'enabled': true};
alter table ks.tb3 add (c1 counter);
DROP KEYSPACE ks;
