-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
create type ks.ut (a int);
create table ks.t (pk frozen<ut> primary key);
alter type ks.ut add b int;
drop table ks.t;
alter type ks.ut add c int;
create table ks.t2 (pk frozen<list<frozen<ut>>> primary key);
alter type ks.ut add d int;
DROP KEYSPACE ks;
