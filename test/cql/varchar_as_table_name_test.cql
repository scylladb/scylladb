-- regression test for #10642
CREATE KEYSPACE reproducer WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
CREATE TABLE reproducer.varchar(pk int, ck int, PRIMARY KEY(pk, ck));
CREATE TABLE reproducer.text(pk int, ck int, PRIMARY KEY(pk, ck));
DROP KEYSPACE reproducer;
