-- single null value
CREATE KEYSPACE reproducer WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
CREATE TABLE reproducer.t(pk int PRIMARY KEY, v int);
INSERT INTO reproducer.t (pk) VALUES (1);
SELECT TOKEN(v) FROM reproducer.t;

-- multiple null values
CREATE TABLE reproducer.t2(pk int PRIMARY KEY, a int, b int);
INSERT INTO reproducer.t2 (pk) VALUES (1);
SELECT TOKEN(pk, a, b) FROM reproducer.t2;
SELECT TOKEN(pk, b) FROM reproducer.t2;
SELECT TOKEN(a, b) FROM reproducer.t2;

DROP KEYSPACE reproducer;