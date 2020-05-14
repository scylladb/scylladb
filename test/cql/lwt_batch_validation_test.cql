CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE k;
CREATE TABLE t1 (userid int PRIMARY KEY);
CREATE TABLE t2 (userid int PRIMARY KEY);
BEGIN BATCH
INSERT INTO t1 (userid) VALUES (1) IF NOT EXISTS
INSERT INTO t2 (userid) VALUES (1) IF NOT EXISTS
APPLY BATCH;
