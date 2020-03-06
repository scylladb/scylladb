--
-- https://github.com/scylladb/scylla/issues/5935
-- crash if all_replicas is empty test we work well when the list of
-- natural endpoints is empty
--
CREATE KEYSPACE t WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'nosuchdc' : 3 } AND DURABLE_WRITES =  true;
CREATE TABLE t.t (a INT PRIMARY KEY, b int);
INSERT INTO t.t (a, b) VALUES (1, 1);
INSERT INTO t.t (a, b) VALUES (2, 2);
SELECT * FROM t.t ALLOW FILTERING;
-- This statement used to trigger a crash
SELECT a FROM t.t WHERE a IN (1, 2);
DELETE FROM t.t WHERE a = 1;
DELETE FROM t.t WHERE a = 2;
CREATE INDEX b ON t.t (b);
SELECT * FROM t.t WHERE b=2;
INSERT INTO t.t (a) VALUES (1) IF NOT EXISTS;
DELETE FROM t.t WHERE a=1 IF EXISTS;
CREATE MATERIALIZED VIEW t.mv AS SELECT a, b FROM t.t WHERE b > 1 PRIMARY KEY (b, a);
SELECT * FROM t.mv WHERE b IN (2, 1);
DROP MATERIALIZED VIEW t.mv;
DROP TABLE t.t;
DROP KEYSPACE t;
--
-- https://github.com/scylladb/scylla/issues/5962
-- wrong de-facto replication factor
-- when RF=0 and SimpleStrategy
--
CREATE KEYSPACE t WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 0 } AND DURABLE_WRITES =  true;
CREATE TABLE t.t (a INT PRIMARY KEY, b int);
INSERT INTO t.t (a, b) VALUES (1, 1);
INSERT INTO t.t (a, b) VALUES (2, 2);
SELECT * FROM t.t ALLOW FILTERING;
-- This statement used to trigger a crash
SELECT a FROM t.t WHERE a IN (1, 2);
DELETE FROM t.t WHERE a = 1;
DELETE FROM t.t WHERE a = 2;
CREATE INDEX b ON t.t (b);
SELECT * FROM t.t WHERE b=2;
INSERT INTO t.t (a) VALUES (1) IF NOT EXISTS;
DELETE FROM t.t WHERE a=1 IF EXISTS;
CREATE MATERIALIZED VIEW t.mv AS SELECT a, b FROM t.t WHERE b > 1 PRIMARY KEY (b, a);
SELECT * FROM t.mv WHERE b IN (2, 1);
DROP MATERIALIZED VIEW t.mv;
DROP TABLE t.t;
DROP KEYSPACE t;
