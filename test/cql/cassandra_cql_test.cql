-- 
-- Modified by ScyllaDB
-- from Cassandra cassandra-dtest/cql_test.py
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
--------------------------------------------------------------------------------
--
-- Copyright (C) 2016-present ScyllaDB
-- 
-- Modified by ScyllaDB
-- 
-- This file is part of Scylla.
-- 
-- Scylla is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
-- 
-- Scylla is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU General Public License
-- along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

-- setup
CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        AND DURABLE_WRITES = true;
ALTER KEYSPACE ks1 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };
SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'ks1';
USE ks1;

-- test_table
CREATE TABLE test1 (k int PRIMARY KEY, v1 int);
ALTER TABLE test1 ADD v2 int;
INSERT INTO test1 (k, v1, v2) VALUES (0, 0, 0);
INSERT INTO test1 (k, v1, v2) VALUES (1, 1, 1);
INSERT INTO test1 (k, v1, v2) VALUES (2, 2, 2);
INSERT INTO test1 (k, v1, v2) VALUES (3, 3, 3);
INSERT INTO test1 (k, v1, v2) VALUES (4, 4, 4);
INSERT INTO test1 (k, v1, v2) VALUES (5, 5, 5);
INSERT INTO test1 (k, v1, v2) VALUES (6, 6, 6);
INSERT INTO test1 (k, v1, v2) VALUES (7, 7, 7);
INSERT INTO test1 (k, v1, v2) VALUES (8, 8, 8);
INSERT INTO test1 (k, v1, v2) VALUES (9, 9, 9);
SELECT * FROM test1;
-- TRUNCATE TABLE test1;
-- SELECT * FROM test1;
DROP TABLE test1;

-- test_table_compact_storage
CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE;
INSERT INTO test2 (k, c1, v1) VALUES (0, 0, 0);
INSERT INTO test2 (k, c1, v1) VALUES (1, 1, 1);
INSERT INTO test2 (k, c1, v1) VALUES (2, 2, 2);
INSERT INTO test2 (k, c1, v1) VALUES (3, 3, 3);
INSERT INTO test2 (k, c1, v1) VALUES (4, 4, 4);
INSERT INTO test2 (k, c1, v1) VALUES (5, 5, 5);
INSERT INTO test2 (k, c1, v1) VALUES (6, 6, 6);
INSERT INTO test2 (k, c1, v1) VALUES (7, 7, 7);
INSERT INTO test2 (k, c1, v1) VALUES (8, 8, 8);
INSERT INTO test2 (k, c1, v1) VALUES (9, 9, 9);
-- TRUNCATE TABLE test2;
-- SELECT * FROM test2;
DROP TABLE test2;

-- test_index
CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int);
CREATE INDEX testidx ON test3 (v1);
INSERT INTO test3 (k, v1, v2) VALUES (0, 0, 0);
INSERT INTO test3 (k, v1, v2) VALUES (1, 1, 1);
INSERT INTO test3 (k, v1, v2) VALUES (2, 2, 2);
INSERT INTO test3 (k, v1, v2) VALUES (3, 3, 3);
INSERT INTO test3 (k, v1, v2) VALUES (4, 4, 4);
INSERT INTO test3 (k, v1, v2) VALUES (5, 5, 5);
INSERT INTO test3 (k, v1, v2) VALUES (6, 6, 6);
INSERT INTO test3 (k, v1, v2) VALUES (7, 7, 7);
INSERT INTO test3 (k, v1, v2) VALUES (8, 8, 8);
INSERT INTO test3 (k, v1, v2) VALUES (9, 9, 9);

-- test_type
CREATE TYPE address_t (street text, city text, zip_code int);
CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>);
ALTER TYPE address_t ADD phones set<text>;
DROP TABLE test4;
DROP TYPE address_t;

-- test_statements
CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time));
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 0, 0, 0);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 1, 1, 1);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 2, 2, 2);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 3, 3, 3);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 4, 4, 4);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 5, 5, 5);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 6, 6, 6);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 7, 7, 7);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 8, 8, 8);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', 9, 9, 9);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 0, 0, 0);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 1, 1, 1);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 2, 2, 2);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 3, 3, 3);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 4, 4, 4);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 5, 5, 5);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 6, 6, 6);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 7, 7, 7);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 8, 8, 8);
INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', 9, 9, 9);

SELECT COUNT(*) FROM test7 WHERE kind = 'ev1';
SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2');
SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0;
SELECT * FROM test7 WHERE kind = 'ev1';
SELECT * FROM test7 WHERE kind = 'ev2';
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=0;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=1;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=2;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=3;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=4;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=5;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=6;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=7;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=8;
UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=9;
SELECT * FROM test7 WHERE kind = 'ev1';
DELETE FROM test7 WHERE kind = 'ev1';
SELECT * FROM test7 WHERE kind = 'ev1';
SELECT COUNT(*) FROM test7 WHERE kind = 'ev1';
DROP TABLE test7;

-- test_partition_key_allow_filtering(self):
CREATE TABLE IF NOT EXISTS test_filter (
    k1  int,
    k2  int,
    ck1 int,
    v   int,
    PRIMARY KEY ((k1, k2), ck1));
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 0, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 1, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 2, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 3, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 0, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 1, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 2, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 3, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 0, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 1, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 2, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 3, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 0, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 1, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 2, 0);
INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 3, 0);
SELECT * FROM test_filter WHERE k1 = 0 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 = 2 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 <=0 AND k2 > 1 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k2 <= 0 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 <= 0 AND k2 = 0 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k2 = 1 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k2 = 2 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 = 0 AND ck1=0 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 = 0 AND k2=1 AND ck1=0 ALLOW FILTERING;
SELECT count(*) FROM test_filter WHERE k2 = 0 ALLOW FILTERING;
SELECT count(*) FROM test_filter WHERE k2 = 1 ALLOW FILTERING;
SELECT count(*) FROM test_filter WHERE k2 = 2 ALLOW FILTERING;
SELECT * FROM test_filter WHERE k1 = 0;
SELECT * FROM test_filter WHERE k1 = 0 AND k2 > 0;
SELECT * FROM test_filter WHERE k1 >= 0 AND k2 in (0,1,2);
SELECT * FROM test_filter WHERE k2 > 0;
DROP TABLE test_filter;

-- test_lwt_with_static_columns
CREATE TABLE lwt_with_static (a int, b int, s int static, d text, PRIMARY KEY (a, b));
UPDATE lwt_with_static SET s = 1 WHERE a = 1 IF s = NULL;
SELECT * FROM lwt_with_static;
UPDATE lwt_with_static SET s = 2 WHERE a = 2 IF EXISTS;
SELECT * FROM lwt_with_static WHERE a = 1;
INSERT INTO lwt_with_static (a, s) VALUES (2, 2) IF NOT EXISTS;
SELECT * FROM lwt_with_static WHERE a = 2;
BEGIN BATCH
INSERT INTO lwt_with_static (a, b, d) values (3, 3, 'a')
UPDATE lwt_with_static SET s = 3 WHERE a = 3 IF s = null
APPLY BATCH;
SELECT * FROM lwt_with_static WHERE a = 3;
--        # LWT applies before INSERT
BEGIN BATCH
INSERT INTO lwt_with_static (a, b, d) values (4, 4, 'a')
UPDATE lwt_with_static SET s = 4 WHERE a = 4 IF s = null
APPLY BATCH;
SELECT * FROM lwt_with_static WHERE a = 4;

-- test_conditional_updates_on_static_columns_with_null_values(self):
CREATE TABLE conditional_updates_on_static_columns_with_null (a int, b int, s int static, d text, PRIMARY KEY (a, b));
INSERT INTO conditional_updates_on_static_columns_with_null (a, b) VALUES (1, 1);
INSERT INTO conditional_updates_on_static_columns_with_null (a, b) VALUES (2, 2);
INSERT INTO conditional_updates_on_static_columns_with_null (a, b) VALUES (3, 3);
INSERT INTO conditional_updates_on_static_columns_with_null (a, b) VALUES (4, 4);
INSERT INTO conditional_updates_on_static_columns_with_null (a, b) VALUES (5, 5);
-- sub _validate_non_existing_or_null_values
UPDATE conditional_updates_on_static_columns_with_null SET s = 1 WHERE a = 1 IF s = NULL;
SELECT a, s, d FROM conditional_updates_on_static_columns_with_null WHERE a = 1;
UPDATE conditional_updates_on_static_columns_with_null SET s = 2 WHERE a = 2 IF s IN (10,20,NULL);
SELECT a, s, d FROM conditional_updates_on_static_columns_with_null WHERE a = 2;
UPDATE conditional_updates_on_static_columns_with_null SET s = 4 WHERE a = 4 IF s != 4;
SELECT a, s, d FROM conditional_updates_on_static_columns_with_null WHERE a = 4;

UPDATE conditional_updates_on_static_columns_with_null  SET s = 30 WHERE a = 3 IF s IN (10,20,30);
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 3;

UPDATE conditional_updates_on_static_columns_with_null SET s = 50 WHERE a = 5 IF s > 3;
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_null SET s = 50 WHERE a = 5 IF s < 3;
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_null SET s = 50 WHERE a = 5 IF s >= 3;
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_null SET s = 50 WHERE a = 5 IF s <= 3;
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_null SET s = 50 WHERE a = 5 IF s = 3;
SELECT * FROM conditional_updates_on_static_columns_with_null WHERE a = 5;

-- test_conditional_updates_on_static_columns_with_non_existing_values
--         conditional_updates_on_static_columns_with_ne
CREATE TABLE conditional_updates_on_static_columns_with_ne (a int, b int, s int static, d text, PRIMARY KEY (a, b));

-- sub _validate_non_existing_or_null_values
UPDATE conditional_updates_on_static_columns_with_ne SET s = 1 WHERE a = 1 IF s = NULL;
SELECT a, s, d FROM conditional_updates_on_static_columns_with_ne WHERE a = 1;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 2 WHERE a = 2 IF s IN (10,20,NULL);
SELECT a, s, d FROM conditional_updates_on_static_columns_with_ne WHERE a = 2;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 4 WHERE a = 4 IF s != 4;
SELECT a, s, d FROM conditional_updates_on_static_columns_with_ne WHERE a = 4;

UPDATE conditional_updates_on_static_columns_with_ne SET s = 30 WHERE a = 3 IF s IN (10,20,30);
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 3;

UPDATE conditional_updates_on_static_columns_with_ne SET s = 50 WHERE a = 5 IF s > 3;
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 50 WHERE a = 5 IF s < 3;
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 50 WHERE a = 5 IF s >= 3;
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 50 WHERE a = 5 IF s <= 3;
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 5;
UPDATE conditional_updates_on_static_columns_with_ne SET s = 50 WHERE a = 5 IF s = 3;
SELECT * FROM conditional_updates_on_static_columns_with_ne WHERE a = 5;

-- test_conditional_updates_on_static_columns_with_null_values_batch(self):
CREATE TABLE lwt_on_static_columns_with_null_batch (a int, b int, s int static, d text, PRIMARY KEY (a, b));
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (1, 1);
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (2, 2);
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (3, 3);
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (4, 4);
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (5, 5);
INSERT INTO lwt_on_static_columns_with_null_batch (a, b) VALUES (6, 6);

--    sub _validate_non_existing_or_null_values_batch
BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, d) values (2, 2, 'a')
UPDATE lwt_on_static_columns_with_null_batch  SET s = 2 WHERE a = 2 IF s = null
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 2;

BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (4, 4, 4, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 5 WHERE a = 4 IF s = null
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 4;

BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (5, 5, 5, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 6 WHERE a = 5 IF s IN (1,2,null)
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 5;

BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (7, 7, 7, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 8 WHERE a = 7 IF s != 7
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 7;

BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (3, 3, 40, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 30 WHERE a = 3 IF s > 5
APPLY BATCH;
BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (3, 3, 40, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 30 WHERE a = 3 IF s < 5
APPLY BATCH;
BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (3, 3, 40, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 30 WHERE a = 3 IF s >= 5
APPLY BATCH;
BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (3, 3, 40, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 30 WHERE a = 3 IF s <= 5
APPLY BATCH;
BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (3, 3, 40, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 30 WHERE a = 3 IF s = 5
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 3;

BEGIN BATCH
INSERT INTO lwt_on_static_columns_with_null_batch (a, b, s, d) values (6, 6, 70, 'a')
UPDATE lwt_on_static_columns_with_null_batch SET s = 60 WHERE a = 6 IF s IN (1,2,3)
APPLY BATCH;

SELECT * FROM lwt_on_static_columns_with_null_batch WHERE a = 6;

-- test_conditional_deletes_on_static_columns_with_null_values
CREATE TABLE conditional_deletes_on_static_with_null (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b));
INSERT INTO conditional_deletes_on_static_with_null (a, b, s1, s2, v) VALUES (1, 1, 1, null, 1);
INSERT INTO conditional_deletes_on_static_with_null (a, b, s1, s2, v) VALUES (2, 2, 2, null, 2);
INSERT INTO conditional_deletes_on_static_with_null (a, b, s1, s2, v) VALUES (3, 3, 3, null, 3);
INSERT INTO conditional_deletes_on_static_with_null (a, b, s1, s2, v) VALUES (4, 4, 4, null, 4);
INSERT INTO conditional_deletes_on_static_with_null (a, b, s1, s2, v) VALUES (5, 5, 5, null, 5);

DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 1 IF s2 = null;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 1;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 2 IF s2 IN (10,20,30);
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 2;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 3 IF s2 IN (null,20,30);
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 3;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 4 IF s2 != 4;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 4;

DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 5 IF s2 > 3;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 5;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 5 IF s2 < 3;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 5;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 5 IF s2 >= 3;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 5;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 5 IF s2 <= 3;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 5;
DELETE s1 FROM conditional_deletes_on_static_with_null WHERE a = 5 IF s2 = 3;
SELECT * FROM conditional_deletes_on_static_with_null WHERE a = 5;

-- test_conditional_deletes_on_static_columns_with_null_values_batch(self):
CREATE TABLE conditional_deletes_on_static_with_null_batch (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b));

BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch  (a, b, s1, v) values (2, 2, 2, 2)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch  WHERE a = 2 IF s2 = null
APPLY BATCH;

SELECT * FROM conditional_deletes_on_static_with_null_batch  WHERE a = 2;

BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (3, 3, 3, 3)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 3 IF s2 < 5
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch   WHERE a = 3;
BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (3, 3, 3, 3)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 3 IF s2 < 5
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch   WHERE a = 3;
BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (3, 3, 3, 3)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 3 IF s2 >= 5
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch   WHERE a = 3;
BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (3, 3, 3, 3)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 3 IF s2 <= 5
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch   WHERE a = 3;
BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (3, 3, 3, 3)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 3 IF s2 = 5
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch   WHERE a = 3;
-- none

BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (6, 6, 6, 6)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 6 IF s2 IN (1,2,3)
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch    WHERE a = 6;
BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (4, 4, 4, 4)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 4 IF s2 = null
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch     WHERE a = 4;

BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) VALUES (5, 5, 5, 5)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 5 IF s1 IN (1,2,null)
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch      WHERE a = 5;

BEGIN BATCH
INSERT INTO conditional_deletes_on_static_with_null_batch (a, b, s1, v) values (7, 7, 7, 7)
DELETE s1 FROM conditional_deletes_on_static_with_null_batch WHERE a = 7 IF s2 != 7
APPLY BATCH;
SELECT * FROM conditional_deletes_on_static_with_null_batch       WHERE a = 7;

