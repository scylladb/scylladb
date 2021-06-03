--
-- Modified by ScyllaDB
-- from cassandra/test/unit/org/apache/cassandra/cql3/validation/operations/BatchTest.java
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
CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE k;

-- testBatch
CREATE TABLE t (userid text PRIMARY KEY, name text, password text);
BEGIN BATCH
INSERT INTO t (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')
UPDATE t SET password = 'ps22dhds' WHERE userid = 'user3'
INSERT INTO t (userid, password) VALUES ('user4', 'ch@ngem3c')
DELETE name FROM t WHERE userid = 'user1'
APPLY BATCH;
SELECT * FROM t;
DROP TABLE t;
--
-- testBatchAndList
CREATE TABLE t (k int PRIMARY KEY, l list<int>);
BEGIN BATCH
UPDATE t SET l = l + [ 1 ] WHERE k = 0
UPDATE t SET l = l + [ 2 ] WHERE k = 0
UPDATE t SET l = l + [ 3 ] WHERE k = 0
APPLY BATCH;
SELECT l FROM t WHERE k = 0;
BEGIN BATCH
UPDATE t SET l = [ 1 ] + l WHERE k = 1
UPDATE t SET l = [ 2 ] + l WHERE k = 1
UPDATE t SET l = [ 3 ] + l WHERE k = 1
APPLY BATCH;
SELECT l FROM t WHERE k = 1;
DROP TABLE t;
--
-- testBatchDeleteInsert
CREATE TABLE t (k int, v int, PRIMARY KEY (k, v));
INSERT INTO t (k, v) VALUES (0, 1);
BEGIN BATCH
DELETE FROM t WHERE k=0 AND v=1
INSERT INTO t (k, v) VALUES (0, 2)
APPLY BATCH;
SELECT * FROM t;
DROP TABLE t;

-- testBatchWithUnset
CREATE TABLE t (k int PRIMARY KEY, s text, i int);
BEGIN BATCH
INSERT INTO t JSON '{"k": 100, "s": null}' DEFAULT UNSET
INSERT INTO t JSON '{"k": 111, "i": null}' DEFAULT UNSET
APPLY BATCH;
SELECT k, s, i FROM t where k in (100,111);
DROP TABLE t;

-- testBatchUpdate
CREATE TABLE t (partitionKey int, clustering_1 int, value int, PRIMARY KEY (partitionKey, clustering_1));
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 0, 0);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 1, 1);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 2, 2);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 3, 3);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 4, 4);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 5, 5);
INSERT INTO t (partitionKey, clustering_1, value) VALUES (0, 6, 6);

BEGIN BATCH
UPDATE t SET value = 7 WHERE partitionKey = 0 AND clustering_1 = 1
UPDATE t SET value = 8 WHERE partitionKey = 0 AND (clustering_1) = (2)
UPDATE t SET value = 10 WHERE partitionKey = 0 AND clustering_1 IN (3, 4)
UPDATE t SET value = 20 WHERE partitionKey = 0 AND (clustering_1) IN ((5), (6))
APPLY BATCH;
SELECT * FROM t;
DROP TABLE t;

-- testBatchEmpty
BEGIN BATCH APPLY BATCH;

-- testBatchMultipleTable
CREATE TABLE t1 (k1 int PRIMARY KEY, v11 int, v12 int);
CREATE TABLE t2 (k2 int PRIMARY KEY, v21 int, v22 int);
BEGIN BATCH
UPDATE t1 SET v11 = 1 WHERE k1 = 0
UPDATE t1 SET v12 = 2 WHERE k1 = 0
UPDATE t2 SET v21 = 3 WHERE k2 = 0
UPDATE t2 SET v22 = 4 WHERE k2 = 0
APPLY BATCH;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t1;
SELECT * FROM t2;
DROP TABLE t1;
DROP TABLE t2;

-- testBatchWithInRestriction
CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a,b));
INSERT INTO t (a,b,c) VALUES (1, 1, 1);
INSERT INTO t (a,b,c) VALUES (1, 2, 2);
INSERT INTO t (a,b,c) VALUES (1, 3, 3);
BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
UPDATE t SET c = 200 WHERE a = 1 AND b IN () IF c = 1
APPLY BATCH;
BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
DELETE FROM t WHERE a = 1 AND b IN () IF c = 1
APPLY BATCH;
BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
UPDATE t SET c = 200 WHERE a IN () AND b = 1 IF c = 1
APPLY BATCH;
BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
DELETE FROM t WHERE a IN () AND b = 1 IF c = 1
APPLY BATCH;

BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
UPDATE t SET c = 200 WHERE a = 1 AND b IN (1, 2) IF c = 1
APPLY BATCH;

BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
DELETE FROM t WHERE a = 1 AND b IN (1, 2) IF c = 1
APPLY BATCH;

BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
UPDATE t SET c = 200 WHERE a IN (1, 2) AND b = 1 IF c = 1
APPLY BATCH;
BEGIN BATCH
UPDATE t SET c = 100 WHERE a = 1 AND b = 1 IF c = 1
DELETE FROM t WHERE a IN (1, 2) AND b = 1 IF c = 1
APPLY BATCH;

SELECT * FROM t;
DROP TABLE t;
DROP KEYSPACE k;
