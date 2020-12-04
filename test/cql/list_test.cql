CREATE TABLE t (pk INT PRIMARY KEY, l LIST<INT>);
UPDATE t SET l = [-1] + l WHERE pk = 0;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = [-3,-2] + l WHERE pk = 0;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = [-4] + l WHERE pk = 0 IF EXISTS;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = [-6, -5] + l WHERE pk = 0 IF EXISTS;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = l + [1] WHERE pk = 0;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = l + [2,3] WHERE pk = 0;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = l + [4] WHERE pk = 0 IF EXISTS;
SELECT l FROM t WHERE pk = 0;
UPDATE t SET l = l + [5,6] WHERE pk = 0 IF EXISTS;
SELECT l FROM t WHERE pk = 0;
BEGIN BATCH
    UPDATE t SET l = l + [7] WHERE pk = 0
    UPDATE t SET l = [-7] + l WHERE pk = 0
    UPDATE t SET l = l + [8, 9] WHERE pk = 0
    UPDATE t SET l = l + [10] WHERE pk = 0
    UPDATE t SET l = [-9, -8] + l WHERE pk = 0
    UPDATE t SET l = [-10] + l WHERE pk = 0
APPLY BATCH;
SELECT l FROM t WHERE pk = 0;
-- LWT batch
BEGIN BATCH
    UPDATE t SET l = [-11] + l WHERE pk = 0 IF EXISTS
    UPDATE t SET l = [-13, -12] + l WHERE pk = 0 IF EXISTS
    UPDATE t SET l = [-14] + l WHERE pk = 0 IF EXISTS
    UPDATE t SET l = l + [11] WHERE pk = 0 IF EXISTS
    UPDATE t SET l = l + [12, 13] WHERE pk = 0 IF EXISTS
    UPDATE t SET l = l + [14] WHERE pk = 0 IF EXISTS
APPLY BATCH;
SELECT l FROM t WHERE pk = 0;

DROP TABLE t;
