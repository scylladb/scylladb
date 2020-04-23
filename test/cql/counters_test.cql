CREATE TABLE ks.tbl_cnt (pk int PRIMARY KEY, c1 counter, c2 counter);

-- insert some values in one column
UPDATE ks.tbl_cnt SET c1 = c1+1 WHERE pk = 1;
UPDATE ks.tbl_cnt SET c1 = c1+2 WHERE pk = 2;
UPDATE ks.tbl_cnt SET c1 = c1+3 WHERE pk = 3;
UPDATE ks.tbl_cnt SET c1 = c1+4 WHERE pk = 4;

-- test various filtering options on counter column
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 < 3 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 < 1 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 <= 3 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 > 2 AND pk = 4 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 >= 3 and pk = 3 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 > 4 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 in (-1, 2, 3) ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 = 0 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 = 1 ALLOW FILTERING;

-- now filter through untouched counters `c2` - they should appear as NULLs and evaluate as zeros
SELECT pk, c1, c2 FROM ks.tbl_cnt WHERE c2 = 0 ALLOW FILTERING;
SELECT pk, c2 FROM ks.tbl_cnt WHERE c2 < 0 ALLOW FILTERING;
SELECT pk, c2 FROM ks.tbl_cnt WHERE c2 > 0 ALLOW FILTERING;

-- delete `c1` and make sure it doesn't appear in filtering results
DELETE c1 from ks.tbl_cnt WHERE pk = 1;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 = 1 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 <= 1000 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 > -1000 ALLOW FILTERING;
