-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
CREATE TABLE ks.tbl_cnt (pk int PRIMARY KEY, c1 counter);

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

-- delete `c1` and make sure it doesn't appear in filtering results
DELETE c1 from ks.tbl_cnt WHERE pk = 1;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 = 1 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 <= 1000 ALLOW FILTERING;
SELECT pk, c1 FROM ks.tbl_cnt WHERE c1 > -1000 ALLOW FILTERING;
DROP KEYSPACE ks;

CREATE TABLE counter_bug (t int, c counter, primary key(t));
UPDATE counter_bug SET c = c + 9223372036854775807 where t = 0;
SELECT * from counter_bug;
UPDATE counter_bug SET c = c + 1 where t = 0;
SELECT * from counter_bug;
UPDATE counter_bug SET c = c - 1 where t = 0;
SELECT * from counter_bug;
