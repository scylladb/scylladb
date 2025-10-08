-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

CREATE TABLE ks.tbl_cdc_lwt_full (pk int, ck int, val1 int, val2 int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled':true, 'preimage': 'full'};

-- (0) successful insert
INSERT INTO ks.tbl_cdc_lwt_full (pk, ck, val1, val2) VALUES (1, 1, 111, 222) IF NOT EXISTS;
-- (1) unsuccessful insert
INSERT INTO ks.tbl_cdc_lwt_full (pk, ck, val1) VALUES (1, 1, 222) IF NOT EXISTS;
-- (2) successful update
UPDATE ks.tbl_cdc_lwt_full set val1 = 333 WHERE pk = 1 and ck = 1 IF EXISTS;
-- (3) unsuccessful update
UPDATE ks.tbl_cdc_lwt_full set val1 = 444 WHERE pk = 888 and ck = 777 IF EXISTS;
-- (4) successful row delete
DELETE FROM ks.tbl_cdc_lwt_full WHERE pk = 1 AND ck = 1 IF EXISTS;
-- (5) unsuccessful row delete
DELETE FROM ks.tbl_cdc_lwt_full WHERE pk = 1 AND ck = 1 IF EXISTS;

SELECT "cdc$batch_seq_no", "cdc$operation", ck, pk, val1 FROM ks.tbl_cdc_lwt_full_scylla_cdc_log;

-- there should be 5 rows in total: (0) + preimg(0) + (2) + preimg(2) + (4)
SELECT count(*) FROM ks.tbl_cdc_lwt_full_scylla_cdc_log;

-- cleanup
-- DROP KEYSPACE ks;
