CREATE TABLE ks.tbl_cdc_lwt (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled':true, 'preimage':true};

-- (0) successful insert
INSERT INTO ks.tbl_cdc_lwt (pk, ck, val) VALUES (1, 1, 111) IF NOT EXISTS;
-- (1) unsuccessful insert
INSERT INTO ks.tbl_cdc_lwt (pk, ck, val) VALUES (1, 1, 222) IF NOT EXISTS;
-- (2) successful update
UPDATE ks.tbl_cdc_lwt set val = 333 WHERE pk = 1 and ck = 1 IF EXISTS;
-- (3) unsuccessful update
UPDATE ks.tbl_cdc_lwt set val = 444 WHERE pk = 888 and ck = 777 IF EXISTS;
-- (4) successful row delete
DELETE FROM ks.tbl_cdc_lwt WHERE pk = 1 AND ck = 1 IF EXISTS;
-- (5) unsuccessful row delete
DELETE FROM ks.tbl_cdc_lwt WHERE pk = 1 AND ck = 1 IF EXISTS;

SELECT "cdc$batch_seq_no", "cdc$operation", ck, pk, val FROM ks.tbl_cdc_lwt_scylla_cdc_log;

-- there should be 6 rows in total: (0) + preimg(0) + (2) + preimg(2) + (4) + preimg(4)
SELECT count(*) FROM ks.tbl_cdc_lwt_scylla_cdc_log;
