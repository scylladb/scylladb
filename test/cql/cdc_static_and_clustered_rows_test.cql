create table ks.t (pk int, ck int, vs int static, vc int, primary key (pk, ck)) with cdc = {'enabled': true, 'preimage': true};

-- generates 1 row: delta(static)
update ks.t set vs = 0 where pk = 0;
-- generates 2 rows: preimage(static), delta(static)
update ks.t set vs = 1 where pk = 0;
select "cdc$batch_seq_no", "cdc$operation", ck, vs, vc from ks.t_scylla_cdc_log where pk = 0 allow filtering;

-- generates 3 rows: preimage(static), delta(static), delta(clustering)
update ks.t set vs = 2, vc = 2 where pk = 0 and ck = 0;
-- generates 4 rows: preimage(static), preimage(clustering), delta(static), delta(clustering)
update ks.t set vs = 3, vc = 3 where pk = 0 and ck = 0;
select "cdc$batch_seq_no", "cdc$operation", ck, vs, vc from ks.t_scylla_cdc_log where pk = 0 and ck = 0 allow filtering;

-- generates 1 row: delta(clustering)
update ks.t set vc = 4 where pk = 1 and ck = 0;
-- generates 2 rows: preimage(clustering), delta(clustering)
update ks.t set vc = 5 where pk = 1 and ck = 0;
select "cdc$batch_seq_no", "cdc$operation", ck, vs, vc from ks.t_scylla_cdc_log where pk = 1 and ck = 0 allow filtering;

-- there should be 13 rows in total
select count(*) from ks.t_scylla_cdc_log;
