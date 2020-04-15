create table ks.t (pk int, ck int, cs counter static, c counter, primary key (pk, ck)) with cdc = {'enabled': true, 'preimage': true, 'postimage': true};

-- generates 2 rows: delta(static) + postimage(static)
update ks.t set cs = cs + 1 where pk = 0;
-- generates 3 rows: preimage(static), delta(static) + postimage(static)
update ks.t set cs = cs + 10 where pk = 0;
select c, "cdc$batch_seq_no", "cdc$operation", ck, cs from ks.t_scylla_cdc_log where pk = 0 allow filtering;

-- generates 5 rows: preimage(static), delta(static+clustered), postimage(static+clustered), but we only select ck==0
update ks.t set cs = cs + 2, c = c + 20 where pk = 0 and ck = 0;
select c, "cdc$batch_seq_no", "cdc$operation", ck, cs from ks.t_scylla_cdc_log where pk = 0 and ck = 0 allow filtering;

-- update one counter at a new PK: delta(clustered) + postimage(clustered)
update ks.t set c = c + 20 where pk = 1 and ck = 0;
-- delete clustered column: preimage(clustered) and delta(clustered) with "cdc$deleted_c"
delete c from ks.t where pk = 1 and ck = 0;
-- update dead clustered counter: nothing should happen in CDC
update ks.t set c = c + 20 where pk = 1 and ck = 0;
select c, "cdc$batch_seq_no", "cdc$deleted_c", "cdc$operation" from ks.t_scylla_cdc_log where pk = 1 and ck = 0 allow filtering;
