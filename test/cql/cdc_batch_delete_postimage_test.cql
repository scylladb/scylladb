-- do range delete in batch
create table ks.t (pk int, ck int, v int, primary key(pk, ck)) with cdc = {'enabled': true, 'preimage': true, 'postimage': true};

begin batch insert into ks.t (pk, ck, v) values (1, 1, 100); delete from t where pk = 1 and ck >= 1 and ck <= 2; apply batch;

select "cdc$batch_seq_no", "cdc$operation", pk, ck, v from t_scylla_cdc_log;

-- do pk delete in batch
create table ks.t2 (pk int, ck int, v int, primary key(pk, ck)) with cdc = {'enabled': true, 'preimage': true, 'postimage': true};

begin batch insert into ks.t2 (pk, ck, v) values (1, 1, 100); delete from t2 where pk = 1; apply batch;

select "cdc$batch_seq_no", "cdc$operation", pk, ck, v from t2_scylla_cdc_log;

-- do range delete in batch, but not matcing ck
create table ks.t3 (pk int, ck int, v int, primary key(pk, ck)) with cdc = {'enabled': true, 'preimage': true, 'postimage': true};

-- do range delete in batch
begin batch insert into ks.t3 (pk, ck, v) values (1, 1, 100); delete from t3 where pk = 1 and ck >= 2 and ck <= 3; apply batch;

select "cdc$batch_seq_no", "cdc$operation", pk, ck, v from t3_scylla_cdc_log;
