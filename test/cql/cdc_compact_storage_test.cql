create table tb2 (pk int, ck int, v int, PRIMARY KEY (pk, ck)) with compact storage and cdc = {'enabled': true, 'preimage': true, 'postimage': true};
-- Should add 2 rows (postimage + delta).
insert into tb2 (pk, ck, v) VALUES (2, 22, 111) USING TTL 2222;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v from tb2_scylla_cdc_log;
-- Should add 3 rows (preimage + postimage + delta).
insert into tb2 (pk, ck, v) VALUES (2, 22, 1111) USING TTL 2223;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v from tb2_scylla_cdc_log;
create table tb3 (pk int, ck int, PRIMARY KEY (pk, ck)) with compact storage and cdc = {'enabled': true, 'preimage': true, 'postimage': true};
-- Should add 2 rows (postimage + delta).
insert into tb3 (pk, ck) VALUES (2, 22) USING TTL 2222;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck from tb3_scylla_cdc_log;
-- Should add 3 rows (preimage + postimage + delta).
insert into tb3 (pk, ck) VALUES (2, 22) USING TTL 2223;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck from tb3_scylla_cdc_log;
drop table tb2;
drop table tb3;
