create table tb2 (pk int, ck int, i1 int, PRIMARY KEY (pk, ck)) with cdc = {'enabled': true, 'preimage': true, 'postimage': true, 'delta': 'keys'};
-- Should add 3 rows (preimage + postimage + delta). Delta has only key columns and "pk" + "ck"
insert into tb2 (pk, ck, i1) VALUES (2, 22, 222) USING TTL 2222;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, i1 from tb2_scylla_cdc_log where pk = 2 and ck = 22 allow filtering;

alter table tb2 with cdc = {'enabled': true, 'preimage': true, 'postimage': true, 'delta': 'full'};
-- Should add 3 rows (preimage + postimage + delta)
insert into tb2 (pk, ck, i1) VALUES (3, 33, 333) USING TTL 3333;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, i1 from tb2_scylla_cdc_log where pk = 3 and ck = 33 allow filtering;
drop table tb2;
