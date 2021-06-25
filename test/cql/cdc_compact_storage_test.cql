create table tb2 (pk int, ck int, PRIMARY KEY (pk, ck)) with compact storage and cdc = {'enabled': true, 'preimage': true, 'postimage': true};
-- Should add 3 rows (preimage + postimage + delta). Delta has only key columns and "pk" + "ck"
insert into tb2 (pk, ck) VALUES (2, 22) USING TTL 2222;
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck from tb2_scylla_cdc_log;
