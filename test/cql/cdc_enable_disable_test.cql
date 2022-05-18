-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- create cdc enabled table
create table ks.t (pk int, ck int, v int, primary key(pk, ck)) with cdc = {'enabled': true};

-- this write goes to the log
insert into ks.t (pk, ck, v) values (1, 1, 100);

-- disable cdc. should retain the log
alter table ks.t with cdc = {'enabled': false};

-- add more data to base - should not generate log
insert into ks.t (pk, ck, v) values (2, 2, 200);

-- should still work, and give one row (1, 1, 100)
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v from ks.t_scylla_cdc_log;

-- lets add a column
alter table ks.t add v2 text;

-- add more data
insert into ks.t (pk, ck, v, v2) values (3, 3, 300, 'apa');

-- turn cdc back on
alter table ks.t with cdc = {'enabled': true};

-- more data - this should also go to log.
insert into ks.t (pk, ck, v, v2) values (4, 4, 400, 'snus');

-- gives two rows (1, 1, 100)+(4, 4, 400, 'snus')
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v, v2 from ks.t_scylla_cdc_log;

-- disable cdc. should retain the log
alter table ks.t with cdc = {'enabled': false};

-- remove older column
alter table ks.t drop v;

-- add more data
insert into ks.t (pk, ck, v2) values (5, 5, 'fisk');

-- turn cdc back on
alter table ks.t with cdc = {'enabled': true};

insert into ks.t (pk, ck, v2) values (6, 6, 'aborre');

-- gives three rows (1, 1)+(4, 4, 'snus')+(6, 6, 'aborre')
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v2 from ks.t_scylla_cdc_log;

DROP KEYSPACE ks;
