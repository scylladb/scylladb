-- Important: using the same partition key for each row so the results of select from log table
-- do not depend on how stream IDs are assigned to partition keys.

-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND
    tablets = {'enabled': true};

-- create table with vector column
create table ks.t (pk int, ck int, v vector<float, 3>, primary key(pk, ck));

-- create index on vector column using 'vector_index' custom class
-- this enables CDC automatically
create index on ks.t (v) using 'vector_index';

-- this write goes to the log
insert into ks.t (pk, ck, v) values (0, 1, [100.0, 101.0, 102.0]);

-- drop index disable CDC. should retain the log
drop index ks.t_v_idx;

-- add more data to base - should not generate log
insert into ks.t (pk, ck, v) values (0, 2, [200.0, 201.0, 202.0]);

-- should still work, and give one row (0, 1, [100.0, 101.0, 102.0])
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v from ks.t_scylla_cdc_log;

-- add more data
insert into ks.t (pk, ck, v) values (0, 3, [300.0, 301.0, 302.0]);

-- recreate the index
create index on ks.t (v) using 'vector_index';

-- more data - this should also go to log.
insert into ks.t (pk, ck, v) values (0, 4, [400.0, 401.0, 402.0]);

-- gives two rows (0, 1, [100.0, 101.0, 102.0])+(0, 4, [400.0, 401.0, 402.0])
select "cdc$batch_seq_no", "cdc$operation", "cdc$ttl", pk, ck, v from ks.t_scylla_cdc_log;

DROP KEYSPACE ks;
