create table tb1 (pk int primary key, v int) with cdc = {'enabled': true};

create materialized view tb1_mv as select * from tb1_scylla_cdc_log where v is not null primary key (v);
