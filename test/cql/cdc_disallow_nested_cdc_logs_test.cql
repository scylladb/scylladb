create table tbl (pk int primary key) with cdc = {'enabled': true};
alter table tbl_scylla_cdc_log with cdc = {'enabled': true};
