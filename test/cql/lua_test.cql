-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
-- setup
create table ks.my_table (key int primary key);
insert into ks.my_table (key) values (1);

-- test list<varint>
create function ks.my_func(val int) called on null input returns list<varint> language lua as 'return {1,2,3}';
select ks.my_func(key) from ks.my_table;
drop function ks.my_func;

-- test list<decimal>
create function ks.my_func(val int) called on null input returns list<decimal> language lua as 'return {1,2,3}';
select ks.my_func(key) from ks.my_table;
drop function ks.my_func;
DROP KEYSPACE ks;
