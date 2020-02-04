-- setup
create table my_table (key int primary key);
insert into my_table (key) values (1);

-- test list<varint>
create function my_func(val int) called on null input returns list<varint> language lua as 'return {1,2,3}';
select my_func(key) from my_table;
drop function my_func;

-- test list<decimal>
create function my_func(val int) called on null input returns list<decimal> language lua as 'return {1,2,3}';
select my_func(key) from my_table;
drop function my_func;
