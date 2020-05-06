create table lwt (a int, b int, primary key (a,b));
-- basic batch: ok
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
apply batch;
-- begin unlogged batch + lwt: ok; unlogged is ignored
-- (same in c*)
begin unlogged batch
    insert into lwt (a, b) values (1, 1) if not exists
apply batch;
-- begin counter batch + lwt: error
begin counter batch
    insert into lwt (a, b) values (1, 1) if not exists
apply batch;
-- a batch affecting two partitions: error
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into lwt (a, b) values (2, 1) if not exists
apply batch;
-- a batch with an LWT statement and other statement affecting another
-- partition: error
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into lwt (a, b) values (2, 1)
apply batch;
-- a batch affecting different clustering keys of the same partition: ok
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into lwt (a, b) values (1, 2) if not exists
apply batch;
-- a batch and non-batch statement, on the same partition: ok
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into lwt (a, b) values (1, 2)
apply batch;
-- a batch affecting two tables: error
create table two (a int primary key, b int);
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into two (a, b) values (1, 1)
apply batch;
drop table two;
-- a batch with custom timestamp set: error
begin batch using timestamp 1
    insert into lwt (a, b) values (1, 1) if not exists
apply batch;
-- a batch with a statement with a custom timestamp: error
begin batch
    insert into lwt (a, b) values (1, 1) if not exists using timestamp 1
apply batch;
-- a batch with an LWT statement and another statement with a custom timestamp set: ok
begin batch
    insert into lwt (a, b) values (1, 1) if not exists
    insert into lwt (a, b) values (1, 1) using timestamp 1
apply batch;
-- a batch with if condition and counters: error
-- if a table has a counter, all its non-primary key columns
-- must be counters
create table lcounter (a int primary key, c counter);
begin batch
    update lcounter set c = c + 1 where a = 1 if c = null
apply batch;
drop table lcounter;
drop table lwt;
create table lwt (a int, b int, c int, primary key (a,b));
-- a batch with IF condition on primary key: error
begin batch
    update lwt set c = 2 where a = 1 and b = 1 if a > 0
apply batch;
begin batch
    update lwt set c = 2 where a = 1 and b = 1 if b > 0
apply batch;
begin batch
    update lwt set c = 2 where a = 1 if c = null
apply batch;
-- a batch with a statement that has IN prediacte and IF condition: error
begin batch
    update lwt set c = 2 where a = 1 and b in (1, 2) if c = null
apply batch;
-- a batch with a statement that has IN prediacte and *another* statement
-- with IF condition: OK. This is an error in C* but I see no reason
-- why it should be an error in Scylla.
delete from lwt where a = 1;
begin batch
    update lwt set c = 2 where a = 1 and b = 1 if c = null
    update lwt set c = 2 where a = 1 and b in (1, 2)
apply batch;
select a, b, c from lwt where a =  1 and b in (1, 2);
drop table lwt;
--
-- BATCH LWT with multiple statements on LIST data type,
-- append/discard operations on the type: all append/discard
-- operations from all stsatemetns of the batch are applied
--
create table lwt (a int, b int, c list<text>, d list<text> static, primary key (a, b));
begin batch
    insert into lwt (a, b, c, d ) values (1, 1, ['1'], ['1']) if not exists
    insert into lwt (a, b, c, d ) values (1, 2, ['2'], ['2']) if not exists
apply batch;
select b, c, d from lwt where a = 1;
begin batch
    update lwt set c = c + ['3'], d = d + ['3'] where a = 1 and b = 1 if exists
    update lwt set c = c + ['4'], d = d + ['4'] where a = 1 and b = 2
apply batch;
select b, c, d from lwt where a = 1;
begin batch
    update lwt set c = c + ['5'], d = d + ['5'] where a = 1 and b = 1 if c[0] = '1' and c[1] = '3'
    update lwt set c = c + ['6'], d = d + ['6'] where a = 1 and b = 2
apply batch;
select b, c, d from lwt where a = 1;
-- multiple conditions:
-- two simple conditions, effects of all statements are applied atomically
-- or not applied at all: ok
begin batch
    update lwt set c = c + ['7'], d = d + ['7'] where a = 1 and b = 1 if c[0] = '1' and c[1] = '2'
    update lwt set c = c + ['8'], d = d + ['8'] where a = 1 and b = 2 if c[3] = '3' and c[4] = '4'
apply batch;
select b, c, d from lwt where a = 1;
begin batch
    update lwt set c = c + ['7'], d = d + ['7'] where a = 1 and b = 1 if c[0] = '1' and c[1] = '3'
    update lwt set c = c + ['8'], d = d + ['8'] where a = 1 and b = 2 if c[1] = '4' and c[2] = '6'
apply batch;
select b, c, d from lwt where a = 1;
drop table lwt;

-- batch + lwt + range UPDATE of multiple rows, clustering key is not fully restricted: ok
-- batch + lwt + range DELETE of multiple rows, clustering key is not fully restricted: ok
-- usually within the same partition key
create table lwt (a int, b int, c int, d int, primary key (a, b, c));
insert into lwt (a, b, c, d) values (1,1,1,1);
insert into lwt (a, b, c, d) values (1,1,2,0);
insert into lwt (a, b, c, d) values (1,1,3,1);
insert into lwt (a, b, c, d) values (1,2,1,1);
insert into lwt (a, b, c, d) values (1,2,2,0);
insert into lwt (a, b, c, d) values (1,2,3,1);
-- update
begin batch
    update lwt set d = 7 where a = 1 and b = 1 and c = 1 if exists
    update lwt set d = 7 where a = 1 and b in  (1,2) and c in (1,2,3)
apply batch;
select a, b, c, d from lwt where a = 1;
-- delete
begin batch
    delete from lwt where a = 1 and b = 1 if exists
    delete from lwt where a = 1 and b = 2 if exists
apply batch;
-- select a, b, c, d from lwt where a = 1;
drop table lwt;

-- Rows fetched for statements that require read but has no conditions
-- must not be included into the result set.
create table lwt(p int, c int, i int, l list<int>, primary key(p, c));
insert into lwt(p, c, i, l) values(1, 1, 1, [1, 2]);
insert into lwt(p, c, i, l) values(1, 2, 2, [1, 2, 3, 4]);
-- doesn't apply
begin batch
update lwt set i = 3 where p = 1 and c = 1 if i = 2
update lwt set l = l - [1] where p = 1 and c = 2
apply batch;
-- applies
begin batch
update lwt set i = 2 where p = 1 and c = 1 if i = 1
update lwt set l = l - [2] where p = 1 and c = 2
apply batch;
-- doesn't apply
begin batch
delete l from lwt where p = 1 and c = 3 if exists
update lwt set l = l - [3] where p = 1 and c = 2
apply batch;
-- applies
begin batch
delete l from lwt where p = 1 and c = 1 if exists
update lwt set l = l - [4] where p = 1 and c = 2
apply batch;
select * from lwt;
drop table lwt;

-- Suppose there's a batch with two statements, one of which has clustering
-- column restrictions that select no row, another has static conditions.
-- In this case we must fetch the static row (provided it exists, of course)
-- to check the static column conditions.
create table lwt (p int, c int, s int static, primary key (p, c));
insert into lwt(p, s) values(1, 1);
begin batch
insert into lwt(p, c) values(1, 1) if not exists
update lwt set s = 2 where p = 1 if s = 1
apply batch;
select * from lwt;
drop table lwt;
--
--
-- condtitions on different list columns and different rows of the same
-- partition, these columns are retreived fine and the appropriate row
-- is found when checking the conds
--
-- correct read command: statement 1 condition is on column a and list update
-- is on column b, while statement b condition is on column c and list
-- update is on column d
-- a composite read command is built and both rows are retreieved,
-- conditions are executed correctly and mutations are applied accordingly
--
--
-- Issue: #6273
-- Delete have priority above Insert.
create table lwt (key bigint, ck int, cv set<text>, PRIMARY KEY((key), ck));
insert into lwt (key, ck, cv) values (1, 0, {'a', 'b'}) if not exists;
begin batch
    delete from lwt where key=1 and ck=0 if exists
    insert into lwt (key, ck, cv) values (1, 0, {'b', 'c'})
apply batch;
select * from lwt;
drop table lwt;
--
-- Workaround for Issue #6273.
-- Delete individual cells instead of entire partition.
--
create table lwt (key bigint, ck int, cv set<text>, PRIMARY KEY((key), ck));
insert into lwt (key, ck, cv) values (1, 0, {'a', 'b'}) if not exists;
begin batch
    update lwt set cv=null where key=1 and ck=0 if exists
    insert into lwt (key, ck, cv) values (1, 0, {'b', 'c'})
apply batch;
select * from lwt;
drop table lwt;