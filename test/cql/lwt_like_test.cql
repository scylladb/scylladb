create table t (pk int primary key, c text);
insert into  t (pk, c) values (1, 'abc');
insert into  t (pk, c) values (2, 'bcd');
insert into  t (pk, c) values (3, 'cde');
-- match
update t set c = 'chg' where pk = 1 if c like 'a%';
update t set c = 'chg' where pk = 2 if c like 'b%';
update t set c = 'chg' where pk = 3 if c like 'c%';
-- null value
insert into  t (pk, c) values (3, null);
update t set c = 'error' where pk = 3 if c like 'a%';
-- unset value
insert into  t json '{ "pk": 4 }' default unset;
update t set c = 'err' where pk = 4 if c like 'a%';
-- empty pattern
update t set c = 'err' where pk = 1 if c like '';
-- invalid pattern type
update t set c = 'err' where pk = 1 if c like 1;
update t set c = 'err' where pk = 1 if c like null;
update t set c = 'err' where pk = 1 if c like bigintAsBlob(1);
-- int column
create table ti (pk int primary key, c int);
insert into  ti (pk, c) values (1, 1);
update ti set c = 2 where pk = 1 if c like 'a%';
-- map column
create table tm (pk int primary key, m map<int, text>);
insert into  tm (pk, m) values (1, { 1: 'abc' });
update tm set m = { 2: 'error' } where pk = 1 if m like 'a%';
-- blob column
create table tb (pk int primary key, b blob);
insert into  tb (pk, b) values (1, bigintAsBlob(1));
update tb set b = bigintAsBlob(2) where pk = 1 if b like 'a%';
