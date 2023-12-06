--
-- basic: ensure the grammar works
--
create table lwt (a int primary key, b int);
-- insert if not exists
insert into lwt (a, b) values (1, 1) if not exists;
select * from lwt allow filtering;
-- delete if exists
delete from lwt where a=1 if exists;
select * from lwt allow filtering;
-- update with condition
update lwt set b = 2 where a=1 if b = 1;
select * from lwt allow filtering;
-- update if exists
update lwt set b = 2 where a = 1 if exists;
select * from lwt allow filtering;
-- update if exists with a scan: proper error message
update lwt set b = 2 where b = 1 if exists;
select * from lwt allow filtering;
-- incorrect syntax: update if *not* exists
update lwt set b=3 where a=2 if not exists;
-- incorrect syntax: delete if *not* exists
delete from lwt where a=2 if not exists;
--
-- timestamp: using timestamp clause is not allowed
--
-- check that using timestamp without LWT clause works
insert into lwt (a, b) values (1, 1) using timestamp 1;
-- with LWT, it's not allowed
insert into lwt (a, b) values (1, 1) if not exists using timestamp 1;
--
--
-- update if not exists is not allowed (grammar error), it doesn't make sense
update lwt set b = 2 where a = 1 if not exists;
-- update with multiple partitions can not have conditions
-- Cassandra output: code=2200 [Invalid query] message="IN on the partition
-- key is not supported with conditional updates"
update lwt set b = 2 where a in (1, 2) if b = 1;

drop table lwt;
--
-- basic: ends
--
--
-- clustering: tests related to lwt and clustering keys
--
-- create a table with a partitioning key and a clustering key
create table lwt (a int, b int, c int, primary key (a,b));
-- update with multiple clustering keys can not have conditions
-- Cassandra output: code=2200 [Invalid query] message="IN on the clustering
-- key is not supported with conditional updates"
update lwt set c = 2 where a = 1 and b in (2,3)  if c = 1;
-- update with a clustering key - should work
update lwt set c = 2 where a = 1 and b = 2  if c = 1;
-- update with a single clustering key inside IN predicate
-- correctly identified as a single-key update
update lwt set c = 2 where a = 1 and b in (2) if c = 1;
-- unrestricted conditional update,
-- Casssandra: code=2200 [Invalid query]
-- message="Some clustering keys are missing: b"
update lwt set c=3 where a=1 if c=2;
--  comparing columns
update lwt set c=3 where a=1 if c=a;
-- cleanup
drop table lwt;
--
-- clustering: ends
--
-- limitations: check grammar/feature limitations of LWT
--
create table lwt (a int, b frozen<map<int, int>>, c int, primary key (a, b));
insert into lwt (a, b, c) values (1, {1:1, 2:2}, 3);
-- Strictcly speaking this is not an LWT check, but we need to ensure that
-- LWT restrictions are a superposition of modification statement
-- restrictions
update lwt set c=3 where a=1 and b contains 1 if c=1;

drop table lwt;

-- conditional updates and counters: not supported
create table lwt (a int primary key, b counter);
update lwt set  b = b+1 where a = 1;
update lwt set  b = b+1 where a = 1 if exists;
insert into lwt (a, b) values (1,1) if not exists;
delete from lwt where a=1 if exists;
delete from lwt where a=1 if b=2;
drop table lwt;
--
-- limitations: ends
--
---
-- if_exists: check if exists/if not exists works
---
create table lwt (a int primary key, b int);
-- insert if not exists
insert into lwt (a, b) values (1, 1) if not exists;
select * from lwt allow filtering;
-- should not update
insert into lwt (a, b) values (1, 2) if not exists;
select * from lwt allow filtering;
-- should update
insert into lwt (a, b) values (2, 2) if not exists;
select * from lwt allow filtering;
-- should delete
delete from lwt where a=2 if exists;
select * from lwt allow filtering;
-- should not update
update lwt set b=3 where a=2 if exists;
select * from lwt allow filtering;
-- should update
update lwt set b=3 where a=1 if exists;
select * from lwt allow filtering;
drop table lwt;
--
-- if_exists: ends end of check for if-exists/if-not-exists
--
-- conditional updates and static columns
-- see also https://issues.apache.org/jira/browse/CASSANDRA-10532
create table lwt (id bigint, uuid uuid, stc text static, value text, primary key(id, uuid));
delete stc FROM lwt WHERE id=1 IF stc='key';
drop table lwt;

create table lwt (p int, c int, s int static, v int, primary key(p, c));
insert into lwt(p, c, v) values(1, 1, 1);
select * from lwt;
-- must fail: regular row conditions do not match
insert into lwt(p, c, v) values(1, 1, 10) if not exists;
insert into lwt(p, c, s) values(1, 1, 10) if not exists;
update lwt set v=10 where p=1 and c=1 if v=2;
delete from lwt where p=1 and c=1 if v=2;
select * from lwt;
-- must succeed: regular row conditions match
insert into lwt(p, c, v) values(1, 2, 2) if not exists;
update lwt set v=10 where p=1 and c=1 if v=1;
update lwt set v=20 where p=1 and c=2 if exists;
select * from lwt;
-- must fail: there is no static row in the partition
update lwt set s=2 where p=1 if exists;
update lwt set s=2 where p=1 if s=1;
update lwt set v=2 where p=1 and c=1 if s=1;
delete s from lwt where p=1 if exists;
delete s from lwt where p=1 if s=1;
delete v from lwt where p=1 and c=1 if s=1;
delete s from lwt where p=1 and c=1 if s=1 and v=10;
delete v from lwt where p=1 and c=1 if s=1 and v=10;
delete from lwt where p=1 and c=1 if s=1;
delete from lwt where p=1 and c=1 if s=1 and v=10;
select * from lwt;
-- must succeed: there is no static row in the partition (even though there are regular rows)
insert into lwt(p, s) values(1, 1) if not exists;
select * from lwt;
-- must fail: static row conditions do not match
insert into lwt(p, s) values(1, 2) if not exists;
update lwt set s=3 where p=1 if s=2;
update lwt set v=3 where p=1 and c=1 if s=2;
delete s from lwt where p=1 if s=2;
delete v from lwt where p=1 and c=1 if s=2;
delete s from lwt where p=1 and c=1 if s=2 and v=10;
delete v from lwt where p=1 and c=1 if s=2 and v=10;
delete from lwt where p=1 and c=1 if s=2;
delete from lwt where p=1 and c=1 if s=2 and v=10;
-- must succeed: regular row conditions match
delete from lwt where p=1 and c=1 if exists;
delete from lwt where p=1 and c=2 if v=20;
select * from lwt;
-- must succeed: there is no regular rows in the partition (even though there is a static row)
insert into lwt(p, c, v) values(1, 1, 1) if not exists;
select * from lwt;
delete from lwt where p=1 and c=1;
select * from lwt;
-- must fail: there is no regular rows in the partition
update lwt set v=10 where p=1 and c=1 if exists;
update lwt set v=10 where p=1 and c=1 if s=1 and v=1;
update lwt set s=10 where p=1 and c=1 if s=1 and v=1;
delete s from lwt where p=1 and c=1 if s=1 and v=1;
delete v from lwt where p=1 and c=1 if s=1 and v=1;
delete from lwt where p=1 and c=1 if s=1 and v=1;
select * from lwt;
-- must succeed: the condition applies only to the static row
delete from lwt where p=1 and c=1 if s=1;
delete v from lwt where p=1 and c=1 if s=1;
update lwt set v=10 where p=1 and c=1 if s=1;
update lwt set s=10 where p=1 if s=1;
select * from lwt;
insert into lwt(p, c, s, v) values(2, 2, 20, 20);
insert into lwt(p, s) values(3, 30);
insert into lwt(p, s) values(4, 40);
select * from lwt;
-- must succeed: static row conditions match
delete s from lwt where p=1 if exists;
delete s from lwt where p=2 if s=20;
delete s from lwt where p=3 if exists;
delete s from lwt where p=4 if s=40;
select * from lwt;
insert into lwt(p, c, v) values(1, 1, 1);
insert into lwt(p, c, v, s) values(1, 2, 2, 1);
insert into lwt(p, c, v) values(2, 1, 1);
insert into lwt(p, c, v) values(2, 2, 2);
insert into lwt(p, c, v, s) values(2, 3, 3, 1);
-- must succeed: both static and regular row conditions match
delete v from lwt where p=1 and c=1 if v=1 and s=1;
delete s from lwt where p=1 and c=2 if v=2 and s=1;
delete from lwt where p=2 and c=3 if v=3 and s=1;
update lwt set v=2 where p=2 and c=1 if v=1 and s=1;
update lwt set s=2 where p=2 and c=2 if v=2 and s=1;
select * from lwt;
-- sanity checks
update lwt set v=1 where p=1 if exists;
update lwt set v=1 where p=1 if s=1;
update lwt set v=1 where p=1 if v=1;
update lwt set v=1 where p=1 if s=1 and v=1;
update lwt set s=1 where p=1 if v=1;
update lwt set s=1 where p=1 if v=1 and s=1;
update lwt set s=1 where p=1 and c=1 if s=1;
update lwt set s=1 where p=1 and c=1 if exists;
delete from lwt where p=1 if exists;
delete from lwt where p=1 if v=10;
delete from lwt where p=1 and c>0 if exists;
delete from lwt where p=1 and c>0 if v=10;
delete v from lwt where p=1 if exists;
delete v from lwt where p=1 if v=10;
delete v from lwt where p=1 and c>0 if exists;
delete v from lwt where p=1 and c>0 if v=10;
delete s from lwt where p=1 if v=1;
delete s from lwt where p=1 if v=1 and s=1;
delete s from lwt where p=1 and c=1 if s=1;
delete s from lwt where p=1 and c=1 if exists;
drop table lwt;

-- mixing reading operations and static column conditions in one statement
create table lwt(p int, c int, s int static, v list<int>, primary key(p, c));
insert into lwt(p, c) values(1, 1);
insert into lwt(p, c, v) values(1, 2, [1, 2, 3]);
insert into lwt(p, c, s) values(1, 3, 1);
update lwt set v=v-[2] where p=1 and c=2 if s=1;
select * from lwt;
drop table lwt;

-- todo: conditional updates and views

--
-- collections: conditional updates and sets, maps, lists, frozen
-- sets, maps and lists
--
create table lwt (a int, b int, setint set<int>, setsetint set<frozen<set<int>>>, primary key (a, b));
insert into test (a, b) values (1,1);
update lwt set setint={1, 2, 3}, setsetint = {{1,2,3}, {2,3,4}, {5,6,7}} where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set setint=setint+{1, 2, 3}, setsetint = setsetint + {{7,8,9}} where a = 1 and b = 1 if setint = {1, 2, 3};
select * from lwt allow filtering;
update lwt set setint=setint-{1, 2, 3}, setsetint = setsetint - {{1,2,3}} where a = 1 and b = 1  if exists;
select * from lwt allow filtering;
drop table lwt;

create table lwt (a int, b int, mapint map<int, int>, mapsetint map<int, frozen<set<int>>>, primary key (a, b));
insert into lwt (a, b) values (1,1);
update lwt set mapint={1: 1, 2: 1, 3: 1}, mapsetint = {1: {1}, 2: {2}, 3: {3}} where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set mapsetint = mapsetint + {1: {3,4}} where a = 1 and b = 1 if mapint[2] = 1;
select * from lwt allow filtering;
update lwt set mapint = mapint + {2: 1} where a = 1 and b = 1 if exists;
update lwt set mapint = mapint - {2} where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
drop table lwt;

create table lwt (a int, b int, listint list<int>, staticsettext set<text> static, primary key(a, b));
insert into lwt (a, b) values (1,1);
update lwt set staticsettext = {'a', 'b', 'c'} where a = 1 if exists;
select * from lwt allow filtering;
update lwt set staticsettext = {'d', 'e', 'f'} where a = 2 if exists;
select * from lwt allow filtering;
update lwt set staticsettext = staticsettext + {'d'}, listint = [1, 2, 3] where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set staticsettext = staticsettext + {'e'} where a = 1 if exists;
select * from lwt allow filtering;
update lwt set listint = listint + [5] where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set listint = [-1] + listint, staticsettext = staticsettext - {'e'} where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
drop table lwt;

create table lwt (a int, b int, flistint frozen<list<int>>, smapint map<text, int> static, primary key(a, b));
insert into lwt (a, b) values (1,1);
update lwt set smapint = {'a': 1, 'b': 1, 'c': 3} where a = 1 if exists;
select * from lwt allow filtering;
update lwt set smapint = smapint + {'d': 3}, flistint = [1] where a = 1 if exists;
update lwt set smapint = smapint + {'d': 4}, flistint = [1] where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set flistint = [1, 2] where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set flistint = flistint + [3] where a = 1 and b = 1 if exists;
select * from lwt allow filtering;
update lwt set flistint = [4]  where a = 1 and b = 1 if smapint['a'] = 1;
update lwt set flistint = [4]  where a = 1 and b = 1;
drop table lwt;
--
-- collection: ends
--
--
-- if expr grammar
--
create table lwt (a int, c int, listint list<int>, mapint map<int, int>, setint set<int>, primary key(a));
-- term
update lwt set c=0 where a = 1 if c;
-- -- equality
update lwt set c=0 where a = 1 if c = 1;
-- inequality: there is na inequality
update lwt set c=0 where a = 1 if c != 1;
-- greater
update lwt set c=0 where a = 1 if c > 1;
-- less
update lwt set c=0 where a = 1 if c < 1;
-- greater equal
update lwt set c=0 where a = 1 if c >= 1;
-- less equal
update lwt set c=0 where a = 1 if c <= 1;
-- AND prediate
update lwt set c=0 where a = 1 if c = 1 and c = 1;
-- OR predicate: Cassandra users do not deserve it :(
update lwt set c=0 where a = 1 if c = 1 or c = 1;
-- null
update lwt set c=0 where a = 1 if c = null;
-- in
update lwt set c=0 where a = 1 if c in (1);
-- in
update lwt set c=0 where a = 1 if c in (1, 2);
-- in (null)
update lwt set c=0 where a = 1 if c in (1, null);
-- list contains
update lwt set c=0 where a = 1 if listint contains null;
-- map contains
update lwt set c=0 where a = 1 if mapint contains key 1;
-- set contains
update lwt set c=0 where a = 1 if setint contains key 1;
-- multi-value set contaias
update lwt set c=0 where a = 1 if setint contains key 1;
-- addressing map element
update lwt set c=0 where a = 1 if mapint[2] = 3;
-- addressing map element
update lwt set c=0 where a = 1 if mapint[2];
-- comparing map element and null
update lwt set c=0 where a = 1 if mapint[2] = null;
-- rinse and repeat with actual data
insert into lwt (a, c, listint, mapint, setint) values (1, 1, [1], {1: 1}, {1});
update lwt set c=1 where a = 1 if c = 0;
select * from lwt allow filtering;
update lwt set c=2 where a = 1 if c != 1;
select * from lwt allow filtering;
update lwt set c=3 where a = 1 if c > 0;
select * from lwt allow filtering;
update lwt set c=4 where a = 1 if c < 2;
select * from lwt allow filtering;
update lwt set c=5 where a = 1 if c >= 1;
select * from lwt allow filtering;
update lwt set c=6 where a = 1 if c <= 5;
select * from lwt allow filtering;
update lwt set c=7 where a = 1 if c = 1 and c = 1;
select * from lwt allow filtering;
update lwt set c=8 where a = 1 if c = null;
select * from lwt allow filtering;
update lwt set c=9 where a = 1 if c in (4);
select * from lwt allow filtering;
update lwt set c=10 where a = 1 if c in (4, 5);
select * from lwt allow filtering;
update lwt set c=11 where a = 1 if c in (5, null);
select * from lwt allow filtering;
update lwt set c=12 where a = 1 if mapint[1] = 1;
select * from lwt allow filtering;
update lwt set c=13 where a = 1 if mapint[1] = null;
select * from lwt allow filtering;
drop table lwt;
--
-- conditions: null handling
--
create table lwt (a int, b int, primary key(a));
insert into lwt (a) values (1);
-- null >|<|<=|<= val = false
update lwt set b = 1 where a = 1 if b > 3;
update lwt set b = 1 where a = 1 if b >= 3;
update lwt set b = 1 where a = 1 if b <= 3;
update lwt set b = 1 where a = 1 if b < 3;
-- null >|<|<=|<= null = error
update lwt set b = 1 where a = 1 if b > null;
update lwt set b = 1 where a = 1 if b >= null;
update lwt set b = 1 where a = 1 if b < null;
update lwt set b = 1 where a = 1 if b <= null;
-- cassandra doesn't allow this (ugh), but test nevertheless
update lwt set b = 1 where a = 1 if 3 < b;
update lwt set b = 1 where a = 1 if 3 <= b;
update lwt set b = 1 where a = 1 if 3 >= b;
update lwt set b = 1 where a = 1 if 3 > b;
update lwt set b = 1 where a = 1 if null < b;
update lwt set b = 1 where a = 1 if null <= b;
update lwt set b = 1 where a = 1 if null > b;
update lwt set b = 1 where a = 1 if null >= b;
update lwt set b = 1 where a = 1 if null = b;
-- null == null = true
update lwt set b = 1 where a = 1 if b = null;
select * from lwt allow filtering;
-- val !=  null = true
update lwt set b = null where a = 1 if b != null;
select * from lwt allow filtering;
update lwt set b = null where a = 1;
select * from lwt allow filtering;
-- null != null  = false
update lwt set b = 1 where a = 1 if b != null;
select * from lwt allow filtering;
-- null != val  = true
update lwt set b = 2 where a = 1 if b != 1;
select * from lwt allow filtering;
drop table lwt;
--
-- conditions: simple expression
--
create table lwt (a int, b boolean, d decimal, f double, i int, s text, t timestamp, primary key(a));
insert into lwt (a, b, d, f, i, s, t) values (1, true, 1, 1, 1, '', dateof(now()));
-- boolean test
update lwt set b = false where a = 1 if b = true;
select b from lwt where a = 1;
update lwt set b = true where a = 1 if b = true;
select b from lwt where a = 1;
update lwt set b = true where a = 1 if b != true;
-- for whatever reason these operators are allowed for boolean values,
-- keep it this way
update lwt set b = true where a = 1 if b > true;
update lwt set b = true where a = 1 if b < false;
update lwt set b = true where a = 1 if b <= false;
update lwt set b = true where a = 1 if b >= true;
-- integer test
update lwt set i = 1 where a = 1 if i = 1;
update lwt set i = 2 where a = 1 if i != 1;
update lwt set i = 3 where a = 1 if i < 1;
update lwt set i = 4 where a = 1 if i > 1;
update lwt set i = 5 where a = 1 if i >= 1;
update lwt set i = 6 where a = 1 if i <= 1;
-- Compare with another column: cassandra doesn't allow anything but a
-- constant on the right hand side :(
update lwt set i = 7 where a = 1 if i = t;
update lwt set i = 7 where a = 1 if i = d;
update lwt set i = 7 where a = 1 if i = f;
update lwt set i = 7 where a = 1 if s = i;
-- float test
update lwt set f = 2 where a = 1 if f = 1;
update lwt set f = 3 where a = 1 if f != 1;
update lwt set f = 4 where a = 1 if f < 1;
update lwt set f = 5 where a = 1 if f > 1;
update lwt set f = 6 where a = 1 if f >= 1;
update lwt set f = 7 where a = 1 if f <= 1;
-- this is broken, obviously
update lwt set f = 8 where a = 1 if f != 6.0000000000000001;
-- decimal
update lwt set d = 2.1 where a = 1 if d = 1;
update lwt set d = 3.2 where a = 1 if d != 2.09999999999999999999999999999999999;
update lwt set d = 4.3 where a = 1 if d < 3.2;
update lwt set d = 5.4 where a = 1 if d > 3.2;
update lwt set d = 6.5 where a = 1 if d >= 3.2;
update lwt set d = 7.6 where a = 1 if d <= 6.5;
-- text
update lwt set s = 'i' where a = 1 if s = '';
-- update lwt set s = 'щ' where a = 1 if s != 'j';
-- wheeled charriot and left-to-right
-- update lwt set s = ' 𐃌   ' where a = 1 if s < 'я';
-- update lwt set s = 'שׁלום' where a = 1 if s >= '1';
select s from lwt where a = 1;
-- timestamp test
-- update lwt set t = dateof(now()) where a = 1 if t <= dateof(now());
drop table lwt;
-- map keys
create table lwt (a int, b map<boolean, boolean>, c list<boolean>, s set<boolean>, primary key (a));
insert into lwt (a, b) values (1, {true: true});
update lwt set b = {true:false} where a = 1 if b[true] = true;
update lwt set b = {true:false} where a = 1 if b[true] = null;
update lwt set b = {false:true} where a = 1 if b[false] = null;
update lwt set b = {false:false} where a = 1 if b[true] != null;
update lwt set b[true] = false where a = 1 if b[true] != 1;
update lwt set b[false] = null where a = 1 if b[true] > null;
update lwt set b[false] = true where a = 1 if b[1] != true;
update lwt set b = {true:false, true:true, false:false} where a = 1 if b[null] = null;
update lwt set b = {false:false} where a = 1 if b[null] = null;
update lwt set b = b + {false:true} where a = 1 if b[null] = true;
update lwt set b[null] = false where a = 1 if b[true] != true;
update lwt set b[null] = false where a = 1 if b[true] = true;
select b from lwt where a = 1;
update lwt set b = {false:true} where a = 1 if b = {true:false};
update lwt set b = {} where a = 1 if exists;
update lwt set b = {} where a = 1 if b = {};
select b from lwt where a = 1;
update lwt set b = {} where a = 1 if b = null;
select b from lwt where a = 1;
update lwt set b = null where a = 1 if b = {};
select b from lwt where a = 1;
-- lists
-- null value == empty list. see CASSANDRA-7155
update lwt set c = [false] where a = 1 if c = [];
-- c is null, really, so this should work
update lwt set c = [] where a = 1 if c = null;
-- check with empty list
update lwt set c = [false] where a = 1 if c = [];
update lwt set c = [false] where a = 1 if c = [false];
update lwt set c = [false] where a = 1 if c = [null];
update lwt set c = [false] where a = 1 if c = [null, true];
select c from lwt where a = 1;
update lwt set c = [true, false] where a = 1 if c[0] = false;
update lwt set c = c + [true] where a = 1 if c[1] = false;
update lwt set c = c + [false] where a = 1 if c[2] = null;
update lwt set c = c + [true] where a = 1 if c[3] = null;
update lwt set c = c + [true] where a = 1 if c[100000000] = null;
update lwt set c = c + [false] where a = 1 if c[100000000] = true;
update lwt set c = c + [true, false] where a = 1 if c[null] = false;
update lwt set c = [false] where a = 1 if c[null] = null;
update lwt set c = [true, false, true, true, true] where a = 1 if c[null] = null;
update lwt set c = c + [false] where a = 1 if c[null] = 1;
update lwt set c = [true] + c where a = 1 if c = [false];
select c from lwt where a = 1;
-- sets
-- null value == empty set
update lwt set s = {} where a = 1 if s = {};
update lwt set s = {} where a = 1 if s = null;
update lwt set s = {false} where a = 1 if s = null;
update lwt set s = {true, false} where a = 1 if s = {false};
update lwt set s = {false} where a = 1 if s = {null};
update lwt set s = {false} where a = 1 if s = {null, true};
update lwt set s = {true, false} where a = 1 if s{true} = false;
update lwt set s = {false} where a = 1 if s = 1;
update lwt set s = {true} + s where a = 1 if s = {false};
-- sets
-- null value == empty set
update lwt set s = {} where a = 1 if s = {};
update lwt set s = {} where a = 1 if s = null;
update lwt set s = {false} where a = 1 if s = null;
update lwt set s = {true, false} where a = 1 if s = {false};
update lwt set s = {false} where a = 1 if s = {null};
update lwt set s = {false} where a = 1 if s = {null, true};
update lwt set s = {true, false} where a = 1 if s{true} = false;
update lwt set s = {false} where a = 1 if s = 1;
update lwt set s = {true} + s where a = 1 if s = {false};
drop table lwt;
--
-- frozen collections
--
create table lwt (a int, b frozen<map<boolean, boolean>>, c frozen<list<boolean>>, s frozen<set<boolean>>, primary key (a));
insert into lwt (a, b) values (1, {true: true});
update lwt set b = {true:false} where a = 1 if b[true] = true;
update lwt set b = {true:false} where a = 1 if b[true] = null;
update lwt set b = {false:true} where a = 1 if b[false] = null;
update lwt set b = {false:false} where a = 1 if b[true] != null;
update lwt set b[true] = false where a = 1 if b[true] != true;
update lwt set b = b + {false:true} where a = 1 if b[true] = true;
select b from lwt where a = 1;
update lwt set b = {false:true} where a = 1 if b = {true:false};
update lwt set b = {} where a = 1 if exists;
update lwt set b = {} where a = 1 if b = {};
select b from lwt where a = 1;
update lwt set b = {} where a = 1 if b = null;
select b from lwt where a = 1;
update lwt set b = null where a = 1 if b = {};
select b from lwt where a = 1;
-- lists
-- null value != empty list for a frozen list
update lwt set c = [false] where a = 1 if c = [];
-- c is null, really, so this should work
update lwt set c = [] where a = 1 if c = null;
-- check with empty list
update lwt set c = [false] where a = 1 if c = [];
update lwt set c = [false] where a = 1 if c = [false];
update lwt set c = [false] where a = 1 if c = [null];
update lwt set c = [false] where a = 1 if c = [null, true];
-- lists with NULL are allowed, but cannot be stored
update lwt set c = [false, null] where a = 1 if c = [false];
select c from lwt where a = 1;
-- negative list index
update lwt set c = [true, false] where a = 1 if c[-1] = false;
update lwt set c = [true, false] where a = 1 if c[-1.5] = false;
update lwt set c = [true, false] where a = 1 if c[0] = false;
update lwt set c = [true] where a = 1 if c[1] = false;
update lwt set c = [true, false] where a = 1 if c[2] = null;
update lwt set c = [true] where a = 1 if c[100000000] = null;
update lwt set c = [false] where a = 1 if c[null] = null;
update lwt set c = [true] where a = 1 if c[null] = null;
update lwt set c = [true] + c where a = 1 if c = [false];
update lwt set c = [true,true,true,false,false,false] where a = 1 if exists;
update lwt set c = null where a = 1 if c[1] = true and c[5] = false and c[7] = null;
select c from lwt where a = 1;
-- sets
-- null value == empty set
update lwt set s = {} where a = 1 if s = {};
update lwt set s = {} where a = 1 if s = null;
update lwt set s = {false} where a = 1 if s = null;
update lwt set s = {true, false} where a = 1 if s = {false};
update lwt set s = {false} where a = 1 if s = {null};
update lwt set s = {false} where a = 1 if s = {null, true};
update lwt set s = {true, false} where a = 1 if s{true} = false;
update lwt set s = s + {true} where a = 1 if s = {true, false};
update lwt set s = {true, true} where a = 1 if exists;
-- check non frozen sets are compared correctly to set literals
update lwt set s = {false, false} where a = 1 if  s = {true, true};
update lwt set s = {false, true} where a = 1 if  s = {true, false};
select s from lwt where a = 1;
drop table lwt;
-- gt/le on composite collections
create table lwt (a int, b map<int, int>, c list<int>, s set<int>, primary key (a));
insert into lwt (a, b, c, s) values (1, {}, [], {});
-- maps
update lwt set b = {1:1} where a = 1 if b > {};
update lwt set b = {1:1} where a = 1 if b < {};
update lwt set b = {1:1} where a = 1 if b = {};
update lwt set b = {2:2} where a = 1 if b > {1:1};
update lwt set b = {2:2} where a = 1 if b >= {1:1};
update lwt set b = {3:3} where a = 1 if b > {1:1};
update lwt set b = {4:4} where a = 1 if b >= {2:3};
update lwt set b = {4:4} where a = 1 if b >= {3:3};
update lwt set b = {1:1} where a = 1 if b < {4:4};
update lwt set b = {1:1} where a = 1 if b <= {4:4};
select b from lwt where a= 1;
-- lists
update lwt set c = [1,1] where a = 1 if c > [];
update lwt set c = [1,1] where a = 1 if c < [];
update lwt set c = [1,1] where a = 1 if c = [];
update lwt set c = [2,2] where a = 1 if c > [1,1];
update lwt set c = [2,2] where a = 1 if c >= [1,1];
update lwt set c = [3,3] where a = 1 if c > [1,1];
update lwt set c = [4,4] where a = 1 if c >= [2,3];
update lwt set c = [4,4] where a = 1 if c >= [3,3];
update lwt set c = [1,1] where a = 1 if c < [4,4];
update lwt set c = [1,1] where a = 1 if c <= [4,4];
select c from lwt where a= 1;
-- sets
update lwt set s = {1} where a = 1 if s > {};
update lwt set s = {1} where a = 1 if s < {};
update lwt set s = {1} where a = 1 if s = {};
update lwt set s = {2} where a = 1 if s > {1};
update lwt set s = {2} where a = 1 if s >= {1};
update lwt set s = {3} where a = 1 if s > {1};
update lwt set s = {4} where a = 1 if s >= {3};
update lwt set s = {1} where a = 1 if s < {4};
update lwt set s = {1} where a = 1 if s <= {4};
select s from lwt where a= 1;
drop table lwt;
-- collections of collections
-- non-frozen nested sets are not supported
create table lwt (a int, b set<map<int,int>>, c list<set<int>>, primary key (a));
-- frozen collection elements are however ok
create table lwt (a int, b set<frozen<list<int>>>, c list<frozen<set<int>>>, primary key (a));
insert into lwt (a, b, c) values (1, {[1,2], [1,2]}, [{1,2}, {1,2}]);
-- sets
update lwt set b={[3,3], [4,4]} where a = 1 if b = {[1,2], [1,2]};
update lwt set b={[5,5,5], [4,4,4]} where a = 1 if b > {[3,3], [4,4]};
update lwt set b={[5,5,5], [4,4,4]} where a = 1 if b >= {[3,3], [4,4]};
select b from lwt where a = 1;
-- lists
update lwt set c=[{3,4}, {4,5}] where a = 1 if c = [{1,2}, {1,2}];
update lwt set c=[{3,4,5}, {4,5,6}] where a = 1 if c > [{3,3}, {4,4}];
update lwt set c=[{5,6,7}, {7,8,9}] where a = 1 if c >= [{3,3}, {5,4}];
update lwt set c=[{5,6,7}, {7,8,9}] where a = 1 if c >= [{3,4}, {4,5}];
select c from lwt where a = 1;
drop table lwt;
-- in predicate, static columns
create table lwt (a int, b int, c list<int>, d set<int> static, primary key (a, b));
insert into lwt (a, b, c, d) values (1,1,[1],{1});
update lwt set d = {2,3,4,5,-1} where a = 1 if d in ({1});
update lwt set d = {2} where a = 1 if d in ({2,3,4,5});
update lwt set d = {2} where a = 1 if d in ({2,3,4,5,-1});
update lwt set c = [2,3,4,5,-1] where a = 1 and b = 1 if c in ([1]);
update lwt set c = [2] where a = 1 and b = 1 if c in ([2,3,4,5]);
-- check null handling in IN condition
update lwt set c = [] where a = 1 and b = 1 if c in ([2,3,4,5,-1]);
update lwt set c = null where a = 1 and b = 1 if c in ([]);
update lwt set c = null where a = 1 and b = 1 if c in ([]);
update lwt set c = null where a = 1 and b = 1 if c in (null);
update lwt set c = [1] where a = 1 and b = 1 if c in ();
update lwt set c = [2] where a = 1 and b = 1 if c in ([1], null, [2]);
delete from lwt where a = 1;
update lwt set d = {2} where a = 1 if d in ();
update lwt set d = {2} where a = 1 if d in (null);
update lwt set d = {} where a = 1 if d in ({}, {2});
update lwt set d = {} where a = 1 if d in (null, {2});
update lwt set d = null where a = 1 if d in (null, {2});
update lwt set d = null where a = 1 if d in (null, {2}, {3,4}, {5});
drop table lwt;

-- in predicate, frozen sets/lists columns
create table lwt (a int, b int, c frozen<list<int>>, d frozen<set<int>> static, primary key (a, b));
insert into lwt (a, b, c, d) values (1,1,[1],{1});
update lwt set d = {2,3,4,5,-1} where a = 1 if d in ({1});
update lwt set d = {2} where a = 1 if d in ({2,3,4,5});
update lwt set d = {2} where a = 1 if d in ({2,3,4,5,-1});
update lwt set c = [2,3,4,5,-1] where a = 1 and b = 1 if c in ([1]);
update lwt set c = [2] where a = 1 and b = 1 if c in ([2,3,4,5]);
-- check null handling in IN condition
update lwt set c = [] where a = 1 and b = 1 if c in ([2,3,4,5,-1]);
update lwt set c = null where a = 1 and b = 1 if c in ([]);
update lwt set c = null where a = 1 and b = 1 if c in ([]);
update lwt set c = null where a = 1 and b = 1 if c in (null);
update lwt set c = [1] where a = 1 and b = 1 if c in ();
update lwt set c = [2] where a = 1 and b = 1 if c in ([1], null, [2]);
delete from lwt where a = 1;
update lwt set d = {2} where a = 1 if d in ();
update lwt set d = {2} where a = 1 if d in (null);
update lwt set d = {} where a = 1 if d in ({}, {2});
update lwt set d = {} where a = 1 if d in (null, {2});
update lwt set d = null where a = 1 if d in (null, {2});
update lwt set d = null where a = 1 if d in (null, {2}, {3,4}, {5});
drop table lwt;
-- LWT and slices; a slice is a Cassandra term for a clustering key range,
-- usually within the same partition key
create table lwt (a int, b int, c int, d int, primary key (a, b, c));
insert into lwt (a, b, c, d) values (1,1,1,1);
insert into lwt (a, b, c, d) values (1,1,2,0);
insert into lwt (a, b, c, d) values (1,1,3,1);
insert into lwt (a, b, c, d) values (1,2,1,1);
insert into lwt (a, b, c, d) values (1,2,2,0);
insert into lwt (a, b, c, d) values (1,2,3,1);
insert into lwt (a, b, c, d) values (1,3,1,1);
insert into lwt (a, b, c, d) values (1,3,2,0);
insert into lwt (a, b, c, d) values (1,3,3,1);
-- update
select d from lwt    where a = 1 and b = 1;
update lwt set d = 7 where a = 1 and b = 1 if d = 0;
select d from lwt    where a = 1 and b = 1;
select d from lwt    where a = 1 and b = 2;
update lwt set d = 7 where a = 1 and b = 2;
-- delete
select a, b, c, d from lwt where a = 1 and b = 1;
delete from lwt            where a = 1 and b = 1 if d = 0;
select a, b, c, d from lwt where a = 1 and b = 1;
select a, b, c, d from lwt where a = 1 and b = 2;
delete from lwt            where a = 1 and b = 2 if d = 1;
select a, b, c, d from lwt where a = 1 and b = 2;
select a, b, c, d from lwt where a = 1 and b = 3;
delete from lwt            where a = 1 and b = 3;
select a, b, c, d from lwt where a = 1 and b = 3;
drop table lwt;
-- ok to delete if d is static
create table lwt (a int, b int, c int, d int static, primary key (a, b, c));
insert into lwt (a, b, c, d) values (1,1,1,1);
insert into lwt (a, b, c, d) values (1,1,2,1);
insert into lwt (a, b, c, d) values (1,1,3,1);
select a, b, c, d from lwt where a = 1 and b = 1;
-- try to delete all - fail
delete from lwt            where a = 1 and b = 1 if d = 0;
-- ok to delete static row
delete d from lwt          where a = 1 if d = 0;
select a, b, c, d from lwt where a = 1;
delete d from lwt          where a = 1 if d = 1;
select a, b, c, d from lwt where a = 1;
drop table lwt;
--
-- lists, sets, maps as partition or primary keys
--
-- This is covered with tests since with collections_as_maps
-- flags, sets/ints in primary key will make it difficult
-- to find the matching cells
--
-- invalid collection type
create table lwt (a int, b int, c set<int>, primary key (a, b, c));
-- ok - frozen collections are allowed
create table lwt (a int, b int, c frozen<list<int>>, primary key (a, b, c));
drop table lwt;
-- invalid collection type
create table lwt (a list<int>, b int, c int, primary key (a, b, c));
--
-- test empty primary key range: it is impossible to construct due to
-- limitations of the query language
--
create table lwt (a int, b int, c int, primary key (a, b));
-- error: can't use token and non-token restrictions
update lwt set c = 1 where a = 1 and b = 1 and token(a) > 0 and token(a) < 0 if c = 1;
-- error: can't use eq and non-eq restriction
update lwt set c = 1 where a = 1 and b = 1 and a < 0 if c = 1;
update lwt set c = 1 where a = 1 and b = 1 and a < 0 if c = 1;
-- error: partition key must be fully restricted
update lwt set c = 1 where a > 0 and a < 0 and b = 1 if c = 1;
-- error: partition key and IN is not supported
update lwt set c = 1 where a in () and b = 1 if c = 1;
update lwt set c = 1 where a = 1 and b IN (1, 2) if c = 1;
update lwt set c = 1 where a = 1 and (b) IN ((1), (2)) if c = 1;
drop table lwt;

-- gh-10081 Conditional UPDATE fails to apply when IF includes both static
-- and non-static columns.
create table t (p int, c int, r int, s int static, primary key(p, c));
insert into t (p, s) values (1, 1);
update t set s=2, r=1 where p=1 and c=1 if s=1 and r=null;
-- in case of LWT batch, each statement in the batch should
-- use correct regular or static row to evaluate the condition.
-- Specifically, the following batch must apply because
-- condition of statement 1 is true for regular row (1,1),
-- and condition of statements 2 and 3 are true for regular rows (1,2)
-- (1,3), which do not exist in the table, but are incarnated
-- to check the condition
begin batch
    update t set s=3, r=10 where p=1 and c=1 if s=2 and r=1
    update t set s=4, r=2 where p=1 and c=2 if s=2 and r=null
    update t set s=5, r=3 where p=1 and c=3 if s=2 and r=null
apply batch;
-- test select behaviour for static rows
-- the table contains 3 rows, right?
select count(*) from t;
-- count(*)  returns 3 rows. So let's delete 3 rows
delete from t where p=1 and c = 1 if exists;
delete from t where p=1 and c = 2 if exists;
delete from t where p=1 and c = 3 if exists;
-- WAT, count(*) returns 1 row even though
-- there were 3 rows and we deleted 3 rows!
select count(*) from t;
-- there is no DELETE statement syntax to
-- delete a static row; yep, that's right,
-- the row is there but you can't DELETE it!
delete from t where p=1 if exists;
-- let's kill the sneaky static row now
update t set s = null where p=1 if exists;
-- no more sneaky static rows.
select count(*) from t;
-- let's add a row back!
insert into t (p, s) values (1,1) if not exists;
-- that's right, there is a row now; what row though?
select count(*) from t;
-- let's add more rows
insert into t (p, c, s) values (1,1,1) if not exists;
-- WAT, we did not add more rows?
select count(*) from t;
-- WAT, you can't add a static row by inserting a partition
-- key alone, but you can add a clustering row by inserting
-- clustering key alone
insert into t (p, c) values (1,2) if not exists;
select count(*) from t;
-- stop this travesty
drop table t;

