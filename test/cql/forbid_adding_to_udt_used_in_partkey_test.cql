create type ut (a int);
create table t (pk frozen<ut> primary key);
alter type ut add b int;
drop table t;
alter type ut add c int;
create table t2 (pk frozen<list<frozen<ut>>> primary key);
alter type ut add d int;
