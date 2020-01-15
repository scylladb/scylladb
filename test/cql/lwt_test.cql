create table t1 (a int primary key);
insert into t1 (a) values (1);
insert into t1 (a) values (2);
select * from t1 allow filtering;
drop table t1;
