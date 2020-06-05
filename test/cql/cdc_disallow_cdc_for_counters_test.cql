create table tb1 (pk int primary key, c1 counter) with cdc = {'enabled': true};

create table tb2 (pk int primary key, c1 counter);
alter table tb2 with cdc = {'enabled': true};

create table tb3 (pk int primary key) with cdc = {'enabled': true};
alter table tb3 add (c1 counter);