create table base (
    a int8 not null,
    b bigint not null,
    c varchar,
    d datetime not null 
) cluster by (a, b);
create table rand like base Engine = Random;
create table sink like base;

alter table base set options(change_tracking=true);

truncate table system.metrics;
