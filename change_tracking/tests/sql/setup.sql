drop database if exists test_stream;
create database test_stream;

use test_stream;

create table base (
    c int32 not null
);
create table rand like base Engine = Random;
create table sink like base;

alter table base set options(change_tracking=true);

truncate table system.metrics;

