drop table if exists test_order;
drop table if exists random_source;
create table test_order (
                            id bigint,
                            id1 bigint,
                            id2 bigint,
                            id3 bigint,
                            id4 bigint,
                            id5 bigint,
                            id6 bigint,
                            id7 bigint,

                            s1 varchar,
                            s2 varchar,
                            s3 varchar,
                            s4 varchar,
                            s5 varchar,
                            s6 varchar,
                            s7 varchar,
                            s8 varchar,
                            s9 varchar,
                            s10 varchar,
                            s11 varchar,
                            s12 varchar,
                            s13 varchar,

                            d1 DECIMAL(20, 8),
                            d2 DECIMAL(20, 8),
                            d3 DECIMAL(20, 8),
                            d4 DECIMAL(20, 8),
                            d5 DECIMAL(20, 8),
                            d6 DECIMAL(30, 8),
                            d7 DECIMAL(30, 8),
                            d8 DECIMAL(30, 8),
                            d9 DECIMAL(30, 8),
                            d10 DECIMAL(30, 8),

                            insert_time datetime,
                            insert_time1 datetime,
                            insert_time2 datetime,
                            insert_time3 datetime,

                            i int

) CLUSTER BY(to_yyyymmdd(insert_time), id) bloom_index_columns='insert_time,id';


create table random_source  like test_order Engine = Random;

truncate table system.metrics;

