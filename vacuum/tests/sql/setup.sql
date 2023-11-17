drop table if exists test;
create table test (
    batch_id bigint,
    val int
) CLUSTER BY(batch_id);

truncate table system.metrics;

