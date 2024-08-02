create or replace database vacuum2;
use vacuum2;
create or replace table test (
    batch_id bigint,
    val int
) CLUSTER BY(batch_id);