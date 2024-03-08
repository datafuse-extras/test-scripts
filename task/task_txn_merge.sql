CREATE TASK IF NOT EXISTS merge_task WAREHOUSE = 'task-test' SCHEDULE = 1 SECOND
WHEN STREAM_STATUS('json_table_stream') = TRUE AS BEGIN BEGIN;

MERGE INTO json_table_flag_1 AS target USING (
        SELECT
                v :event [0] :attr1 :a1 as id,
                v :create_at as create_at,
                v
        FROM
                json_table_stream
        where
                v :source_flag = 'source1'
) source ON target.id = source.id
WHEN MATCHED THEN
UPDATE
        *
        WHEN NOT MATCHED THEN
INSERT
        *;

MERGE INTO json_table_flag_2 AS target USING (
        SELECT
                v :event [0] :attr1 :a1 as id,
                v :create_at as create_at,
                v
        FROM
                json_table_stream
        where
                v :source_flag = 'source2'
) source ON target.id = source.id
WHEN MATCHED THEN
UPDATE
        *
        WHEN NOT MATCHED THEN
INSERT
        *;

MERGE INTO json_table_flag_3 AS target USING (
        SELECT
                v :event [0] :attr1 :a1 as id,
                v :create_at as create_at,
                v
        FROM
                json_table_stream
        where
                v :source_flag = 'source3'
) source ON target.id = source.id
WHEN MATCHED THEN
UPDATE
        *
        WHEN NOT MATCHED THEN
INSERT
        *;

MERGE INTO json_table_flag_4 AS target USING (
        SELECT
                v :event [0] :attr1 :a1 as id,
                v :create_at as create_at,
                v
        FROM
                json_table_stream
        where
                v :source_flag = 'source4'
) source ON target.id = source.id
WHEN MATCHED THEN
UPDATE
        *
        WHEN NOT MATCHED THEN
INSERT
        *;

COMMIT;

END;