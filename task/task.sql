CREATE TASK IF NOT EXISTS merge_task
    WAREHOUSE = 'task-test'
    SCHEDULE = 1 SECOND
    WHEN STREAM_STATUS('json_table_stream') = TRUE
AS BEGIN   
        BEGIN;
        MERGE INTO json_table_flag_1 AS target
        USING (SELECT * FROM json_table_flag_1_stream) source
        ON source. source_flag = 1 AND target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.id,target.create_at = source.create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.id,source.create_at)
    ;

        MERGE INTO json_table_flag_2 AS target
            USING (SELECT * FROM json_table_flag_2_stream) source
        ON source. source_flag = 2 AND target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.id,target.create_at = source.create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.id,source.create_at)
    ;

        MERGE INTO json_table_flag_3 AS target
        USING (SELECT * FROM json_table_flag_3_stream) source
        ON source. source_flag = 3 AND target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.id,target.create_at = source.create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.id,source.create_at)
    ;

        MERGE INTO json_table_flag_4 AS target
        USING (SELECT * FROM json_table_flag_4_stream) source
        ON source. source_flag = 4 AND target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.id,target.create_at = source.create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.id,source.create_at)
    ;
        COMMIT
    ;
END;