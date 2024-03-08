CREATE TASK IF NOT EXISTS merge_task
    WAREHOUSE = 'task-test'
    SCHEDULE = 1 SECOND
    WHEN STREAM_STATUS('json_table_stream') = TRUE
AS BEGIN   
        BEGIN;
        MERGE INTO json_table_flag_1 AS target
        USING (SELECT * FROM json_table_stream) source
        ON source.v:source_flag = 1 AND target.id = source.v:id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.v:id,target.create_at = source.v:create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.v:id,source.v:create_at)
    ;

        MERGE INTO json_table_flag_2 AS target
            USING (SELECT * FROM json_table_stream) source
        ON source.v:source_flag = 2 AND target.id = source.v:id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.v:id,target.create_at = source.v:create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.v:id,source.v:create_at)
    ;

        MERGE INTO json_table_flag_3 AS target
        USING (SELECT * FROM json_table_stream) source
        ON source.v:source_flag = 3 AND target.id = source.v:id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.v:id,target.create_at = source.v:create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.v:id,source.v:create_at)
    ;

        MERGE INTO json_table_flag_4 AS target
        USING (SELECT * FROM json_table_stream) source
        ON source.v:source_flag = 4 AND target.id = source.v:id
        WHEN MATCHED THEN 
                UPDATE SET target.id = source.v:id,target.create_at = source.v:create_at
        WHEN NOT MATCHED THEN
                INSERT (id,create_at) VALUES (source.v:id,source.v:create_at)
    ;
        COMMIT
    ;
END;