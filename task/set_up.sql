CREATE
OR REPLACE TABLE default.json_table(create_at TIMESTAMP, v VARIANT);

CREATE
OR REPLACE STREAM json_table_stream ON TABLE default.json_table APPEND_ONLY = true;

COPY INTO json_table
FROM
    @s_temp PATTERN = '.*[.]csv' FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE
OR REPLACE TABLE default.json_table_flag_1(
    id BIGINT,
    create_at TIMESTAMP,
    v VARIANT
);

CREATE
OR REPLACE TABLE default.json_table_flag_2(
    id BIGINT,
    create_at TIMESTAMP,
    v VARIANT
);

CREATE
OR REPLACE TABLE default.json_table_flag_3(
    id BIGINT,
    create_at TIMESTAMP,
    v VARIANT
);

CREATE
OR REPLACE TABLE default.json_table_flag_4(
    id BIGINT,
    create_at TIMESTAMP,
    v VARIANT
);