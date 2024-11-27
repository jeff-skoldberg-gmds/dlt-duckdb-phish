COPY (
    SELECT row_to_json(main.user_setlists_v) AS json_data
    FROM main.user_setlists_v
) TO 'output/part_{}.parquet' (FORMAT PARQUET, FILE_SIZE_BYTES 100000000);