set hive.cli.print.header=true;
DROP TABLE IF EXISTS hospitals;
CREATE TABLE hospitals
    STORED AS PARQUET
    AS
SELECT provider_id, hospital_name as name, state
FROM raw_hospitals
WHERE provider_id is not NULL and provider_id <> '' 
    AND state is not NULL and state <> ''
ORDER BY provider_id;
