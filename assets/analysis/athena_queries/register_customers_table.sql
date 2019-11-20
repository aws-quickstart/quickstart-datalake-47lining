CREATE EXTERNAL TABLE customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    region STRING,
    state STRING,
    cbgid STRING,
    marital_status STRING,
    education_level STRING
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ customers_curated_dir }}';