CREATE EXTERNAL TABLE orders (
    customer_id STRING,
    sku STRING,
    price DOUBLE,
    order_date TIMESTAMP
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ orders_curated_dir }}';