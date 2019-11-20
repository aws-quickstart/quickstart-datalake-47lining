CREATE EXTERNAL TABLE products (
  sku                     STRING,
  product_category        STRING,
  link                    STRING,
  company                 STRING,
  price                   DOUBLE,
  release_date            STRING
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ products_curated_dir }}';