DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    customer_id VARCHAR(100) encode zstd,
    sku VARCHAR(100) encode zstd,
    price decimal(8,2) encode delta32k,
    order_date timestamp
)
DISTKEY(customer_id)
SORTKEY(customer_id);

COPY orders
FROM 's3://{{ curated_bucket_name }}/{{ orders_curated_path }}'
IAM_ROLE '{{ redshift_role_arn }}'
DELIMITER ','
REGION '{{ region_name }}';