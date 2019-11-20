DROP SCHEMA IF EXISTS {{ external_schema_name }};
CREATE EXTERNAL SCHEMA {{ external_schema_name }}
FROM DATA CATALOG
DATABASE '{{ external_database_name }}'
IAM_ROLE '{{ redshift_role_arn }}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;