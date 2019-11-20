CREATE EXTERNAL TABLE {{ external_schema_name }}.ext_demographics (
  geoid                   varchar(35),
  state                   varchar(50),
  population              numeric(18),
  population_density      numeric(37,19),
  households              numeric(18),
  middle_aged_people      numeric(32),
  household_income        numeric(18),
  bachelors_degrees       numeric(20),
  families_with_children  numeric(21),
  children_under_5        numeric(20),
  owner_occupied          numeric(18),
  marriedcouple_family    numeric(18)
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ demographics_curated_dir }}';

ALTER TABLE {{ external_schema_name }}.ext_demographics
ADD PARTITION(dt='{{ demographics_partition }}')
LOCATION 's3://{{ curated_bucket_name }}/{{ demographics_curated_dir }}dt={{ demographics_partition }}/';

CREATE EXTERNAL TABLE {{ external_schema_name }}.ext_products (
  sku                     varchar(35),
  product_category        varchar(50),
  link                    varchar(100),
  company                 varchar(100),
  price                   numeric(32),
  release_date            varchar(30)
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ products_curated_dir }}';

{% for partition in ('2016-12-31', '2015-12-31', '2014-12-31', '2013-12-31') %}

ALTER TABLE {{ external_schema_name }}.ext_products
ADD PARTITION(dt='{{ partition }}')
LOCATION 's3://{{ curated_bucket_name }}/{{ products_curated_dir }}dt={{ partition }}/';

{% endfor %}
