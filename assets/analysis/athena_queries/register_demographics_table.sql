CREATE EXTERNAL TABLE demographics (
  geoid                   STRING,
  state                   STRING,
  population              BIGINT,
  population_density      DOUBLE,
  households              BIGINT,
  middle_aged_people      BIGINT,
  household_income        BIGINT,
  bachelors_degrees       BIGINT,
  families_with_children  BIGINT,
  children_under_5        BIGINT,
  owner_occupied          BIGINT,
  marriedcouple_family    BIGINT
)
PARTITIONED BY (dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://{{ curated_bucket_name }}/{{ demographics_curated_dir }}';