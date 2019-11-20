DROP TABLE IF EXISTS state_population;
CREATE TABLE state_population AS (
  SELECT state, SUM(population) AS population FROM {{ external_schema_name }}.ext_demographics GROUP BY state
);