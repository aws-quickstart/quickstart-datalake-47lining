DROP TABLE IF EXISTS customer_lifetime_value;
CREATE TABLE customer_lifetime_value AS (
  WITH with_spend AS (
    SELECT orders.customer_id, SUM(price) AS spend FROM orders JOIN customers ON orders.customer_id = customers.customer_id
    GROUP BY orders.customer_id
  ),
  with_customer_spend AS (
    SELECT with_spend.*, customers.* FROM with_spend JOIN customers ON with_spend.customer_id = customers.customer_id
  ),
  with_married_spend AS (
    SELECT AVG(spend) AS segment_spend FROM with_customer_spend WHERE marital_status = 'married'
  ),
  with_single_spend AS (
    SELECT AVG(spend) AS segment_spend FROM with_customer_spend WHERE marital_status = 'single'
  ),
  with_bachelor_spend AS (
    SELECT AVG(spend) AS segment_spend FROM with_customer_spend WHERE education_level = 'bachelor'
  ),
  with_primary_spend AS (
    SELECT AVG(spend) AS segment_spend FROM with_customer_spend WHERE education_level = 'primary'
  ),
  with_avg_spend AS (
    SELECT AVG(spend) AS segment_spend FROM with_customer_spend
  )
  SELECT 'Average' AS segment, * FROM with_avg_spend
  UNION
  SELECT 'Married' AS segment, * FROM with_married_spend
  UNION
  SELECT 'Single' AS segment, * FROM with_single_spend
  UNION
  SELECT 'Bachelor' AS segment, * FROM with_bachelor_spend
  UNION
  SELECT 'Primary education' AS segment, * FROM with_primary_spend
);