DROP TABLE IF EXISTS spend_distribution;
CREATE TABLE spend_distribution AS
  WITH with_customer_spend AS (
    SELECT orders.customer_id, SUM(price) AS spend FROM orders JOIN customers ON orders.customer_id = customers.customer_id
    GROUP BY orders.customer_id
  ),
  with_segment AS (
    SELECT with_spend.*, marital_status, education_level FROM with_customer_spend AS with_spend JOIN customers ON with_spend.customer_id = customers.customer_id
  )
SELECT * FROM with_segment;