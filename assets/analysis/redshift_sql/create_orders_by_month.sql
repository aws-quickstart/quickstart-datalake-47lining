DROP TABLE IF EXISTS orders_by_month;
CREATE TABLE orders_by_month AS (
  WITH married_orders AS (
    SELECT DATE_PART('year', order_date) AS year, DATE_PART('month', order_date) AS month, COUNT(*) AS married_orders
    FROM orders JOIN customers ON orders.customer_id = customers.customer_id WHERE marital_status = 'married' GROUP BY year, month
  ),
  single_orders AS (
    SELECT DATE_PART('year', order_date) AS year, DATE_PART('month', order_date) AS month, COUNT(*) AS single_orders
    FROM orders JOIN customers ON orders.customer_id = customers.customer_id WHERE marital_status = 'single' GROUP BY year, month
  ),
  total_orders AS (
    SELECT DATE_PART('year', order_date) AS year, DATE_PART('month', order_date) AS month, COUNT(*) AS total_orders
    FROM orders JOIN customers ON orders.customer_id = customers.customer_id GROUP BY year, month
  )
  SELECT
    married_orders.year || '-' || LPAD(married_orders.month, 2, '0') || '-01' AS month_date,
    married_orders,
    single_orders,
    total_orders
  FROM married_orders
  JOIN single_orders
  ON married_orders.year = single_orders.year AND married_orders.month = single_orders.month
  JOIN total_orders
  ON married_orders.year = total_orders.year AND married_orders.month = total_orders.month
  WHERE married_orders.year < 2017
  ORDER BY married_orders.year, married_orders.month
);