DROP TABLE IF EXISTS product_category_distribution;
CREATE TABLE product_category_distribution AS
SELECT product_category, COUNT(*) AS orders_count
FROM orders JOIN {{ external_schema_name }}.ext_products ON orders.sku = ext_products.sku
WHERE ext_products.dt = '2016-12-31'
GROUP BY product_category;