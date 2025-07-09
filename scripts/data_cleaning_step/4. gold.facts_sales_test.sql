SELECT * FROM gold.dim_products
SELECT * FROM gold.dim_customers

-- foreign key check
SELECT * FROM 
gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_Key = f.customer_key
--LEFT JOIN gold.dim_products p
--ON p.product_key = f.prod
WHERE c.customer_key IS NULL


-- foreign key check
SELECT * FROM 
gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_Key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL