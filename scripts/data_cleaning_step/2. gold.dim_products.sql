-- code checks for dup id
SELECT prd_key, COUNT(*) FROM
(SELECT
      pn.cat_id,
      pn.prd_key,
      pn.prd_nm,
      pn.prd_cost,
      pn.prd_line,
      pn.prd_start_dt,
      pc.cat,
      pc.subcat,
      pc.maintenance
  FROM silver.crm_prd_info AS pn
  LEFT JOIN silver.erp_px_cat_g1v2 AS pc
  ON pn.cat_id = pc.id
  WHERE prd_end_dt IS null)t -- filter out all historical data 
  GROUP BY prd_key 
  HAVING COUNT(*) > 1


-- code creates gold.dim_products V.table
CREATE VIEW gold.dim_products AS
SELECT
      ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- surrogate_key
      pn.prd_id AS product_id,
      pn.prd_key AS product_number,
      pn.prd_nm AS product_name,
      pn.cat_id AS category_id,
      pc.cat AS category,
      pc.subcat AS sub_category,
      pc.maintenance,
      pn.prd_cost AS cost,
      pn.prd_line AS product_line,
      pn.prd_start_dt AS start_date
  FROM silver.crm_prd_info AS pn
  LEFT JOIN silver.erp_px_cat_g1v2 AS pc
  ON pn.cat_id = pc.id

  SELECT * FROM gold.dim_products