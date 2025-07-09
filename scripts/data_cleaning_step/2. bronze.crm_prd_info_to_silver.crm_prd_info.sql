-- code checks for dup
SELECT prd_id, 
COUNT(*) AS DUP
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL -- no dups in product id

-- code checks for gaps in prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm) -- no gaps in product name

-- code checks for NULLS or Negative numbers
-- Exception: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- code fixes NULLS or Negative numbers
SELECT
ISNULL(prd_cost, 0) AS prd_cost
FROM  bronze.crm_prd_info

-- data trim and standardization for prd_line
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

SELECT prd_line,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 's' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	Else 'N/A'
END AS prd_line
FROM bronze.crm_prd_info

-- code check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- code fixes invlaid date
/*NB start should alwasy older than end date, 
the begining of a new start date should be older than the previous end date*/

SELECT CAST(prd_start_dt AS DATE) AS prd_start_dt, -- data transformation
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info -- data transformation and enrichment



--INSERTING TRANSFORMED AND CLEAN DATA INTO silver.crm_prd_info
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
prd_id,
/*the category ID column is needed to link the ID on
bronze.erp_px_cat_g1v2 table*/
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,  -- forms category ID column

/* the prd_key column is needed to link the sls_prd_key on bronze.sales.details*/
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_Key,  --forms product key column

prd_nm,

ISNULL(prd_cost, 0) AS prd_cost,

CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	Else 'N/A'
END AS prd_line,

CAST(prd_start_dt AS DATE) AS prd_start_dt,

CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info



-- DATA QTY CHECKS for silver.crm_prd_info

-- code checks for dup
SELECT prd_id, 
COUNT(*) AS DUP
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- code checks for gaps in prd_nm
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm)

-- code checks for NULLS or Negative numbers
-- Exception: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- code checks for data standardization and consistency for prd_line
SELECT DISTINCT prd_line
FROM silver.crm_prd_info


-- code checks for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

