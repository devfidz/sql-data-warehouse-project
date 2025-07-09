-- code checks for gap
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num!= TRIM(sls_ord_num)


-- code checks for non matching prd key
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)


-- code checks for non matching cst_id
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- code checks for invalide date and formats for sls_order_dt, sls_ship_dt, sls_due_dt
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101


-- code checks if sls_order_dt > sls_ship_dt or sls_due_dt
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt


-- code cheks for consistency Between Sales, QTY and Price
-- >> sales = qty * price
-- >> Value must not be Null, zero or negative
-- >> fixing errors here rquires meeting the business experts
SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS ols_sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales,sls_quantity, sls_price

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	--sls_quantity,
	sls_price AS old_sls_price,
CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;


--INSERTING TRANSFORMED AND CLEAN DATA INTO silver.crm_sales_details

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price )

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,

			-- code fix invalid dates and formats
			CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) != 8
			THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,

			CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) != 8
			THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) != 8
			THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			-- Code fix invalid price, qty
			CASE 
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
							THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect

			sls_quantity,

		   CASE 
						WHEN sls_price IS NULL OR sls_price <= 0 
							THEN sls_sales / NULLIF(sls_quantity, 0)
						ELSE sls_price
		  END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details





-- data qty checks


-- code checks if sls_order_dt > sls_ship_dt or sls_due_dt
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt


-- code follows busimess rules
SELECT DISTINCT
	 sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales,sls_quantity, sls_price

Select * from silver.crm_sales_details