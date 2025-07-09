/*code transforms cid on bronze.erp_loc_a101
to match cst_key on silver.crm_cust_info */
SELECT
REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101

-- uncomment this lines to compare cid and cst key
-- WHERE REPLACE(cid, '-', '') NOT IN 
-- (SELECT cst_key FROM silver.crm_cust_info)


-- code checks for distincts country
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

--code transforms an standadise country
SELECT
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL then 'N/A'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101





--INSERTING TRANSFORMED AND CLEAN DATA INTO silver.erp_loc_a10

INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)

		SELECT
		REPLACE(cid, '-', '') cid,

		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL then 'N/A'
			ELSE TRIM(cntry)
		END AS cntry

		FROM bronze.erp_loc_a101


-- Data QTY checks
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101;