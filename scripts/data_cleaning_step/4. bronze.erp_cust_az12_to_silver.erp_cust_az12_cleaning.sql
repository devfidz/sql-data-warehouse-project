-- code checks and fix cid
-- we use substring here on cid so as to match cst_key on bronze.crm_cust_info
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	ELSE cid
	END cid,
bdate,
gen
FROM bronze.erp_cust_az12

/*comparing the bronze.erp_cust_az12 this with silver.crm_cust_info
to comfrim if our data transformation worked correctly*/

WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- code checks for bdate that is out of range
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- code fixes bdate thta is out range
SELECT 
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12


-- code checks for distinct values of gender
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12


-- code fiesx improper gender labeling
SELECT DISTINCT gen,
CASE 
	 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12


--INSERTING TRANSFORMED AND CLEAN DATA INTO silver.erp_cust_az12

INSERT INTO silver.erp_cust_az12(
cid, 
bdate, 
gen)


	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
			ELSE cid
		END cid,


	CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
	END AS bdate,


	CASE 
		 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS gen
	FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12



-- DATA QTY CHECKS

-- code checks bdate that is out of range
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- code cheks for undefined geneder
SELECT DISTINCT gen
FROM silver.erp_cust_az12