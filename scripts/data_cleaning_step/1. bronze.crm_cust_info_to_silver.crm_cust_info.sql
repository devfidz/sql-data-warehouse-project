-- DATA CLEANING bronze.crm_cust_info TO LOAD INTO silver.crm_cust_info
SELECT * FROM bronze.crm_cust_info

-- code checks for dup
SELECT cst_id, COUNT(*) AS dupd
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- code removes dups by selecting the id with the most recent create date
SELECT * FROM
(
SELECT * ,
 ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS SERIAL_NUMBER
FROM bronze.crm_cust_info
)t WHERE SERIAL_NUMBER = 1

-- code check for gaps in cst_key
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key !=TRIM(cst_key) -- no gaps on cst_key


-- code check for gaps in name
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname !=TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname !=TRIM(cst_lastname)

-- code fixes gaps in names
SELECT 
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info;

-- code check for gaps in marital  status
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status !=TRIM(cst_marital_status) -- no gaps on marital status

-- code standardise marital status
SELECT DISTINCT 
    CASE UPPER(TRIM(cst_marital_status))
      WHEN 'S' THEN 'Single'
      WHEN 'M' THEN 'Married'
      ELSE 'N/A'
    END
AS cst_marital_status
FROM bronze.crm_cust_info;


-- code checks for gaps in customer gender
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr !=TRIM(cst_gndr) -- no gaps on customer gender

-- code stadardize customer gender
SELECT DISTINCT 
    CASE UPPER(TRIM(cst_gndr))
      WHEN 'F' THEN 'Female'
      WHEN 'M' THEN 'Male'
      ELSE 'N/A'
    END
   AS cst_gndr
FROM bronze.crm_cust_info;




--INSERTING TRANSFORMED AND CLEAN DATA INTO silver. crm_cust_info
INSERT INTO silver.crm_cust_info(
       cst_id,
       cst_key,
       cst_firstname,
       cst_lastname,
       cst_marital_status,
       cst_gndr,
       cst_create_date)


SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
    
    CASE UPPER(TRIM(cst_marital_status))
      WHEN 'S' THEN 'Single'
      WHEN 'M' THEN 'Married'
      ELSE 'N/A'
    END AS cst_marital_status,
      
        CASE UPPER(TRIM(cst_gndr))
          WHEN 'F' THEN 'Female'
          WHEN 'M' THEN 'Male'
          ELSE 'N/A'
        END AS cst_gndr,

    cst_create_date
FROM (
	SELECT * ,
	 ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS SERIAL_NUMBER
	FROM bronze.crm_cust_info
	)t WHERE SERIAL_NUMBER = 1

-- after loading the silver table the row with null value was deleted
SELECT * FROM silver.crm_cust_info
DELETE FROM silver.crm_cust_info
WHERE cst_id IS NULL



-- DATA QTY CHEKCS for silver.crm_cust_info

-- code checks if duplicates cst_id exist
SELECT 
cst_id, 
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL



-- code checks if gaps still exist in name
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname !=TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname !=TRIM(cst_lastname)


-- code checks to confirm distinct marital status
SELECT DISTINCT  
    cst_marital_status
FROM silver.crm_cust_info


-- code checks to comfirm distinct geneder 
SELECT DISTINCT  
    cst_gndr
FROM silver.crm_cust_info