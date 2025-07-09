-- was checked and clean
/*recall thta we created a new coulmn cat_id on silver.crm_prd_info
this will be used to link prd_info and px_cat*/
select * from silver.crm_prd_info


-- code checks for unwanted spaces
SELECT * FROM
bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR 
subcat != TRIM(subcat) OR
maintenance != TRIM(maintenance)

-- checks for data standardization
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2


-- checks for data standardization
SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2


-- checks for data standardization
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2



--INSERTING TRANSFORMED AND CLEAN DATA INTO silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
