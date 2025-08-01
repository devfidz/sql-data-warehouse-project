/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

How to execute:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
	SET @batch_start_time = GETDATE();

	PRINT'===================================================';
	PRINT'Loading Bronze Layer';
	PRINT'===================================================';

	PRINT'---------------------------------------------------';
	PRINT'Loading CRM Tables';
	PRINT'---------------------------------------------------';

	PRINT'Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT'>> Inserting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH
		(
		FIRSTROW = 2, /* this tells the SQL to skip the first row 
						when loading the data from the source*/
		FIELDTERMINATOR = ',', /* this tells SQL that the columns 
								from the source are sperated by a column*/
		TABLOCK
		);
	-- SELECT * FROM bronze.crm_cust_info;

	PRINT'Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT'>> Inserting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	-- SELECT * FROM bronze.crm_prd_info;

	PRINT'Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT'>> Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	-- SELECT * FROM bronze.crm_sales_details;

	

	PRINT'---------------------------------------------------';
	PRINT'Loading ERP Tables';
	PRINT'----------------------------------------------------';

	PRINT'Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	-- SELECT * FROM bronze.erp_cust_az12;

	PRINT'Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	-- SELECT * FROM bronze.erp_loc_a101;

	PRINT'Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\Qunatum_Dave\Desktop\ANALYTICS\SQL DATA WHAREHOUSE FROM SCRATCH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	-- SELECT * FROM bronze.erp_px_cat_g1v2;
END