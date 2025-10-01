/*
==================================================================================================================
STORED PROCEDURE: Load Bronze Layer (Source -> Bronze)
==================================================================================================================
-- Script purpose:
Stored procedure loads data from the external csv file.
It truncates bronze tables before loading.
uses bulk insert to load large data from the csv file

usage: EXEC bronze.load_bronze;
===================================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		set @batch_start_time = GETDATE()
		PRINT'============================================================';
		PRINT'Loading the Bronze layer';
		PRINT'============================================================';
	

		print'-------------------------------------------------------------------';
		print'Loading CRM Table';
		print'-------------------------------------------------------------------';


		set @start_time = GETDATE()
		print'>> Truncating table:bronze.crm_cust_info'
		truncate table bronze.crm_cust_info;
		print'>> Inserting data into:bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';


		set @start_time = GETDATE()
		print'>> Truncating table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print'>> Inserting data into: bronze.crm_prd_info';

		bulk insert bronze.crm_prd_info
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';


		set @start_time = GETDATE();
		print'>> Truncating table:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
	
		print'>> Inserting data into: bronze.crm_sales_details';

		bulk insert bronze.crm_sales_details
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';


		print'-------------------------------------------------------------------';
		print'Loading ERP Table';
		print'-------------------------------------------------------------------';

		set @start_time = GETDATE();
		print'>> Truncating table:bronze.erp_cust_az12';

		truncate table bronze.erp_cust_az12;

		print'>> Inserting data into: bronze.erp_cust_az12';

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';

		set @start_time = GETDATE();
		print'>> Truncating table: bronze.erp_loc_a101';

		truncate table bronze.erp_loc_a101;

		print'>> Inserting data into: bronze.erp_loc_a101';

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';


		set @start_time = GETDATE();
		print'>> Truncating table: bronze.erp_px_cat_g1v2';

		truncate table bronze.erp_px_cat_g1v2;

		print'>> Inserting data into: bronze.erp_px_cat_g1v2';

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE();
		print'>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';

		set @batch_end_time = GETDATE();
		print'==============================================='
		print'Loading Bronze Layer is completed'
		print'>>Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		print'----------------------';


	END TRY


	BEGIN CATCH
		PRINT'============================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT'ERROR MESSAGE:' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END;


