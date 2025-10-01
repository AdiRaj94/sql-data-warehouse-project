-- Create database "Datawarehouse"

use master;
go

/* 
============================================================
Create Database and schemas
============================================================
Script Purpose:
This script creats new database named 'Datawarehouse' after checking it already exists.
If the database exists , it is dropped and recreated. Additionally, the script creates three schemas within the database: "Bronze"
"Silver" and "Gold".


Warning:

Running this script will drop the entire 'DataWarehouse' if it exists.
All the data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups for running the script.

*/

use master;
go
  
  -- Drop and recreate the 'Datawarehouse' database
if exists (select 1 from sys.databases where name = 'DataWarehouse')
Begin
	Alter Database DataWarehouse set single_user with rollback immediate;
	Drop Database DataWarehouse;
End;
Go

-- Create a DataWarehouse database
create database DataWarehouse;
Go

use DataWarehouse;
Go

-- Create Schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
