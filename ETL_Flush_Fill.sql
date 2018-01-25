--*************************************************************************--
-- Author: <JasonBerney>
-- Change Log: When,Who,What
-- 2018-01-17,<JasonBerney>,Created File
--**************************************************************************--
USE [DWNorthwindLite];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimProducts')
   Drop Procedure pETLFillDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimCustomers')
   Drop Procedure pETLFillDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillFactOrders')
   Drop Procedure pETLFillFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;

--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--
go
Create Procedure pETLDropForeignKeyConstraints
/* Author: <JasonBerney>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	  Drop Constraint [fkFactOrdersToDimProducts]; 
	Alter Table [DWNorthwindLite].dbo.FactOrders
	  Drop Constraint [fkFactOrdersToDimCustomers]; 
    -- Optional: Unlike the other tables DimDates does not change often --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	   Drop Constraint [fkFactOrdersToDimDates];
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/
go

Create Procedure pETLTruncateTables
/* Author: <JasonBerney>
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Truncate Table [DWNorthwindLite].dbo.DimProducts;
	Truncate Table [DWNorthwindLite].dbo.DimCustomers;
	Truncate Table [DWNorthwindLite].dbo.FactOrders;
    -- Optional: Unlike the other tables DimDates does not change often --
    Truncate Table [DWNorthwindLite].dbo.DimDates; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/
go

--********************************************************************--
-- B) FILL the Tables
--********************************************************************--
/****** [dbo].[DimProducts] ******/
go 
Create View vETLDimProducts
/* Author: <JasonBerney>
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
   ,[StartDate] = 20000101
   ,[EndDate] = Null -- Default
   ,[IsCurrent] = 'Yes' -- Default
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLFillDimProducts
/* Author: <JasonBerney>
** Desc: Inserts data into DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimProducts) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate]
       ,[EndDate]
       ,[IsCurrent]
      FROM vETLDimProducts
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimProducts;
 Print @Status;
*/


/****** [dbo].[DimCustomers] ******/
go 
Create View vETLDimCustomers
/* Author: <JasonBerney>
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
As
  SELECT
    [CustomerID] = c.CustomerID
   ,[CustomerName] = CAST(c.CompanyName as nVarchar(100))
   ,[CustomerCity] = CAST(c.City as nVarchar(100))
   ,[CustomerCountry] = CAST(c.Country as nVarchar(100))
   ,[StartDate] = 20000101
   ,[EndDate] = Null -- Default
   ,[IsCurrent] = 'Yes' -- Default
  FROM [NorthwindLite].dbo.Customers as c
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLFillDimCustomers
/* Author: <JasonBerney>
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimCustomers) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.DimCustomers
      ([CustomerID],[CustomerName],[CustomerCity],[CustomerCountry],[StartDate],[EndDate],[IsCurrent])
      SELECT
		 [CustomerID]
		,[CustomerName]
		,[CustomerCity]
		,[CustomerCountry]
		,[StartDate]
		,[EndDate]
		,[IsCurrent]
      FROM vETLDimCustomers
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimCustomers;
 Print @Status;
*/
go

/****** [dbo].[DimDates] ******/
Create Procedure pETLFillDimDates
/* Author: <JasonBerney>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '12/31/1999' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ( [DateKey], [USADateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthKey]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Cast(DateName(YYYY,@DateInProcess) + '0' + (DateName(quarter, @DateInProcess) ) as int)  -- [QuarterKey]
        ,'Q' + DateName(quarter, @DateInProcess) + ' - ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/
go

/****** [dbo].[FactOrders] ******/
go 
Create View vETLFactOrders
/* Author: <JasonBerney>
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
As
 SELECT
     [OrderID] = Orders.OrderID
    ,[CustomerKey] = DimCustomers.CustomerKey
    ,[OrderDateKey] = [DimDates].[DateKey]
    ,[ProductKey] = DimProducts.ProductKey
    ,[ActualOrderUnitPrice] = CAST([OrderDetails].[UnitPrice] as Decimal(10,2))
    ,[ActualOrderQuantity] = CAST([OrderDetails].[Quantity] as int)
	 FROM NorthwindLite.dbo.OrderDetails
	  INNER JOIN NorthwindLite.dbo.Orders
		ON OrderDetails.OrderID = Orders.OrderID
	  INNER JOIN DWNorthwindLite.dbo.DimCustomers
		ON Orders.CustomerID = DimCustomers.CustomerID
	  INNER JOIN DWNorthwindLite.dbo.DimDates
		ON Cast(Convert(nVarchar(50), [Orders].[OrderDate], 112) as int) = Cast(Convert(nVarchar(50), [DimDates].[DateKey], 112) as int)
	  INNER JOIN DWNorthwindLite.dbo.DimProducts
		ON DimProducts.ProductID = OrderDetails.ProductID
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLFillFactOrders
/* Author: <JasonBerney>
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From FactOrders) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.FactOrders
      ([OrderID],[CustomerKey],[OrderDateKey],[ProductKey],[ActualOrderUnitPrice],[ActualOrderQuantity])
      SELECT
		 [OrderID]
		,[CustomerKey]
		,[OrderDateKey]
		,[ProductKey]
		,[ActualOrderUnitPrice]
		,[ActualOrderQuantity]
      FROM vETLFactOrders
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactOrders;
 Print @Status;
*/
go

--********************************************************************--
-- C) Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
go
Create Procedure pETLAddForeignKeyConstraints
/* Author: <JasonBerney>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimProducts
      FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey);

	ALTER TABLE DWNorthwindLite.dbo.FactOrders
	  ADD CONSTRAINT fkFactOrdersToDimCustomers
	  FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey)

    -- Optional: Unlike the other tables DimDates does not change often --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimDates 
      FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLAddForeignKeyConstraints;
 Print @Status;
*/
go

--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int;
Exec @Status = pETLDropForeignKeyConstraints;
Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = @Status;

Exec @Status = pETLTruncateTables;
Select [Object] = 'pETLTruncateTables', [Status] = @Status;

Exec @Status = pETLFillDimProducts;
Select [Object] = 'pETLFillDimProducts', [Status] = @Status;

Exec @Status = pETLFillDimCustomers;
Select [Object] = 'pETLFillDimCustomers', [Status] = @Status;

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;

Exec @Status = pETLFillFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

Exec @Status = pETLAddForeignKeyConstraints;
Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = @Status;

go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];