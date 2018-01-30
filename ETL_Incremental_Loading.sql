--*************************************************************************--
-- Author: <JasonBerney>
-- Desc: This file shows how to create a ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-01-27,<JasonBerney>,Created File
--**************************************************************************--
USE [DWNorthwindLite_withSCD];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimProducts')
   Drop Procedure pETLSyncDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimCustomers')
   Drop Procedure pETLSyncDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncFactOrders')
   Drop Procedure pETLSyncFactOrders;

--********************************************************************--
-- A) NOT NEEDED FOR INCREMENTAL LOADING: Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--

--********************************************************************--
-- B) Synchronize the Tables
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
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLSyncDimProducts
/* Author: <JasonBerney>
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 20189-01-17,<JasonBerney>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
		With ChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From ChangedProducts)
    ;

    -- 2)For INSERT or UPDATES: Add new rows to the table
		With AddedORChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO [DWNorthwindLite_withSCD].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimProducts
      WHERE ProductID IN (Select ProductID From AddedORChangedProducts)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
   	)UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From DeletedProducts)
   ;
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
 Exec @Status = pETLSyncDimProducts;
 Print @Status;
 Select * From DimProducts Order By ProductID
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
  FROM [NorthwindLite].dbo.Customers as c
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLSyncDimCustomers
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
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
	With ChangedCustomers
			As(
				Select CustomerID, CustomerName, CustomerCity, CustomerCountry From vETLDimCustomers
					Except
				Select CustomerID, CustomerName, CustomerCity, CustomerCountry From DimCustomers
					Where IsCurrent = 1 -- Needed if the value is changed back to previous value
			  )
	UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers
		SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
        ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From ChangedCustomers)
    ;

    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedCustomers 
		As(
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From vETLDimCustomers
				Except
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From DimCustomers
				Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		  )
	INSERT INTO [DWNorthwindLite_withSCD].dbo.DimCustomers
		([CustomerID],[CustomerName],[CustomerCity],[CustomerCountry],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [CustomerID]
       ,[CustomerName]
       ,[CustomerCity]
       ,[CustomerCountry]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimCustomers
      WHERE CustomerID IN (Select CustomerID From AddedORChangedCustomers)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedCustomers
		As(
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From DimCustomers
				Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
				Select CustomerID, CustomerName, CustomerCity, CustomerCountry From vETLDimCustomers
   		  )
	UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From DeletedCustomers)
   ;
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
 Exec @Status = pETLSyncDimCustomers;
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
      Delete From DimDates; -- Clears table data with the need for dropping FKs
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
	  INNER JOIN [DWNorthwindLite_withSCD].dbo.DimCustomers
		ON Orders.CustomerID = DimCustomers.CustomerID
	  INNER JOIN [DWNorthwindLite_withSCD].dbo.DimDates
		ON Cast(Convert(nVarchar(50), [Orders].[OrderDate], 112) as int) = Cast(Convert(nVarchar(50), [DimDates].[DateKey], 112) as int)
	  INNER JOIN [DWNorthwindLite_withSCD].dbo.DimProducts
		ON DimProducts.ProductID = OrderDetails.ProductID
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLSyncFactOrders
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
   		MERGE Into FactOrders as TargetTable
		Using vETLFactOrders as SourceTable
			ON TargetTable.OrderID = SourceTable.OrderID
			WHEN NOT MATCHED BY TARGET
				THEN -- The ID in the Source is not found the the Target
					INSERT
					VALUES ( SourceTable.OrderID, SourceTable.CustomerKey, SourceTable.OrderDateKey, SourceTable.ProductKey, SourceTable.ActualOrderUnitPrice, SourceTable.ActualOrderQuantity )
			WHEN MATCHED -- When the IDs match for the row currently being looked 
			--AND ( SourceTable.CustomerKey <> TargetTable.CustomerKey -- but the CustomerKey 
				--OR SourceTable.OrderDateKey <> TargetTable.OrderDateKey ) -- or OrderDateKey do not match...
				Then 
					UPDATE -- It knows your target, so you dont specify the FactOrders
					SET TargetTable.CustomerKey = SourceTable.CustomerKey
					  , TargetTable.OrderDateKey = SourceTable.OrderDateKey
					  , TargetTable.ProductKey = SourceTable.ProductKey
					  , TargetTable.ActualOrderUnitPrice = SourceTable.ActualOrderUnitPrice
					  , TargetTable.ActualOrderQuantity = SourceTable.ActualOrderQuantity
			WHEN NOT MATCHED By SOURCE 
				THEN -- The OrderID is in the Target table, but not the source table
					DELETE
		; -- The merge statement demands a semicolon at the end!
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
 Exec @Status = pETLSyncFactOrders;
 Print @Status;
*/
go

--********************************************************************--
-- C)  NOT NEEDED FOR INCREMENTAL LOADING: Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--


--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int = 0;
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = @Status;

Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = @Status;

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;

Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];