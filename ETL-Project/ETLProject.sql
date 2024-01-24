// Permorming ETL on the WWI DataMart

use [master];
go
Alter database WWIDM  set single_user with rollback immediate;
GO
DROP Database WWIDM ;
GO

CREATE DATABASE WWIDM;
GO
Use WWIDM;
go

CREATE TABLE dbo.DimCities(
    CityKey INT NOT NULL,
    CityName NVARCHAR(50) NULL,
    StateProvCode NVARCHAR(5) NULL,
    StateProvName NVARCHAR(50) NULL,
    CountryName NVARCHAR(60) NULL,
    CountryFormalName NVARCHAR(60) NULL,
    CONSTRAINT PK_DimCities PRIMARY KEY CLUSTERED ( CityKey )
);

CREATE TABLE dbo.DimCustomers(
    CustomerKey INT NOT NULL,
    CustomerName NVARCHAR(100) NULL,
    CustomerCategoryName NVARCHAR(50) NULL,
    DeliveryCityName NVARCHAR(50) NULL,
    DeliveryStateProvCode NVARCHAR(5) NULL,
    DeliveryCountryName NVARCHAR(50) NULL,
    PostalCityName NVARCHAR(50) NULL,
    PostalStateProvCode NVARCHAR(5) NULL,
    PostalCountryName NVARCHAR(50) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_DimCustomers PRIMARY KEY CLUSTERED ( CustomerKey )
);

CREATE TABLE dbo.DimProducts(
    ProductKey INT NOT NULL,
    ProductName NVARCHAR(100) NULL,
    ProductColour NVARCHAR(20) NULL,
    ProductBrand NVARCHAR(50) NULL,
    ProductSize NVARCHAR(20) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_DimProducts PRIMARY KEY CLUSTERED ( ProductKey )
);

CREATE TABLE dbo.DimSalesPeople(
    SalespersonKey INT NOT NULL,
    FullName NVARCHAR(50) NULL,
    PreferredName NVARCHAR(50) NULL,
    LogonName NVARCHAR(50) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    EmailAddress NVARCHAR(256) NULL,
    CONSTRAINT PK_DimSalesPeople PRIMARY KEY CLUSTERED (SalespersonKey )
);

CREATE TABLE dbo.DimDate(
    DateKey INT NOT NULL,
    DateValue DATE NOT NULL,
    Year SMALLINT NOT NULL,
    Month TINYINT NOT NULL,
    Day TINYINT NOT NULL,
    Quarter TINYINT NOT NULL,
    StartOfMonth DATE NOT NULL,
    EndOfMonth DATE NOT NULL,
    MonthName VARCHAR(9) NOT NULL,
    DayOfWeekName VARCHAR(9) NOT NULL,
    CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED ( DateKey )
);

CREATE TABLE dbo.DimPickingStaff(
    PickingStaffKey INT NOT NULL,
    FullName NVARCHAR(50) NULL,
    PreferredName NVARCHAR(50) NULL,
    LogonName NVARCHAR(50) NULL,
    CustomFields NVARCHAR(MAX) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    EmailAddress NVARCHAR(256) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_DimPickingStaff PRIMARY KEY CLUSTERED (PickingStaffKey)

)

CREATE TABLE dbo.FactOrders(
    CustomerKey INT NOT NULL,
    CityKey INT NOT NULL,
    ProductKey INT NOT NULL,
    SalespersonKey INT NOT NULL,
    DateKey INT NOT NULL,
    PickingStaffKey INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18, 2) NOT NULL,
    TaxRate DECIMAL(18, 3) NOT NULL,
    TotalBeforeTax DECIMAL(18, 2) NOT NULL,
    TotalAfterTax DECIMAL(18, 2) NOT NULL,

    CONSTRAINT FK_FactOrders_DimCities FOREIGN KEY(CityKey) REFERENCES dbo.DimCities (CityKey),
    CONSTRAINT FK_FactOrders_DimCustomers FOREIGN KEY(CustomerKey) REFERENCES dbo.DimCustomers (CustomerKey),
    CONSTRAINT FK_FactOrders_DimDate FOREIGN KEY(DateKey) REFERENCES dbo.DimDate (DateKey),
    CONSTRAINT FK_FactOrders_DimProducts FOREIGN KEY(ProductKey) REFERENCES dbo.DimProducts (ProductKey),
    CONSTRAINT FK_FactOrders_DimSalesPeople FOREIGN KEY(SalespersonKey) REFERENCES dbo.DimSalesPeople (SalespersonKey),
    CONSTRAINT FK_FactOrders_DimPickingStaff FOREIGN KEY(PickingStaffKey) REFERENCES dbo.DimPickingStaff (PickingStaffKey)
    
);
CREATE INDEX IX_FactOrders_CustomerKey ON dbo.FactOrders(CustomerKey);
CREATE INDEX IX_FactOrders_CityKey ON dbo.FactOrders(CityKey);
CREATE INDEX IX_FactOrders_ProductKey ON dbo.FactOrders(ProductKey);
CREATE INDEX IX_FactOrders_SalespersonKey ON dbo.FactOrders(SalespersonKey);
CREATE INDEX IX_FactOrders_DateKey ON dbo.FactOrders(DateKey);
CREATE INDEX IX_FactOrders_PickingStaffKey ON dbo.FactOrders(PickingStaffKey);

GO

/*  Star Schema Dimensional Model Data Warehouse Data Mart structure created.  */

CREATE PROCEDURE dbo.DimDate_Load 
    @DateValue DATE
AS
BEGIN;

    INSERT INTO dbo.DimDate
    SELECT CAST( YEAR(@DateValue) * 10000 + MONTH(@DateValue) * 100 + DAY(@DateValue) AS INT),
           @DateValue,
           YEAR(@DateValue),
           MONTH(@DateValue),
           DAY(@DateValue),
           DATEPART(qq,@DateValue),
           DATEADD(DAY,1,EOMONTH(@DateValue,-1)),
           EOMONTH(@DateValue),
           DATENAME(mm,@DateValue),
           DATENAME(dw,@DateValue);
END
GO
  
Execute dbo.DimDate_Load '2013-01-01';
GO

/* Start of Stage table creation & Extract from OLTP database tables stored procedures:  */

CREATE TABLE dbo.Customers_Stage (
    CustomerName NVARCHAR(100),
    CustomerCategoryName NVARCHAR(50),
    DeliveryCityName NVARCHAR(50),
    DeliveryStateProvinceCode NVARCHAR(5),
    DeliveryStateProvinceName NVARCHAR(50),
    DeliveryCountryName NVARCHAR(50),
    DeliveryFormalName NVARCHAR(60),
    PostalCityName NVARCHAR(50),
    PostalStateProvinceCode NVARCHAR(5),
    PostalStateProvinceName NVARCHAR(50),
    PostalCountryName NVARCHAR(50),
    PostalFormalName NVARCHAR(60)
);
GO
CREATE PROCEDURE dbo.Customers_Extract
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT;

    TRUNCATE TABLE dbo.Customers_Stage;

    WITH CityDetails AS (
        SELECT ci.CityID,
               ci.CityName,
               sp.StateProvinceCode,
               sp.StateProvinceName,
               co.CountryName,
               co.FormalName

        FROM WideWorldImporters.Application.Cities ci
        LEFT JOIN WideWorldImporters.Application.StateProvinces sp
            ON ci.StateProvinceID = sp.StateProvinceID
        LEFT JOIN WideWorldImporters.Application.Countries co
            ON sp.CountryID = co.CountryID ) 
    INSERT INTO dbo.Customers_Stage (
        CustomerName,
        CustomerCategoryName,
        DeliveryCityName,
        DeliveryStateProvinceCode,
        DeliveryStateProvinceName,
        DeliveryCountryName,
        DeliveryFormalName,
        PostalCityName,
        PostalStateProvinceCode,
        PostalStateProvinceName,
        PostalCountryName,
        PostalFormalName )
    SELECT cust.CustomerName,
           cat.CustomerCategoryName,
           dc.CityName,
           dc.StateProvinceCode,
           dc.StateProvinceName,
           dc.CountryName,
           dc.FormalName,
           pc.CityName,
           pc.StateProvinceCode,
           pc.StateProvinceName,
           pc.CountryName,
           pc.FormalName
    FROM WideWorldImporters.Sales.Customers cust
    LEFT JOIN WideWorldImporters.Sales.CustomerCategories cat
        ON cust.CustomerCategoryID = cat.CustomerCategoryID
    LEFT JOIN CityDetails dc
        ON cust.DeliveryCityID = dc.CityID
    LEFT JOIN CityDetails pc
        ON cust.PostalCityID = pc.CityID;

    SET @RowCt = @@ROWCOUNT;
    IF @RowCt = 0 
    BEGIN;
        THROW 50001, 'No records found. Check with source system.', 1;
    END;
END;
GO
Execute dbo.Customers_Extract;
GO

/*   Products   extract  */
CREATE TABLE dbo.Products_Stage (
    ProductName NVARCHAR(100),
    ProductColour NVARCHAR(20) ,
    ProductBrand NVARCHAR(50) ,
    ProductSize NVARCHAR(20) 
);
GO

CREATE PROCEDURE dbo.Products_Extract
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT;

    TRUNCATE TABLE dbo.Products_Stage;

    INSERT INTO dbo.Products_Stage (
        ProductName,
        ProductColour,
    ProductBrand,
    ProductSize  )
    SELECT prod.StockItemName,
           col.ColorName,
           prod.Brand,
           prod.Size
    FROM WideWorldImporters.Warehouse.Stockitems prod
    LEFT JOIN WideWorldImporters.Warehouse.Colors col
        ON prod.ColorID = col.ColorID
    ;

    SET @RowCt = @@ROWCOUNT;
    IF @RowCt = 0 
    BEGIN;
        THROW 50001, 'No records found. Check with source system.', 1;
    END;
END;
GO

Execute dbo.Products_Extract;
GO

--  Salespeople Extract from the OLTP Database Table -- 
CREATE TABLE dbo.SalesPeople_Stage(
    
    FullName NVARCHAR(50) ,
    PreferredName NVARCHAR(50) ,
    LogonName NVARCHAR(50) ,
    PhoneNumber NVARCHAR(20) ,
    FaxNumber NVARCHAR(20) ,
    EmailAddress NVARCHAR(256) 
);
GO
CREATE PROCEDURE dbo.SalesPeople_Extract
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT;

    TRUNCATE TABLE dbo.SalesPeople_Stage;

    INSERT INTO dbo.SalesPeople_Stage (
        FullName,
        PreferredName,
    LogonName,
    PhoneNumber,
    FaxNumber,
    EmailAddress
  )
    SELECT Peop.FullName,
        Peop.PreferredName,
    Peop.LogonName,
    Peop.PhoneNumber,
        Peop.FaxNumber,
    Peop.EmailAddress
    FROM WideWorldImporters.Application.People Peop
    WHERE Peop.IsSalesperson = 1 
    ;

    SET @RowCt = @@ROWCOUNT;
    IF @RowCt = 0 
    BEGIN;
        THROW 50001, 'No records found. Check with source system.', 1;
    END;
END;
GO

Execute dbo.SalesPeople_Extract;
GO

CREATE TABLE dbo.PickingStaff_Stage (
    FullName NVARCHAR(50),
    PreferredName NVARCHAR(50),
    LogonName NVARCHAR(50),
    CustomFields NVARCHAR(MAX),
    PhoneNumber NVARCHAR(20),
    FaxNumber NVARCHAR(20),
    EmailAddress NVARCHAR(256)
);
GO
CREATE PROCEDURE dbo.PickingStaff_Extract
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT;

    TRUNCATE TABLE dbo.PickingStaff_Stage;

    INSERT INTO dbo.PickingStaff_Stage (
        FullName,
        PreferredName,
        LogonName,
        CustomFields,
        PhoneNumber,
        FaxNumber,
        EmailAddress )
    SELECT ppl.FullName,
        ppl.PreferredName,
        ppl.LogonName,
        ppl.CustomFields,
        ppl.PhoneNumber,
        ppl.FaxNumber,
        ppl.EmailAddress

    FROM WideWorldImporters.Sales.Orders ord
    LEFT JOIN WideWorldImporters.Application.People ppl
        ON ppl.PersonID = ord.PickedByPersonID;

    SET @RowCt = @@ROWCOUNT;
    IF @RowCt = 0 
    BEGIN;
        THROW 50001, 'No records found. Check with source system.', 1;
    END;
END;
GO
Execute dbo.PickingStaff_Extract;
GO

CREATE TABLE dbo.Orders_Stage (
    OrderDate         DATE,
    Quantity          INT,
    UnitPrice         DECIMAL(18,2),
    TaxRate           DECIMAL(18,3),
    CustomerName      NVARCHAR(100),
    CityName          NVARCHAR(50),
    StateProvinceName NVARCHAR(50),
    CountryName       NVARCHAR(60),
    StockItemName     NVARCHAR(100),
    LogonName         NVARCHAR(50),
    PickingStaffName  NVARCHAR(50)
);
GO

CREATE PROCEDURE dbo.Orders_Extract (
   @OrdersDate DATE)
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT;

    TRUNCATE TABLE dbo.Orders_Stage;

    INSERT INTO dbo.Orders_Stage (
        OrderDate,   
        Quantity ,    
        UnitPrice    ,
        TaxRate      ,
        CustomerName ,
        CityName  ,   
        StateProvinceName ,
        CountryName  ,
    StockItemName ,
        LogonName ,
        PickingStaffName  
    
)
    SELECT 
     ord.OrderDate,
     orl.Quantity,
     orl.UnitPrice,
     orl.TaxRate,
     cust.CustomerName,
     cit.CityName,
     pro.StateProvinceName,
     cou.CountryName,
     sto.StockItemName,
     peop.LogonName,
     ppl.LogonName
    
          
    FROM WideWorldImporters.Sales.Orders ord
    LEFT JOIN WideWorldImporters.Sales.Customers cust
        ON cust.CustomerID = ord.CustomerID
    LEFT JOIN WideWorldImporters.Sales.OrderLines orl
        ON ord.OrderID = orl.OrderID
    LEFT JOIN WideWorldImporters.Application.People peop
        ON ord.SalespersonPersonID = peop.PersonID
    LEFT JOIN WideWorldImporters.Application.Cities cit
        ON cit.cityID = cust.DeliveryCityID
    LEFT JOIN WideWorldImporters.Application.StateProvinces pro
        ON pro.StateProvinceID = cit.StateProvinceID
    LEFT JOIN WideWorldImporters.Application.Countries cou
        ON cou.CountryID = pro.CountryID
    LEFT JOIN WideWorldImporters.Warehouse.StockItems sto
        ON sto.StockItemID = orl.StockItemID
    LEFT JOIN WideWorldImporters.Application.People ppl
        ON ord.PickedByPersonID=ppl.PersonID
    
        WHERE Ord.OrderDate = @OrdersDate;

    SET @RowCt = @@ROWCOUNT;
    IF @RowCt = 0 
    BEGIN;
        THROW 50001, 'No records found. Check with source system.', 1;
    END;
END;

GO
Execute dbo.Orders_Extract @OrdersDate = '2013-01-01';
GO

/* End of Extracts - now create _Preload tables & transforms Procs to Insert data into them */

CREATE TABLE dbo.Cities_Preload (
    CityKey INT NOT NULL,   
    CityName NVARCHAR(50) NULL,
    StateProvCode NVARCHAR(5) NULL,
    StateProvName NVARCHAR(50) NULL,
    CountryName NVARCHAR(60) NULL,
    CountryFormalName NVARCHAR(60) NULL,
    CONSTRAINT PK_Cities_Preload PRIMARY KEY CLUSTERED ( CityKey )
);

CREATE SEQUENCE dbo.CityKey START WITH 1;
GO

CREATE PROCEDURE dbo.Cities_Transform
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Cities_Preload;

    BEGIN TRANSACTION;

    INSERT INTO dbo.Cities_Preload /* Column list excluded for brevity */
       (CityKey,
       CityName,
       StateProvCode,
       StateProvName,
       CountryName,
       CountryFormalName)
    SELECT NEXT VALUE FOR dbo.CityKey AS CityKey,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryStateProvinceName,
           cu.DeliveryCountryName,
           cu.DeliveryFormalName
    FROM dbo.Customers_Stage cu
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimCities ci
                       WHERE cu.DeliveryCityName = ci.CityName
                             AND cu.DeliveryStateProvinceName = ci.StateProvName
                             AND cu.DeliveryCountryName = ci.CountryName );

    INSERT INTO dbo.Cities_Preload /* Column list excluded for brevity */
        (CityKey,
        CityName,
       StateProvCode,
       StateProvName,
       CountryName,
       CountryFormalName)
    SELECT ci.CityKey,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryStateProvinceName,
           cu.DeliveryCountryName,
           cu.DeliveryFormalName
    FROM dbo.Customers_Stage cu
    JOIN dbo.DimCities ci
        ON  cu.DeliveryCityName = ci.CityName
        AND cu.DeliveryStateProvinceName = ci.StateProvName
        AND cu.DeliveryCountryName = ci.CountryName;

    COMMIT TRANSACTION;
END;
GO
Execute dbo.Cities_Transform;
GO

CREATE TABLE dbo.SalesPeople_Preload (
    SalespersonKey INT NOT NULL,
    FullName NVARCHAR(50) NULL,
    PreferredName NVARCHAR(50) NULL,
    LogonName NVARCHAR(50) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    EmailAddress NVARCHAR(256) NULL,
    CONSTRAINT PK_SalesPeoplePreload PRIMARY KEY CLUSTERED (SalespersonKey )
);

CREATE SEQUENCE dbo.SalespersonKey START WITH 1;
GO

CREATE PROCEDURE dbo.Salespeople_Transform
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Salespeople_Preload;

    BEGIN TRANSACTION;

    INSERT INTO dbo.Salespeople_Preload /* Column list excluded for brevity */
       (SalespersonKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress )
    SELECT NEXT VALUE FOR dbo.SalespersonKey SalespersonKey,
        salu.FullName ,
    salu.PreferredName ,
    salu.LogonName ,
    salu.PhoneNumber ,
    salu.FaxNumber ,
    salu.EmailAddress 
    FROM dbo.SalesPeople_Stage salu
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimSalespeople sali
                       WHERE salu.LogonName = sali.LogonName  );

 INSERT INTO dbo.Salespeople_Preload /* Column list excluded for brevity */
       (SalespersonKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress )
    SELECT NEXT VALUE FOR dbo.SalespersonKey SalespersonKey,
        salu.FullName ,
    salu.PreferredName ,
    salu.LogonName ,
    salu.PhoneNumber ,
    salu.FaxNumber ,
    salu.EmailAddress 
    FROM dbo.SalesPeople_Stage salu
    JOIN dbo.DimSalespeople sali    
        ON salu.LogonName = sali.LogonName  ;

    COMMIT TRANSACTION;
END;
GO
Execute dbo.Salespeople_Transform;
GO

CREATE TABLE dbo.Customers_Preload (
CustomerKey INT NOT NULL,
    CustomerName NVARCHAR(100) NULL,
    CustomerCategoryName NVARCHAR(50) NULL,
    DeliveryCityName NVARCHAR(50) NULL,
    DeliveryStateProvCode NVARCHAR(5) NULL,
    DeliveryCountryName NVARCHAR(50) NULL,
    PostalCityName NVARCHAR(50) NULL,
    PostalStateProvCode NVARCHAR(5) NULL,
    PostalCountryName NVARCHAR(50) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_Customers_Preload PRIMARY KEY CLUSTERED ( CustomerKey )
);
GO
CREATE SEQUENCE dbo.CustomerKey START WITH 1;
GO

CREATE PROCEDURE dbo.Customers_Transform
   @StartDate DATE,
   @EndDate DATE
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Customers_Preload;
    
    BEGIN TRANSACTION;

    -- Add updated records
    INSERT INTO dbo.Customers_Preload 
    /* Column list excluded for brevity */
    ( CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate )

    SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
           stg.CustomerName,
           stg.CustomerCategoryName,
           stg.DeliveryCityName,
           stg.DeliveryStateProvinceCode,
           stg.DeliveryCountryName,
           stg.PostalCityName,
           stg.PostalStateProvinceCode,
           stg.PostalCountryName,
           @StartDate,
           NULL
    FROM dbo.Customers_Stage stg
    JOIN dbo.DimCustomers cu
        ON stg.CustomerName = cu.CustomerName
        AND cu.EndDate IS NULL
    WHERE stg.CustomerCategoryName <> cu.CustomerCategoryName
          OR stg.DeliveryCityName <> cu.DeliveryCityName
          OR stg.DeliveryStateProvinceCode <> cu.DeliveryStateProvCode
          OR stg.DeliveryCountryName <> cu.DeliveryCountryName
          OR stg.PostalCityName <> cu.PostalCityName
          OR stg.PostalStateProvinceCode <> cu.PostalStateProvCode
          OR stg.PostalCountryName <> cu.PostalCountryName;

    INSERT INTO dbo.Customers_Preload /* Column list excluded for brevity */
        ( CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate )
    SELECT cu.CustomerKey,
           cu.CustomerName,
           cu.CustomerCategoryName,
           cu.DeliveryCityName,
           cu.DeliveryStateProvCode,
           cu.DeliveryCountryName,
           cu.PostalCityName,
           cu.PostalStateProvCode,
           cu.PostalCountryName,
           cu.StartDate,
           CASE 
               WHEN pl.CustomerName IS NULL THEN NULL
               ELSE @EndDate
           END AS EndDate
    FROM dbo.DimCustomers cu
    LEFT JOIN dbo.Customers_Preload pl    
        ON pl.CustomerName = cu.CustomerName
        AND cu.EndDate IS NULL;
    
    -- Create new records
    INSERT INTO dbo.Customers_Preload /* Column list excluded for brevity */
        ( CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate )
    SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
           stg.CustomerName,
           stg.CustomerCategoryName,
           stg.DeliveryCityName,
           stg.DeliveryStateProvinceCode,
           stg.DeliveryCountryName,
           stg.PostalCityName,
           stg.PostalStateProvinceCode,
           stg.PostalCountryName,
           @StartDate,
           NULL
    FROM dbo.Customers_Stage stg
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimCustomers cu WHERE stg.CustomerName = cu.CustomerName );

    -- Expire missing records
    INSERT INTO dbo.Customers_Preload /* Column list excluded for brevity */
        ( CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate )
    SELECT cu.CustomerKey,
           cu.CustomerName,
           cu.CustomerCategoryName,
           cu.DeliveryCityName,
           cu.DeliveryStateProvCode,
           cu.DeliveryCountryName,
           cu.PostalCityName,
           cu.PostalStateProvCode,
           cu.PostalCountryName,
           cu.StartDate,
           @EndDate
    FROM dbo.DimCustomers cu
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.Customers_Stage stg WHERE stg.CustomerName = cu.CustomerName )
          AND cu.EndDate IS NULL;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.Customers_Transform '2013-01-01', '2012-12-31';
GO

CREATE TABLE dbo.Products_Preload(
    ProductKey INT NOT NULL,
    ProductName NVARCHAR(100) NULL,
    ProductColour NVARCHAR(20) NULL,
    ProductBrand NVARCHAR(50) NULL,
    ProductSize NVARCHAR(20) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_Products_Preload PRIMARY KEY CLUSTERED ( ProductKey )
);

GO
CREATE SEQUENCE dbo.ProductKey START WITH 1;
GO

CREATE PROCEDURE dbo.Products_Transform
   @StartDate DATE,
   @EndDate DATE
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Products_Preload;

    BEGIN TRANSACTION;

    -- Add updated records
    INSERT INTO dbo.Products_Preload 
    /* Column list excluded for brevity */
    ( ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate )

    SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
           stg.ProductName,
           stg.ProductColour,
           stg.ProductBrand,
           stg.ProductSize,
           @StartDate,
           NULL
    FROM dbo.Products_Stage stg
    JOIN dbo.DimProducts cu
        ON stg.ProductName = cu.ProductName
        AND cu.EndDate IS NULL
    WHERE stg.ProductColour <> cu.ProductColour
          OR stg.ProductBrand <> cu.ProductBrand
          OR stg.ProductSize <> cu.ProductSize
          ;

    INSERT INTO dbo.Products_Preload /* Column list excluded for brevity */
        ( ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate )
    SELECT cu.ProductKey,
           cu.ProductName,
           cu.ProductColour,
           cu.ProductBrand,
           cu.ProductSize,
           cu.StartDate,
           CASE 
               WHEN pl.ProductName IS NULL THEN NULL
               ELSE @EndDate
           END AS EndDate
    FROM dbo.DimProducts cu
    LEFT JOIN dbo.Products_Preload pl    
        ON pl.ProductName = cu.ProductName
        AND cu.EndDate IS NULL;

    INSERT INTO dbo.Products_Preload /* Column list excluded for brevity */
        ( ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate )
    SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
           stg.ProductName,
           stg.ProductColour,
           stg.ProductBrand,
           stg.ProductSize,
           @StartDate,
           NULL
    FROM dbo.Products_Stage stg
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimProducts cu WHERE stg.ProductName = cu.ProductName );

    INSERT INTO dbo.Products_Preload /* Column list excluded for brevity */
        (ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate)
    SELECT cu.ProductKey,
           cu.ProductName,
           cu.ProductColour,
           cu.ProductBrand,
           cu.ProductSize,
           cu.StartDate,
           @EndDate
    FROM dbo.DimProducts cu
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.Products_Stage stg WHERE stg.ProductName = cu.ProductName )
          AND cu.EndDate IS NULL;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.Products_Transform '2013-01-01', '2012-12-31';
GO

CREATE TABLE dbo.PickingStaff_Preload (
    PickingStaffKey INT NOT NULL,
    FullName NVARCHAR(50) NULL,
    PreferredName NVARCHAR(50) NULL,
    LogonName NVARCHAR(50) NULL,
    CustomFields NVARCHAR(MAX) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    EmailAddress NVARCHAR(256) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    CONSTRAINT PK_PickingStaffPreload PRIMARY KEY CLUSTERED (PickingStaffKey)
);

CREATE SEQUENCE dbo.PickingStaffKey START WITH 1;
GO

CREATE PROCEDURE dbo.PickingStaff_Transform
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.PickingStaff_Preload;

    BEGIN TRANSACTION;

    INSERT INTO dbo.PickingStaff_Preload /* Column list excluded for brevity */
       (PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate )
    SELECT NEXT VALUE FOR dbo.PickingStaffKey PickingStaffKey,
        stg.FullName ,
    stg.PreferredName ,
    stg.LogonName ,
    stg.CustomFields ,
    stg.PhoneNumber ,
    stg.FaxNumber ,
    stg.EmailAddress ,
    @StartDate ,
    NULL
    FROM dbo.PickingStaff_Stage stg
    JOIN dbo.DimPickingStaff ps
        ON stg.LogonName = ps.LogonName
        AND ps.EndDate IS NULL
    WHERE stg.FullName <> ps.FullName
          OR stg.PreferredName <> ps.PreferredName
          OR stg.CustomFields <> ps.CustomFields
          OR stg.PhoneNumber <> ps.PhoneNumber
          OR stg.FaxNumber <> ps.FaxNumber
          OR stg.EmailAddress <> ps.EmailAddress
          ;

    INSERT INTO dbo.PickingStaff_Preload /* Column list excluded for brevity */
        ( PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate )
    SELECT dps.PickingStaffKey,
           dps.FullName,
           dps.PreferredName,
           dps.LogonName,
           dps.CustomFields,
           dps.PhoneNumber,
           dps.FaxNumber,
           dps.EmailAddress,
           dps.StartDate,
           CASE 
               WHEN pl.LogonName IS NULL THEN NULL
               ELSE @EndDate
           END AS EndDate
    FROM dbo.DimPickingStaff dps
    LEFT JOIN dbo.PickingStaff_Preload pl    
        ON pl.LogonName = dps.LogonName
        AND dps.EndDate IS NULL;
    
    INSERT INTO dbo.PickingStaff_Preload /* Column list excluded for brevity */
        ( PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate )
    SELECT NEXT VALUE FOR dbo.PickingStaffKey AS PickingStaffKey,
           stg.FullName,
           stg.PreferredName,
           stg.LogonName,
           stg.CustomFields,
           stg.PhoneNumber,
           stg.FaxNumber,
           stg.EmailAddress,
           @StartDate,
           NULL
    FROM dbo.PickingStaff_Stage stg
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimPickingStaff cu WHERE stg.LogonName = cu.LogonName );

    INSERT INTO dbo.PickingStaff_Preload /* Column list excluded for brevity */
        (PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate)
    SELECT cu.PickingStaffKey,
           cu.FullName,
           cu.PreferredName,
           cu.LogonName,
           cu.CustomFields,
           cu.PhoneNumber,
           cu.FaxNumber,
           cu.EmailAddress,
           cu.StartDate,
           @EndDate
    FROM dbo.DimPickingStaff cu
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.PickingStaff_Stage stg WHERE stg.LogonName = cu.LogonName )
          AND cu.EndDate IS NULL;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.PickingStaff_Transform '2013-01-01', '2012-12-31';
GO
 

CREATE TABLE dbo.Orders_Preload (
CustomerKey INT NOT NULL,
    CityKey INT NOT NULL,
    ProductKey INT NOT NULL,
    SalespersonKey INT NOT NULL,
    PickingStaffKey INT NOT NULL,
    
    DateKey INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18, 2) NOT NULL,
    TaxRate DECIMAL(18, 3) NOT NULL,
    TotalBeforeTax DECIMAL(18, 2) NOT NULL,
    TotalAfterTax DECIMAL(18, 2) NOT NULL
);
GO
CREATE PROCEDURE dbo.Orders_Transform
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Orders_Preload;

    INSERT INTO dbo.Orders_Preload /* Columns excluded for brevity */
     ( CustomerKey ,
    CityKey ,
    ProductKey ,
    SalespersonKey ,
    PickingStaffKey ,
    
    DateKey ,
    Quantity ,
    UnitPrice ,
    TaxRate ,
    TotalBeforeTax ,
    TotalAfterTax )
    SELECT cu.CustomerKey,
           ci.CityKey,
           pr.ProductKey,
           sp.SalespersonKey,
           psp.PickingStaffKey,
         
           CAST(YEAR(ord.OrderDate) * 10000 + MONTH(ord.OrderDate) * 100 + DAY(ord.OrderDate) AS INT) AS DateKey,
           (ord.Quantity) AS Quantity,
           (ord.UnitPrice) AS UnitPrice,
           (ord.TaxRate) AS TaxRate,
           (ord.Quantity * ord.UnitPrice) AS TotalBeforeTax,
           (ord.Quantity * ord.UnitPrice * (1 + ord.TaxRate/100)) AS TotalAfterTax
    FROM dbo.Orders_Stage ord
    JOIN dbo.Customers_Preload cu
        ON ord.CustomerName = cu.CustomerName
    JOIN dbo.Cities_Preload ci
        ON ord.CityName = ci.CityName
        AND ord.StateProvinceName = ci.StateProvName
        AND ord.CountryName = ci.CountryName
    JOIN dbo.Products_Preload pr
        ON ord.StockItemName = pr.ProductName
    JOIN dbo.SalesPeople_Preload sp
        ON ord.LogonName = sp.LogonName
    JOIN dbo.PickingStaff_Preload psp
        ON ord.PickingStaffName = psp.LogonName
    
    ;
END;
GO
Execute dbo.Orders_Transform;
GO

CREATE PROCEDURE dbo.Customers_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    DELETE cu
    FROM dbo.DimCustomers cu
    JOIN dbo.Customers_Preload pl
        ON cu.CustomerKey = pl.CustomerKey;

    INSERT INTO dbo.DimCustomers /* Columns excluded for brevity */
      ( CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate )
        
    SELECT  /* Columns excluded for brevity */
      CustomerKey ,
    CustomerName ,
    CustomerCategoryName ,
    DeliveryCityName ,
    DeliveryStateProvCode ,
    DeliveryCountryName ,
    PostalCityName ,
    PostalStateProvCode ,
    PostalCountryName ,
    StartDate ,
    EndDate 
    FROM dbo.Customers_Preload;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.Customers_Load;
GO

CREATE PROCEDURE dbo.Products_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    DELETE cu
    FROM dbo.DimProducts cu
    JOIN dbo.Products_Preload pl
        ON cu.ProductKey = pl.ProductKey;

    INSERT INTO dbo.DimProducts /* Columns excluded for brevity */
       (ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate)
        
    SELECT  /* Columns excluded for brevity */
      ProductKey ,
    ProductName ,
    ProductColour ,
    ProductBrand ,
    ProductSize ,
    StartDate ,
    EndDate
    FROM dbo.Products_Preload;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.Products_Load;
GO

CREATE PROCEDURE dbo.Salespeoples_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    DELETE cu
    FROM dbo.DimSalespeople cu
    JOIN dbo.Salespeople_Preload pl
        ON cu.SalespersonKey = pl.SalespersonKey;

    INSERT INTO dbo.DimSalespeople /* Columns excluded for brevity */
        (SalespersonKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress )
        
    SELECT  /* Columns excluded for brevity */
       SalespersonKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress  
    FROM dbo.Salespeople_Preload;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.SalesPeoples_Load;
GO

CREATE PROCEDURE dbo.Cities_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    DELETE cu
    FROM dbo.DimCities cu
    JOIN dbo.Cities_Preload pl
        ON cu.CityKey = pl.CityKey;

    INSERT INTO dbo.DimCities /* Columns excluded for brevity */
        (CityKey,
        CityName,
       StateProvCode,
       StateProvName,
       CountryName,
       CountryFormalName)
        
    SELECT  /* Columns excluded for brevity */
       CityKey,
        CityName,
       StateProvCode,
       StateProvName,
       CountryName,
       CountryFormalName 
    FROM dbo.Cities_Preload;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.Cities_Load;
GO

CREATE PROCEDURE dbo.PickingStaff_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    DELETE cu
    FROM dbo.DimPickingStaff cu
    JOIN dbo.PickingStaff_Preload pl
        ON cu.PickingStaffKey = pl.PickingStaffKey;

    INSERT INTO dbo.DimPickingStaff /* Columns excluded for brevity */
       (PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate)
        
    SELECT  /* Columns excluded for brevity */
      PickingStaffKey ,
    FullName ,
    PreferredName ,
    LogonName ,
    CustomFields ,
    PhoneNumber ,
    FaxNumber ,
    EmailAddress ,
    StartDate ,
    EndDate
    FROM dbo.PickingStaff_Preload;

    COMMIT TRANSACTION;
END;
GO
EXECUTE dbo.PickingStaff_Load;
GO

CREATE PROCEDURE dbo.Orders_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    INSERT INTO dbo.FactOrders /* Columns excluded for brevity */
    SELECT * /* Columns excluded for brevity */
    FROM dbo.Orders_Preload;

END;

GO
EXECUTE dbo.Orders_Load;
GO
Execute dbo.Customers_Extract;
GO
Execute dbo.Products_Extract;
GO
Execute dbo.SalesPeople_Extract;
GO

Execute dbo.DimDate_Load '2013-01-02';
GO
Execute dbo.Orders_Extract @OrdersDate = '2013-01-02';
GO
Execute dbo.Orders_Transform;
GO
EXECUTE dbo.Orders_Load;
GO

Execute dbo.DimDate_Load '2013-01-03';
GO
Execute dbo.Orders_Extract @OrdersDate = '2013-01-03';
GO
Execute dbo.Orders_Transform;
GO
EXECUTE dbo.Orders_Load;
GO

Execute dbo.DimDate_Load '2013-01-04';
GO
Execute dbo.Orders_Extract @OrdersDate = '2013-01-04';
GO
Execute dbo.Orders_Transform;
GO
EXECUTE dbo.Orders_Load;
GO

Execute dbo.Cities_Transform;
GO
Execute dbo.Salespeople_Transform;
GO

EXECUTE dbo.Customers_Transform '2013-01-02', '2013-01-01';
GO
EXECUTE dbo.Products_Transform '2013-01-02', '2013-01-01';
GO
EXECUTE dbo.Customers_Load;
GO
EXECUTE dbo.Products_Load;
GO

EXECUTE dbo.SalesPeoples_Load;
GO
EXECUTE dbo.Cities_Load;
GO     

 
SELECT * FROM DimDate;
SELECT * FROM DimProducts;
SELECT * FROM FactOrders

