INSERT INTO [dbo].[AggregateSales] SELECT [SalesAmount],[PostalCode],[CustomerIncome],[OrderDate] FROM [dbo].[AggregateSales_External]
INSERT INTO [dbo].[DatabaseLog] SELECT [DatabaseLogID],[PostTime],[DatabaseUser],[Event],[Schema],[Object],[TSQL] FROM [dbo].[DatabaseLog_External]
INSERT INTO [dbo].[DimAccount] SELECT [AccountKey],[ParentAccountKey],[AccountCodeAlternateKey],[ParentAccountCodeAlternateKey],[AccountDescription],[AccountType],[Operator],[CustomMembers],[ValueType],[CustomMemberOptions] FROM [dbo].[DimAccount_External]
INSERT INTO [dbo].[DimCurrency] SELECT [CurrencyKey],[CurrencyAlternateKey],[CurrencyName] FROM [dbo].[DimCurrency_External]
INSERT INTO [dbo].[DimCustomer] SELECT [CustomerKey],[GeographyKey],[CustomerAlternateKey],[Title],[FirstName],[MiddleName],[LastName],[NameStyle],[BirthDate],[MaritalStatus],[Suffix],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],[EnglishEducation],[SpanishEducation],[FrenchEducation],[EnglishOccupation],[SpanishOccupation],[FrenchOccupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],[AddressLine2],[Phone],[DateFirstPurchase],[CommuteDistance] FROM [dbo].[DimCustomer_External]
INSERT INTO [dbo].[DimDate] SELECT [DateKey],[FullDateAlternateKey],[DayNumberOfWeek],[EnglishDayNameOfWeek],[SpanishDayNameOfWeek],[FrenchDayNameOfWeek],[DayNumberOfMonth],[DayNumberOfYear],[WeekNumberOfYear],[EnglishMonthName],[SpanishMonthName],[FrenchMonthName],[MonthNumberOfYear],[CalendarQuarter],[CalendarYear],[CalendarSemester],[FiscalQuarter],[FiscalYear],[FiscalSemester] FROM [dbo].[DimDate_External]
INSERT INTO [dbo].[DimDepartmentGroup] SELECT [DepartmentGroupKey],[ParentDepartmentGroupKey],[DepartmentGroupName] FROM [dbo].[DimDepartmentGroup_External]
INSERT INTO [dbo].[DimEmployee] SELECT [EmployeeKey],[ParentEmployeeKey],[EmployeeNationalIDAlternateKey],[ParentEmployeeNationalIDAlternateKey],[SalesTerritoryKey],[FirstName],[LastName],[MiddleName],[NameStyle],[Title],[HireDate],[BirthDate],[LoginID],[EmailAddress],[Phone],[MaritalStatus],[EmergencyContactName],[EmergencyContactPhone],[SalariedFlag],[Gender],[PayFrequency],[BaseRate],[VacationHours],[SickLeaveHours],[CurrentFlag],[SalesPersonFlag],[DepartmentName],[StartDate],[EndDate],[Status] FROM [dbo].[DimEmployee_External]
INSERT INTO [dbo].[DimGeography] SELECT [GeographyKey],[City],[StateProvinceCode],[StateProvinceName],[CountryRegionCode],[EnglishCountryRegionName],[SpanishCountryRegionName],[FrenchCountryRegionName],[PostalCode],[SalesTerritoryKey] FROM [dbo].[DimGeography_External]
INSERT INTO [dbo].[DimOrganization] SELECT [OrganizationKey],[ParentOrganizationKey],[PercentageOfOwnership],[OrganizationName],[CurrencyKey] FROM [dbo].[DimOrganization_External]
INSERT INTO [dbo].[DimProduct] SELECT [ProductKey],[ProductAlternateKey],[ProductSubcategoryKey],[WeightUnitMeasureCode],[SizeUnitMeasureCode],[EnglishProductName],[SpanishProductName],[FrenchProductName],[StandardCost],[FinishedGoodsFlag],[Color],[SafetyStockLevel],[ReorderPoint],[ListPrice],[Size],[SizeRange],[Weight],[DaysToManufacture],[ProductLine],[DealerPrice],[Class],[Style],[ModelName],[EnglishDescription],[FrenchDescription],[ChineseDescription],[ArabicDescription],[HebrewDescription],[ThaiDescription],[GermanDescription],[JapaneseDescription],[TurkishDescription],[StartDate],[EndDate],[Status] FROM [dbo].[DimProduct_External]
INSERT INTO [dbo].[DimProductCategory] SELECT [ProductCategoryKey],[ProductCategoryAlternateKey],[EnglishProductCategoryName],[SpanishProductCategoryName],[FrenchProductCategoryName] FROM [dbo].[DimProductCategory_External]
INSERT INTO [dbo].[DimProductSubcategory] SELECT [ProductSubcategoryKey],[ProductSubcategoryAlternateKey],[EnglishProductSubcategoryName],[SpanishProductSubcategoryName],[FrenchProductSubcategoryName],[ProductCategoryKey] FROM [dbo].[DimProductSubcategory_External]
INSERT INTO [dbo].[DimPromotion] SELECT [PromotionKey],[PromotionAlternateKey],[EnglishPromotionName],[SpanishPromotionName],[FrenchPromotionName],[DiscountPct],[EnglishPromotionType],[SpanishPromotionType],[FrenchPromotionType],[EnglishPromotionCategory],[SpanishPromotionCategory],[FrenchPromotionCategory],[StartDate],[EndDate],[MinQty],[MaxQty] FROM [dbo].[DimPromotion_External]
INSERT INTO [dbo].[DimReseller] SELECT [ResellerKey],[GeographyKey],[ResellerAlternateKey],[Phone],[BusinessType],[ResellerName],[NumberEmployees],[OrderFrequency],[OrderMonth],[FirstOrderYear],[LastOrderYear],[ProductLine],[AddressLine1],[AddressLine2],[AnnualSales],[BankName],[MinPaymentType],[MinPaymentAmount],[AnnualRevenue],[YearOpened] FROM [dbo].[DimReseller_External]
INSERT INTO [dbo].[DimSalesReason] SELECT [SalesReasonKey],[SalesReasonAlternateKey],[SalesReasonName],[SalesReasonReasonType] FROM [dbo].[DimSalesReason_External]
INSERT INTO [dbo].[DimSalesTerritory] SELECT [SalesTerritoryKey],[SalesTerritoryAlternateKey],[SalesTerritoryRegion],[SalesTerritoryCountry],[SalesTerritoryGroup] FROM [dbo].[DimSalesTerritory_External]
INSERT INTO [dbo].[DimScenario] SELECT [ScenarioKey],[ScenarioName] FROM [dbo].[DimScenario_External]
INSERT INTO [dbo].[FactCallCenter] SELECT [FactCallCenterID],[DateKey],[WageType],[Shift],[LevelOneOperators],[LevelTwoOperators],[TotalOperators],[Calls],[AutomaticResponses],[Orders],[IssuesRaised],[AverageTimePerIssue],[ServiceGrade] FROM [dbo].[FactCallCenter_External]
INSERT INTO [dbo].[FactCurrencyRate] SELECT [CurrencyKey],[DateKey],[AverageRate],[EndOfDayRate] FROM [dbo].[FactCurrencyRate_External]
INSERT INTO [dbo].[FactFinance] SELECT [FinanceKey],[DateKey],[OrganizationKey],[DepartmentGroupKey],[ScenarioKey],[AccountKey],[Amount] FROM [dbo].[FactFinance_External]
INSERT INTO [dbo].[FactInternetSales] SELECT [ProductKey],[OrderDateKey],[DueDateKey],[ShipDateKey],[CustomerKey],[PromotionKey],[CurrencyKey],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],[RevisionNumber],[OrderQuantity],[UnitPrice],[ExtendedAmount],[UnitPriceDiscountPct],[DiscountAmount],[ProductStandardCost],[TotalProductCost],[SalesAmount],[TaxAmt],[Freight],[CarrierTrackingNumber],[CustomerPONumber] FROM [dbo].[FactInternetSales_External]
INSERT INTO [dbo].[FactInternetSalesReason] SELECT [SalesOrderNumber],[SalesOrderLineNumber],[SalesReasonKey] FROM [dbo].[FactInternetSalesReason_External]
INSERT INTO [dbo].[FactResellerSales] SELECT [ProductKey],[OrderDateKey],[DueDateKey],[ShipDateKey],[ResellerKey],[EmployeeKey],[PromotionKey],[CurrencyKey],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],[RevisionNumber],[OrderQuantity],[UnitPrice],[ExtendedAmount],[UnitPriceDiscountPct],[DiscountAmount],[ProductStandardCost],[TotalProductCost],[SalesAmount],[TaxAmt],[Freight],[CarrierTrackingNumber],[CustomerPONumber] FROM [dbo].[FactResellerSales_External]
INSERT INTO [dbo].[FactSalesQuota] SELECT [SalesQuotaKey],[EmployeeKey],[DateKey],[CalendarYear],[CalendarQuarter],[SalesAmountQuota] FROM [dbo].[FactSalesQuota_External]
INSERT INTO [dbo].[FactSurveyResponse] SELECT [SurveyResponseKey],[DateKey],[CustomerKey],[ProductCategoryKey],[EnglishProductCategoryName],[ProductSubcategoryKey],[EnglishProductSubcategoryName] FROM [dbo].[FactSurveyResponse_External]
INSERT INTO [dbo].[ProspectiveBuyer] SELECT [ProspectiveBuyerKey],[ProspectAlternateKey],[FirstName],[MiddleName],[LastName],[BirthDate],[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],[AddressLine2],[City],[StateProvinceCode],[PostalCode],[Phone],[Salutation],[Unknown] FROM [dbo].[ProspectiveBuyer_External]
INSERT INTO [dbo].[SalesByCategory] SELECT [SalesAmount],[ProductLine] FROM [dbo].[SalesByCategory_External]
INSERT INTO [dbo].[SalesByCustomer] SELECT [SalesAmount],[Gender],[NumberCarsOwned],[CustomerYearlyIncome],[TotalChildren] FROM [dbo].[SalesByCustomer_External]
INSERT INTO [dbo].[SalesByDate] SELECT [SalesAmount],[EnglishMonthName],[CalendarYear] FROM [dbo].[SalesByDate_External]
INSERT INTO [dbo].[SalesByRegion] SELECT [SalesAmount],[PostalCode],[StateProvinceCode] FROM [dbo].[SalesByRegion_External]
INSERT INTO [dbo].[vDMPrep] SELECT [EnglishProductCategoryName],[Model],[CustomerKey],[Region],[Age],[IncomeGroup],[CalendarYear],[FiscalYear],[Month],[OrderNumber],[LineNumber],[Quantity],[Amount] FROM [dbo].[vDMPrep_External]
INSERT INTO [dbo].[vTargetMail] SELECT [CustomerKey],[GeographyKey],[CustomerAlternateKey],[Title],[FirstName],[MiddleName],[LastName],[NameStyle],[BirthDate],[MaritalStatus],[Suffix],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],[EnglishEducation],[SpanishEducation],[FrenchEducation],[EnglishOccupation],[SpanishOccupation],[FrenchOccupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],[AddressLine2],[Phone],[DateFirstPurchase],[CommuteDistance],[Region],[Age],[BikeBuyer] FROM [dbo].[vTargetMail_External]




--These tables are used by an SSIS package later in the lab
CREATE TABLE dbo.FactInternetSales_STAGE
(
  ProductKey INT NOT NULL,
  CustomerKey INT NOT NULL,
  OrderDateKey INT NOT NULL,
  OrderNumber INT NOT NULL,
  OrderQty SMALLINT NOT NULL,
  SalesAmt MONEY NOT NULL
)
WITH
(
  CLUSTERED COLUMNSTORE INDEX,
  DISTRIBUTION = ROUND_ROBIN
);

CREATE TABLE dbo.FactResellerSales_STAGE
(
  ProductKey INT NOT NULL,
  CustomerKey INT NOT NULL,
  OrderDateKey INT NOT NULL,
  OrderNumber INT NOT NULL,
  SalesPersonKey INT NULL,
  OrderQty SMALLINT NOT NULL,
  SalesAmt MONEY NOT NULL
)
WITH
(
  CLUSTERED COLUMNSTORE INDEX,
  DISTRIBUTION = ROUND_ROBIN
);