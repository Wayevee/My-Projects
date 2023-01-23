use [AdventureWorks2017]
go

select top 10 *
from Person.Person;

select 
row_number()over(order by businessentityid) as rownum
,RANK()over(order by businessentityid) as rank
,DENSE_RANK()over(order by businessentityid) as dense_rank
,JobTitle
from HumanResources.Employee
Order by rownum;

select * 
from person.person
order by lastname;

select FirstName,LastName,BusinessEntityID as Employee_id
from Person.Person
order by LastName;

select ProductID, ProductNumber, Name
from Production.Product
where ProductLine = 'T' AND SellStartDate is not null
order by name; 

SELECT SalesOrderID, CustomerID, OrderDate, SubTotal,TaxAmt/SubTotal * 100 as per_centage
from Sales.SalesOrderHeader
order by SubTotal desc ;

select distinct jobtitle
from HumanResources.Employee
order by JobTitle;
 
 select SUM(freight) as total_freight, customerid
 from sales.SalesOrderHeader
 GROUP by CustomerID
 ORDER by CustomerID;

 select AVG(subtotal) as average_subtotla
 , SUM(subtotal) as sum_subtotal
 , customerid
 , SalesPersonID
 from sales.SalesOrderHeader
 group by CustomerID, SalesPersonID
 order by CustomerID desc;

 select productid, SUM(quantity) AS SUM_OF_Quantity
 from Production.ProductInventory
 where Shelf in('A','C','H') AND Quantity <= 500
 GROUP BY ProductID
 ORDER BY ProductID;

select SUM(quantity)  as total
from Production.ProductInventory
group by (LocationID * 10);


select p.BusinessEntityID, p.FirstName, p.LastName, ph.PhoneNumber
from person.person as p 
join person.PersonPhone as Ph 
on p.BusinessEntityID = ph.BusinessEntityID
where LastName like 'l%'
order by LastName , firstName ;

select SUM(subtotal) as sum_subtotal, SalesPersonID , customerid
from sales.SalesOrderHeader
where SalesPersonID is not null
GROUP by Rollup(SalesPersonID, CustomerID);

select SUM(quantity) as sum_quantity, locationid, shelf 
from Production.ProductInventory
group by rollup(locationid, shelf)
order by LocationID;

select SUM(quantity) as Total_quantity, locationid, shelf
from Production.ProductInventory
GROUP  by LocationID, Shelf;

select locationid, SUM(quantity)
FROm Production.ProductInventory
GROUP by LocationID;

select count(businessentityid)over(partition by city ), city
from person.BusinessEntityAddress ea
join Person.Address ad
on ea.AddressID = ad.AddressID;

select distinct year(orderdate), sum(TotalDue)
from Sales.SalesOrderHeader
group by year(orderdate)
order by year(orderdate);

select year(orderdate), sum(totaldue)
from Sales.SalesOrderHeader
where year(orderdate) <=  '2016'
GROUP BY year(orderdate)
order by year(orderdate);

select  name,ContactTypeID
from Person.ContactType
 where name like '%manager%'
 order by ContactTypeID desc;

 select pb. BusinessEntityID, p.FirstName, p.LastName
 from person.BusinessEntityContact pb 
 inner join person.ContactType pc 
 on pb.ContactTypeID = pc.ContactTypeID
 inner join person.person p 
 on p.BusinessEntityID = pb.PersonID
 where pc.name = 'Purchasing Manager'
 order by LastName, FirstName ;

 SELECT pp.BusinessEntityID, LastName, FirstName
    FROM Person.BusinessEntityContact AS pb 
        INNER JOIN Person.ContactType AS pc
            ON pc.ContactTypeID = pb.ContactTypeID
        INNER JOIN Person.Person AS pp
            ON pp.BusinessEntityID = pb.PersonID
    WHERE pc.Name = 'Purchasing Manager'
    ORDER BY LastName, FirstName;
use [AdventureWorks2017]
go
select ptc.contacttypeid, ptc.name, COUNT(pc.ContactTypeID)
from Person.BusinessEntityContact pc 
join Person.ContactType ptc 
on pc.ContactTypeID =ptc.ContactTypeID
group by  ptc.contacttypeid, ptc.name
order by COUNT(pc.ContactTypeID);

select CAST(p.RateChangeDate as date) as fromdate, CONCAT(pp.FirstName,' ', pp.MiddleName,' ', pp.LastName) as FullName, 40 * p.Rate as rate
from HumanResources.EmployeePayHistory p 
join person.person pp 
on p.BusinessEntityID = pp.BusinessEntityID
order by FullName;

SELECT
SUM(OrderQty) over(Partition by salesorderid)
,COUNT(OrderQty) over (Partition by salesorderid)
,MIN(OrderQty) over (Partition by salesorderid)
,MAX(OrderQty) over (Partition by salesorderid)
,AVG(OrderQty) over (Partition by salesorderid)
,salesorderid,productid,orderqty
from sales.SalesOrderDetail
where SalesOrderID  in (43659, 43664);

select SalesOrderID,ProductID,OrderQty, SUM(OrderQty) as total,AVG(OrderQty) as avg
--,COUNT(OrderQty) OVER(ORDER BY SalesOrderID, ProductID  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS Count
from sales.SalesOrderDetail
where SalesOrderID in (43659,43664) and productid like '71%'
group by SalesOrderID, SalesOrderID,ProductID,OrderQty;

select salesorderid, SUM(orderqty * unitprice)as cost
from sales.SalesOrderDetail
group by SalesOrderID
having SUM(unitprice * OrderQty) >= 100000;

select productid, name 
from Production.Product
where name like'lock washer%'
order by ProductID;

select productid, name, color 
from Production.Product
order by ListPrice;

select businessentityid, jobtitle, CAST(HireDate as varchar)
from HumanResources.Employee
order by year(hiredate);

select lastname, firstname
from person.Person
where lastname like 'r%'
order by FirstName, LastName desc;

select businessentityid, salariedflag
from HumanResources.Employee
order by case when salariedflag = 1 then businessentityid end desc,
        case when salariedflag = 0 then businessentityid END;

 select * from sales.vSalesPerson
 where TerritoryName is not null
 order by case when  countryregionname = 'united states' then TerritoryName
 else CountryRegionName end;

 select firstname, lastname, row_number()over(order by postalcode) as rownumber
 ,rank()over(order by postalcode) as rank
 ,dense_rank()over(order by postalcode) as denserank
 ,ntile(11)over(order by postalcode) as quiartile, SalesYTD,postalcode
 from Sales.SalesPerson s 
 join person.Person p 
 on s.BusinessEntityID = p.BusinessEntityID
 join person.Address a 
 on a.AddressID = s.BusinessEntityID
 where SalesYTD <> 0 and TerritoryID is not null
 order by postalcode;
--OFFSET USED TO SKIP A NUMBER OF ROWS
 select * FROM HumanResources.Department
 order by Departmentid offset 10 rows

 SELECT  * FROM HumanResources.Department
 ORDER BY DepartmentID
  OFFSET 5 ROWS
  FETCH NEXT 5 ROWS ONLY;

 SELECT NAME,COLOR ,LISTPRICE 
 FROM PRODUCTION.Product
 WHERE COLOR IN ('RED','BLUE')
 ORDER BY ListPrice;

 SELECT P.Name, SalesOrderID
 FROM Production.Product P 
LEFT JOIN SALES.SalesOrderDetail S 
 ON P.ProductID = S.ProductID
 ORDER BY P.Name;

SELECT name, SalesOrderID
 from production.product p
full outer join sales.salesorderdetail s
on p.productid = s.ProductID
order by Name;

select name ,BusinessEntityID
from sales.SalesTerritory s 
right join Sales.SalesPerson p 
on s.TerritoryID = p.TerritoryID;

select CONCAT(FirstName,' ',LastName) as fullname, city
from Person.Person p 
join HumanResources.Employee e 
on p.BusinessEntityID = e.BusinessEntityID
join (
    select City, BusinessEntityID
    from person.Address a 
    join person.BusinessEntityAddress b 
    on a.AddressID = b.AddressID) as z
    on z.BusinessEntityID = p.BusinessEntityID
order by lastname,FirstName;
--=====================================================================================================
select BusinessEntityID, FirstName,LastName
 from person.person 
 where PersonType like 'in%' and LastName like 'adams%'
 order by FirstName; 

 select BusinessEntityID, FirstName,LastName
 from (
     select * from Person.Person
     where PersonType like 'in%' 
 ) as derived
 where LastName like 'adams%'
 order by firstname;
 
 select BusinessEntityID, FirstName,LastName
 from person.Person
     where BusinessEntityID  <= 1500 
 and LastName like '%Al%' and FirstName like '%M%'
 order by firstname;

select  productid,p.Name, color 
from Production.Product p
inner join ( values('Blade'),('crown race'),('AWC Logo Cap')) as v(name)
on p.Name = v.name;

with Sales_CTE(SalesPersonID, salesorderid,salesyear)
as (
    select SalesPersonID, salesorderid,year(OrderDate) as salesyear
    from Sales.SalesOrderHeader
    where SalesPERSONID is not null
)
select SalesPersonID, COUNT(salesorderid),salesyear
from Sales_CTE
GROUP BY SalesPersonID,salesyear
ORDER BY SalesPersonID,salesyear;

--==== using Common Table Expressio CTE...=========
with average_total  (SubTotal, SalesPERSONID) as (
    select count(*) as subtotal, SalesPersonID
    from sales.SalesOrderHeader
     group by SalesPersonID
)
select sum(Subtotal), salespersonid
 from average_total
 group by SalesPERSONID;

 WITH AVG_CTE (sals, salespersonid) as
 (SELECT count(*) , salespersonid
 from sales.SalesOrderHeader
 where SalesPersonID is not null
 group by  salespersonid
 ) 
 select avg(sals) as total
 from AVG_CTE;
 --======================================================

 Select * from Production.ProductPhoto
 where LargePhotoFileName like '%green%';

 select a.AddressLine1, a.AddressLine2, a.PostalCode, p.CountryRegionCode, a.City
  from person.Address a 
 join person.StateProvince p 
 on a.StateProvinceID = p.StateProvinceID
 where CountryRegionCode not in ('US') and city like 'pa%';

select top 20 hiredate, jobtitle 
from HumanResources.Employee
order by HireDate desc;

select * 
from sales.SalesOrderHeader s 
join sales.SalesOrderDetail d 
on s.SalesOrderID = d.SalesOrderID
where (OrderQty > 5 or UnitPriceDiscount < 1000) and TotalDue > 100;

select name , color 
from Production.Product
where color like '%red%';

select name , listprice
from Production.Product
where ListPrice =80.99 and name like '%mountain%';

select name, Color
from Production.Product 
where name like '%mountain%' or name like'%road%';

select name , color 
from Production.Product
where  name like '%mountain%' and name like'%black%';

select * from Production.Product
where name like '%chain%' and name like 'chain%';

select * from Production.Product
where name like '%chain%' or name like '%full%';

select CONCAT(p.FirstName,'-',p.MiddleName,'-',p.LastName,'     &    ',e.EmailAddress) as fullinfo
from Person.Person P 
join person.EmailAddress e 
on p.BusinessEntityID =e.BusinessEntityID
where firstName ='ken' and MiddleName = 'j';

select City, STRING_AGG(cast (EmailAddress as varchar(7000)),' & ' ) AS emails
from Person.BusinessEntityAddress b 
inner join person.Address a 
on b.AddressID = a.AddressID
inner join Person.EmailAddress e 
on e.BusinessEntityID = b.BusinessEntityID
where city is not NULL
group by city;

select JobTitle, REPLACE(JobTitle,'production supervisor', 'Production Assistant')
from humanresources.Employee 
where JobTitle like '%supervisor%';

select p.FirstName, p.MiddleName,p.LastName,e.JobTitle
from Person.person p 
inner join humanresources.Employee e 
on p.BusinessEntityID = e.BusinessEntityID
where jobtitle like 'Sales%';

select CONCAT(FirstName,'   ', UPPER(lastname))
 from person.person
 order by lastName; 

 select  firstname, lastname, SUBSTRING(Title, 1, 25), CAST(SickLeaveHours as char(1))
 from HumanResources.Employee e 
 join person.person p 
 on e.BusinessEntityID = p.BusinessEntityID;

 select name, listprice
 from production.product
 where ListPrice like '33%';

 select SalesYTD, CommissionPct, cast(salesytd/CommissionPct as int) as computed
 from Sales.SalesPerson
 where CommissionPct != 0;

select p.FirstName, p.LastName,SalesYTD ,p.BusinessEntityID
from Person.Person p 
left join Sales.SalesPerson s
on p.BusinessEntityID =s.BusinessEntityID
where cast(cast(SalesYTD as int) as char(20)) like '2%';

select listprice,
case when name like 'Long-Sleeve Logo Jersey%' then CAST(name as char(16)) 
    else name end as 'names'
    from production.product
    where name like'Long-Sleeve Logo Jersey%';

select ProductID, UnitPrice, UnitPriceDiscount, cast(round(UnitPrice*UnitPriceDiscount,0) as int) as discountprice
from sales.SalesOrderDetail
where SalesOrderid = 46672
and UnitPriceDiscount > .02
order by ProductID;

select AVG(vacationhours) as 'average vacation hours' , SUM(sickleavehours) as 'total sick leave hours'
from HumanResources.Employee
where jobtitle like 'vice president%';

select TerritoryID, AVG(bonus) as 'average bonus', sum(SalesYTD) as 'YTD sales'
from Sales.SalesPerson
GROUP by TerritoryID;

select  AVG(distinct listprice) as 'average listprice'
from Production.Product;

SELECT BusinessEntityID, TerritoryID   
   ,YEAR(ModifiedDate) AS SalesYear  
   ,cast(SalesYTD as VARCHAR(20)) AS  SalesYTD  
   ,AVG(SalesYTD) OVER (PARTITION BY TerritoryID )--ORDER BY YEAR(ModifiedDate)) AS MovingAvg  
   ,SUM(SalesYTD) OVER (PARTITION BY TerritoryID) --ORDER BY YEAR(ModifiedDate)) AS CumulativeTotal  
FROM Sales.SalesPerson  
WHERE TerritoryID IS NULL OR TerritoryID < 5  
ORDER BY TerritoryID,SalesYear;

select businessentityid, territoryid, year(ModifiedDate) as saledate, SalesYTD, 
--AVG(SalesYTD) as 'average saleYTD', 
AVG(SalesYTD)OVER(partition by territoryid order by year(ModifiedDate)) as Movingavg, 
sum(SalesYTD)OVER(partition by territoryid order by year(ModifiedDate))
from sales.salesperson
--group by BusinessEntityID, TerritoryID, year(ModifiedDate)
order by TerritoryID, year(ModifiedDate);

select count(distinct jobtitle)
from HumanResources.Employee
 where JobTitle is not null;

 select count(distinct BusinessEntityID)
from HumanResources.Employee
 where BusinessEntityID is not null;

 select COUNT(businessentityid), avg(bonus)
 from Sales.SalesPerson
 where SalesQuota >= 25000;

 select Name, MIN(p.Rate), MAX(Rate), AVG(Rate),COUNT(p.BusinessEntityID)
 from HumanResources.EmployeePayHistory p 
 join HumanResources.EmployeeDepartmentHistory d 
 on p.BusinessEntityID = d.BusinessEntityID
 join HumanResources.Department de 
 on de.DepartmentID = d.DepartmentID
 GROUP by Name;

 select jobtitle, COUNT(businessentityid)
 from HumanResources.Employee
 GROUP by JobTitle
 having COUNT(BusinessEntityID) >= 15;

select SalesOrderID, COUNT(ProductID)
from Sales.SalesOrderDetail
where salesorderid in('43855', '43661')
GROUP by SalesOrderID;

select VAR(salesquota)over(order by datepart(quarter, quotadate)) as variance,datepart(quarter, quotadate) as quater
,YEAR(quotadate) as year
from Sales.SalesPersonQuotaHistory
WHERE businessentityid = 277 AND YEAR(quotadate) = 2012 
--group by datepart(quarter, quotadate),YEAR(quotadate);

select VAR(distinct salesquota) as uniquevalue, VAR(salesquota) as duplicate
from Sales.SalesPersonQuotaHistory;

select color, sum(listprice) as listprice_sum, SUM(standardcost) as standardcost_sum
from Production.Product
where name like 'mountain%' and ListPrice  > 0 and color is not null
GROUP by color;

select SalesQuota, SUM(SalesYTD) as totalsalesytd, GROUPING(SalesQuota)
from Sales.SalesPerson
group by rollup(SalesQuota)
order by GROUPING(SalesQuota) desc;

select color, SUM(listprice), SUM(StandardCost)
from Production.Product
GROUP by Color;
--============================================================================================================================================================================================================
--============================================================================================================================================================================================================

select Department, LastName,Rate
,CUME_DIST()over(partition by department order by rate) as cumulative
,PERCENT_RANK()over(partition by department order by rate) as percentile
from HumanResources.vEmployeeDepartmentHistory h 
join HumanResources.EmployeePayHistory p 
on h.BusinessEntityID= p.BusinessEntityID
where Department is not NULL
order by department, rate desc;

select top 10 name, listprice, FIRST_VALUE(name)over(order by listprice asc) as leastExpensive
from Production.Product
where ProductSubcategoryID = 37;

select top 10 FIRST_VALUE(LastName)over(PARTITION by jobtitle order by vacationhours) as fewestvactionhours, JobTitle,LastName,VacationHours
from HumanResources.Employee e 
join person.person p 
on e.BusinessEntityID = p.BusinessEntityID;

select BusinessEntityID,YEAR(QuotaDate) as year, 
 LAG(SalesQuota, 1,0) OVER (ORDER BY YEAR(QuotaDate) ) AS PreviousQuota
from sales.SalesPersonQuotaHistory;

SELECT  top 10 TerritoryName, BusinessEntityID, SalesYTD,
LAG(SalesYTD,1,0)OVER(partition by TerritoryName order by SalesYTD)
from Sales.vSalesPerson
where TerritoryName is not null and TerritoryName in ('canada','northwest');

SELECT Department, LastName,Rate, HireDate,LAST_VALUE(HireDate)over(partition by department order by rate) as lastvalue
from HumanResources.vEmployeeDepartmentHistory d 
join HumanResources.EmployeePayHistory p 
on p.BusinessEntityID = d.BusinessEntityID
join HumanResources.Employee e
on e.BusinessEntityID = d.BusinessEntityID;

select distinct P.BusinessEntityID, DATEPART(quarter, P.modifieddate) as QUARTERLY, CONCAT(FirstName, '  ',LastName) AS NAME
, YEAR(P.ModifiedDate)
, SUM(RATE)OVER(Partition BY DATEPART(quarter, P.modifieddate) ORDER BY YEAR(P.ModifiedDate)) AS 'SALARY BI-QUARTERLY'
from Person.Person p 
 join HumanResources.EmployeePayHistory h 
 on p.BusinessEntityID = h.BusinessEntityID
 WHERE firstname like'ken%' 
 -- ORDER BY FirstName;

 select BusinessEntityID, datepart(quarter, QuotaDate) as Quarter, YEAR(QuotaDate) as salesyear,
 SalesQuota as quotathisquarter,SalesQuota - FIRST_VALUE(SalesQuota)OVER(partition by businessentityid,datepart(quarter, QuotaDate)order by datepart(year, QuotaDate)) as firstdiff,
  SalesQuota - last_VALUE(SalesQuota)OVER(partition by businessentityid,datepart(quarter, QuotaDate)order by datepart(year, QuotaDate)   RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as lastdiff
  , lag(salesquota)OVER(partition by businessentityid order by year(quotadate)) as previousquota
 from sales.SalesPersonQuotaHistory
  where year(quotadate) > 2005 and BusinessEntityID BETWEEN 274 and 275
  order by BusinessEntityID, salesyear,[Quarter];

select QuotaDate as year, datepart(quarter, QuotaDate) as Quarter, SalesQuota, VAR(SalesQuota)over(order by datepart(quarter, QuotaDate), year(QuotaDate) )as variance
from sales.SalesPersonQuotaHistory
--group by datepart(quarter, QuotaDate),SalesQuota,QuotaDate
where BusinessEntityID = 277 and year(QuotaDate) = 2012
order by datepart(quarter, QuotaDate)
;

select BusinessEntityID, year(QuotaDate) as year, SalesQuota as currentquota, lead(salesquota,1,0)over(order by year(QuotaDate))
from sales.SalesPersonQuotaHistory
where businessentityid = 277;

select TerritoryName, BusinessEntityID, SalesYTD, LEAD(SalesYTD,1,0)OVER(PARTITION by TerritoryName order by salesYTD)
from sales.vSalesPerson
where TerritoryName in ('canada','northwest');

select year(QuotaDate) as year, datepart(quarter, QuotaDate) as quarter, SalesQuota,
 lead(SalesQuota,1,0)over( order by year(QuotaDate), datepart(quarter, QuotaDate) ) AS NEXTvalue, 
SalesQuota- lead(SalesQuota,1,0)over( order by year(QuotaDate), datepart(quarter, QuotaDate) ) AS diff
 from sales.SalesPersonQuotaHistory
 where year(QuotaDate) BETWEEN 2012 and 2013 and BusinessEntityID =277;

 select Department, LastName, rate,round(CUME_DIST()over( partition by Department order by rate),2), round(PERCENT_RANK()over(partition by Department order by rate), 1)
  from HumanResources.vEmployeeDepartmentHistory d 
  join HumanResources.EmployeePayHistory p 
  on d.BusinessEntityID = p.BusinessEntityID
  order by Department, rate desc;
 
 select SalesOrderID, OrderDate, (OrderDate + 2)as promisedate
 from sales.SalesOrderHeader;

 select CONCAT(LastName,'  ', FirstName) as name , getdate(), getdate() + 2
 from Sales.SalesPerson p 
 inner join person.Person pp 
 on p.BusinessEntityID = pp.BusinessEntityID
 join Person.Address a 
 on a.AddressID = p.TerritoryID
  where SalesLastYear > 0;

  select concat(max(cast(OrderDate as int)) - min(cast(OrderDate as int)),'  ','days')
  from sales.SalesOrderHeader;

  select i.productid, name, LocationID, Quantity, rank()over(PARTITION by locationid order by quantity desc)
  from Production.ProductInventory i 
  JOIN Production.Product p 
  on i.ProductID = p.ProductID
  where locationid in ('3','4') order by Quantity desc;
;

select BusinessEntityID, Rate, rank()OVER(order by rate desc) 
from HumanResources.EmployeePayHistory
--where rate != 0
order by BusinessEntityID;

select ROW_NUMBER()over(order by salesytd desc) as rownumber,
FirstName,LastName,SalesYTD
from sales.vSalesPerson;

 with rownum
 as(
     select ROW_NUMBER()over(order by orderdate) as rownum, SalesOrderID, OrderDate
     from sales.SalesOrderHeader
 )
 select rownum, salesorderid, orderdate
 from rownum
 where rownum BETWEEN 50 and 60;

 select BusinessEntityID,LastName,TerritoryName,CountryRegionName
 from sales.vSalesPerson
 where TerritoryName is not null
 order by case when CountryRegionName = 'United States' then TerritoryName
 else CountryRegionName end ;

 select jobtitle, max(rate) as maxrate
     from HumanResources.Employee e 
 join HumanResources.EmployeePayHistory h 
 on e.BusinessEntityID = h.BusinessEntityID
 group by JobTitle
having (max( case when gender = 'M' THEN RATE ELSE NULL END) > 40 
OR
MAX(CASE WHEN GENDER = 'F' then rate else null end) > '40')
order by maxrate desc;

SELECT BusinessEntityID, SalariedFlag
 from HumanResources.Employee
 order by case when salariedflag = '0' then businessentityid end DESC,
            case when salariedflag = '1' then businessentityid end ;

 select ProductNumber, Name, ListPrice, 
 case when ListPrice <= 0 then 'Mfg item - not for resale'
        when ListPrice < 50 then 'under $50'
        when ListPrice < 100 then 'under $100'
         when ListPrice < 250 then 'under $250'
          when ListPrice < 1000 then 'under $1000'
          else 'over $1000' end as 'Price Range'
  from Production.Product
  order by ProductNumber;

  select ProductNumber, 
  case ProductLine
                when 'M' then 'Mountain'
                when 'R' then 'Road'
                when 'S' then   'Sale'
                when 'T' then 'Tight'
                else 'Not for Sale'
                end as 'Category', Name
  from Production.Product
   --where ProductLine is not NULL
   order by ProductNumber;

select ProductID, MakeFlag, FinishedGoodsFlag,
case when MakeFlag = FinishedGoodsFlag then null
else MakeFlag end as 'makeflag2'
 from Production.Product 
  where MakeFlag is not null
 and productid < 10;

 select name, class, ProductNumber,color,
coalesce(class,color, productnumber)
  from Production.Product;

  select ProductID, MakeFlag,FinishedGoodsFlag, 
  case when MakeFlag = FinishedGoodsFlag then null
  else MakeFlag end as 'Null if equal'
  from Production.Product
   where ProductID < 10;

  select ProductID from Production.Product
  intersect
  select  ProductID from Production.WorkOrder;

   select ProductID from Production.Product
 except
  select  ProductID from Production.WorkOrder;

 select ProductID from Production.WorkOrder
 except
  select  ProductID from Production.Product;

select businessentityid from person.BusinessEntity
INTERSECT
select businessentityid from person.person 
where PersonType = 'IN'
order by BusinessEntityid;


select businessentityid from person.BusinessEntity
except
select businessentityid from person.person 
where PersonType = 'IN'
order by BusinessEntityid;

select ProductModelID, name
from Production.ProductModel
union 
select ProductID,Name
from Production.Product
where ProductID not in(3,4)
order by name;

select CONCAT(FirstName,'  ',LastName), VacationHours,SickLeaveHours,VacationHours+SickLeaveHours as 'Total Hours Away'
from HumanResources.Employee e 
join person.person p 
on e.BusinessEntityID = p.BusinessEntityID
order by [Total Hours Away];

select MAX(taxrate) - MIN(taxrate) as 'Tax Rate Difference'
from Sales.SalesTaxRate;

select p.BusinessEntityID as salespersonid, CONCAT(FirstName,'  ',LastName),salesquota
, round(salesquota/12, 1) as 'Target per Month'
FROM Sales.SalesPerson P 
JOIN HumanResources.Employee E 
On p.BusinessEntityID = e.BusinessEntityID
join Person.Person pp 
on pp.BusinessEntityID = p.BusinessEntityID
where salesquota is not null;

SELECT ProductID, UnitPrice, OrderQty,  
   CAST(UnitPrice AS INT) % OrderQty AS Modulo  
FROM Sales.SalesOrderDetail;

select BusinessEntityID,LoginID, JobTitle,VacationHours
from HumanResources.Employee
where JobTitle like 'Marketing Assistant%' and VacationHours > 41 ;

select FirstName, LastName,Rate
from HumanResources.vEmployee e 
join HumanResources.EmployeePayHistory p 
on e.BusinessEntityID = p.BusinessEntityID
where rate not BETWEEN 27 and 30
order by rate ;

select BusinessEntityID, RateChangeDate
from HumanResources.EmployeePayHistory
where RateChangeDate between '20111212' and'20120105';

SELECT DepartmentID, Name   
FROM HumanResources.Department   
WHERE EXISTS (SELECT NULL)  
ORDER BY Name ;

select FirstName, LastName
from Person.Person p 
join HumanResources.employee e 
on p.BusinessEntityID = e.BusinessEntityID
where lastname like 'johnson%';

select case when s.name = p.name then s.name 
else null end as 'Names'
from sales.Store s 
join Purchasing.vendor  p 
on s.BusinessEntityId = p.BusinessEntityID;
--===================================================================================
select  FirstName,LastName,JobTitle
 from Person.Person p 
 join HumanResources.Employee e 
 on p.BusinessEntityID = e.BusinessEntityID
 join HumanResources.EmployeeDepartmentHistory h 
  on h.BusinessEntityID = p.BusinessEntityID
  where departmentid in (
      select DepartmentID
      from HumanResources.Department
      where name like 'P%'
  );

  select FirstName, LastName, JobTitle
  from Person.Person p 
  join HumanResources.Employee e 
  on p.BusinessEntityID = e.BusinessEntityID
  join HumanResources.EmployeeDepartmentHistory h 
  on h.BusinessEntityID = p.BusinessEntityID
  join HumanResources.Department d 
  on d.DepartmentID = h.DepartmentID
  where d.Name like 'P%';

  select  FirstName,LastName,JobTitle
 from Person.Person p 
 join HumanResources.Employee e 
 on p.BusinessEntityID = e.BusinessEntityID
 join HumanResources.EmployeeDepartmentHistory h 
  on h.BusinessEntityID = p.BusinessEntityID
  where departmentid in (
      select DepartmentID
      from HumanResources.Department
      where name not like 'P%'
  )
  order by LastName;

  select FirstName, LastName, JobTitle
  from Person.Person p 
  join HumanResources.Employee e 
  on p.BusinessEntityID = e.BusinessEntityID
  join HumanResources.EmployeeDepartmentHistory h 
  on h.BusinessEntityID = p.BusinessEntityID
  join HumanResources.Department d 
  on d.DepartmentID = h.DepartmentID
  where d.Name not like 'P%'
  order by LastName;
--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select FirstName,LastName,JobTitle
 from person.Person pp 
 join HumanResources.Employee e 
 on pp.BusinessEntityID = e.BusinessEntityID
 where JobTitle in('Design Engineer', 'tool designer', 'marketing assistant');
 
 select CONCAT(FirstName,'__', LastName) as FullName
 from person.person pp
 join Sales.SalesPerson sp 
 on pp.BusinessEntityID = sp.BusinessEntityID
 where SalesQuota > 250000;

  select CONCAT(FirstName,'__', LastName) as FullName
 from person.person pp
 join Sales.SalesPerson sp 
 on pp.BusinessEntityID = sp.BusinessEntityID
 where SalesQuota not in (
     select salesquota from Sales.SalesPerson
     where SalesQuota > 2500000
 );

select SalesOrderID, r.SalesReasonID,r.ModifiedDate
 from sales.SalesOrderHeaderSalesReason r 
 join sales.SalesReason sr 
  on r.SalesReasonID= sr.SalesReasonID
  where r.SalesReasonID= sr.SalesReasonID;

  select FirstName,LastName,PhoneNumber
  from person.Person pp 
  join person.PersonPhone ph 
  on pp.BusinessEntityID = ph.BusinessEntityID
   where PhoneNumber like  '415%'
   order by LastName;

  select FirstName,LastName,PhoneNumber
  from person.Person pp 
  join person.PersonPhone ph 
  on pp.BusinessEntityID = ph.BusinessEntityID
   where PhoneNumber not like  '415%' and FirstName = 'Gail '
   order by LastName;

   select ProductID, name, color, StandardCost
   from Production.Product
   where  ProductID in (
       select productid  from Production.Product
       where color like 'silver%' and StandardCost < 400
   )
   and name like 'mountain%';

   select FirstName,LastName,Shift
    from HumanResources.vEmployeeDepartmentHistory
    where shift in ('evening', 'night') and Department like 'Quality Assurance%';

  select  FirstName,LastName
  from Person.Person
  where FirstName like '_an'
  order by FirstName;

select distinct --SalesOrderID,OrderDate,
--OrderDate ::timestamp at time zone'America/Denver' as Denver_timezone,
year(OrderDate)as year , datepart(quarter, OrderDate) as Quarter, 
sum(SalesOrderID)over(partition by 2013, datepart(quarter, OrderDate)order by datepart(quarter, OrderDate))
from sales.SalesOrderHeader
order by  [year];

SELECT SalesOrderID, OrderDate,
    OrderDate ::timestamp at TIME ZONE 'europe/london' AS OrderDate_TimeZonePST  
FROM Sales.SalesOrderHeader;

select *
from Production.productphoto ph 
where LargePhotoFileName like '%green%';

select AddressLine1,AddressLine2,city,PostalCode,CountryRegionCode
from person.Address pa 
join person.StateProvince psp 
on pa.StateProvinceID =psp.StateProvinceID
where city like 'pa%' and PSP.CountryRegionCode NOT IN ('US');

 SELECT ProductID,n.name,color
 FROM Production.Product pp
 inner join (values ('blade'), ('crown race'), ('AWC logo cap')) as n(name)
 on pp.Name = n.name 
 --================================================================================================================
--using Common Table Expessions to get amt above and below
with salesorder (salespersonid, totalsales, yearsale)
as (
select SalespersonID, sum(TotalDue), year(OrderDate)
from sales.SalesOrderHeader
group by SalespersonID,year(OrderDate)
),
 salequota (businessentityid, salesquota, salesquotayear )
as (
    select BusinessEntityID, sum(SalesQuota), year(QuotaDate)
    from sales.SalesPersonQuotaHistory
    group by BusinessEntityID,year(QuotaDate
))
select salespersonid,cast(totalsales as varchar(10)),yearsale,cast(salesquota as varchar(10)),salesquotayear,
cast (totalsales - salesquota as int) as amt_above_or_below
from salesorder s 
join salequota q 
on s.salespersonid = q.businessentityid and s.yearsale = q.salesquotayear
order by salespersonid, yearsale;
--===================================================================================================================================

select BusinessEntityID,Name
from HumanResources.Employee e 
cross join HumanResources.Department d  
order by BusinessEntityID;

select SalesOrderID, p.ProductID, p.Name
from sales.SalesOrderDetail o 
join Production.Product p 
on o.ProductID = p.ProductID
--order by o.ProductID desc;

select SalesOrderID, p.ProductID, p.Name
from sales.SalesOrderDetail o 
join Production.Product p 
on o.ProductID = p.ProductID
where SalesOrderID > 60000
order by SalesOrderID;

select st.TerritoryID, SalesOrderID, CountryRegionCode
from sales.salesterritory st 
left join sales.salesorderheader h 
on st.territoryid = h.territoryid
order by SalesOrderID;

select st.TerritoryID, SalesOrderID, CountryRegionCode
from sales.salesterritory st 
left join sales.salesorderheader h 
on st.territoryid = h.territoryid
order by SalesOrderID;

select t.TerritoryID, SalesOrderID 
 from Sales.SalesTerritory t
cross join Sales.SalesOrderHeader
order by SalesOrderID;

select BusinessEntityID,JobTitle,BirthDate
 from HumanResources.Employee
 where BirthDate >'1970-01-01'
-- order by DAY(BirthDate)
 order by BirthDate;

  select BusinessEntityID,JobTitle,BirthDate
  from (
      select * from HumanResources.Employee
       where BirthDate >'1970-01-01'  
  ) as derived 
  where JobTitle = 'production Technician - wc40'  
  order by BirthDate;

  select BusinessEntityID,FirstName,MiddleName,LastName, PersonType
  from person.Person
  where FirstName != 'Adam'
  order by FirstName;

   select BusinessEntityID,FirstName,MiddleName,LastName, PersonType
  from person.Person
  where FirstName = 'Adam'
  order by FirstName;

     select BusinessEntityID,FirstName,MiddleName,LastName, PersonType
  from person.Person
  where MiddleName is not null
  order by FirstName;

       select BusinessEntityID,FirstName,MiddleName,LastName, PersonType
  from person.Person
  where MiddleName is  null
  order by FirstName;

  select name,Weight,Color
  from Production.Product
   where Weight < 10 or Color is null
   order by name;

   select BusinessEntityID, CAST(SalesYTD as varchar) as saleYTD, CAST(GETDATE() AS varchar)
   from sales.SalesPerson
   ORDER BY BusinessEntityID;

SELECT name, case 
when GROUPING_ID(name,JobTitle) = 0 then JobTitle
when GROUPING_ID(name,JobTitle) = 1 then CONCAT('total:', JobTitle)
when GROUPING_ID(name,JobTitle) = 3 then 'comapany issue'
else 'unknown' end as jobtitle,COUNT(e.BusinessEntityID) as employeecount
FROM HumanResources.Employee AS E  
    INNER JOIN HumanResources.EmployeeDepartmentHistory AS DH  
        ON E.BusinessEntityID = DH.BusinessEntityID  
    INNER JOIN HumanResources.Department AS D  
        ON D.DepartmentID = DH.DepartmentID       
WHERE DH.EndDate IS NULL  
    AND D.DepartmentID IN (12,14) 
    group by rollup( Name,JobTitle); 


SELECT D.Name  
    ,E.JobTitle  
    ,GROUPING_id(D.Name, E.JobTitle) AS "Grouping Level"  
    ,COUNT(E.BusinessEntityID) AS "Employee Count"  
FROM HumanResources.Employee AS E  
    INNER JOIN HumanResources.EmployeeDepartmentHistory AS DH  
        ON E.BusinessEntityID = DH.BusinessEntityID  
    INNER JOIN HumanResources.Department AS D  
        ON D.DepartmentID = DH.DepartmentID       
WHERE DH.EndDate IS NULL  
    AND D.DepartmentID IN (12,14)  
GROUP BY ROLLUP(D.Name, E.JobTitle)
HAVING GROUPING_ID(D.Name, E.JobTitle) = 0;

select year(QuotaDate) as year , DATEPART(QUARTER, QuotaDate) as quarter, SalesQuota as 'current' ,
 lag(SalesQuota)over(order by QuotaDate ) as previousquota
  ,SalesQuota -  lag(SalesQuota)over(order by QuotaDate ) as diff
from Sales.SalesPersonQuotaHistory
where BusinessEntityID = 277 and year(QuotaDate) in (2012,2013)
order by QuotaDate;

select DATEADD(MONTH,4,OrderDate), OrderDate
from Sales.SalesOrderHeader;

select SalesOrderID, OrderDate,SalesPersonID, CustomerID,
SubTotal, SUM(SubTotal)over(partition by customerid order by orderdate,salesorderid)as runningtotal
from Sales.SalesOrderHeader
where OrderDate >= 2011-12-01

select name, ProductNumber,CONCAT('0000',ProductNumber)as fullproductnumber
from Production.Product;
--========================================================================================================================================
  select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME like '%products%' 
  -- Finding a table with schema