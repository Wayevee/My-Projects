
use PotfolioProject
go
select location, date, total_cases, new_cases, total_deaths, population, cast(DATEPART(month, date) as varchar) as month
from CovidDeaths
order by 1,2;
--=================================
select location, date, total_cases, total_deaths,(total_deaths/total_cases) * 100 as diff, sum(total_deaths) over(partition by location)
from CovidDeaths
--where location like '%states%'
order by 1,2; 


select location, date, total_cases, population,(total_cases/population ) * 100 as diff -- ,sum(total_deaths) over(partition by location)
from CovidDeaths
--where location like '%states%'
order by 1,2; 
 

 select max(total_cases) as most_infected, location, population , max((total_cases/population)) * 100 as percenttotal
  from CovidDeaths
  group by location, population
  order by percenttotal desc

   select max( cast(total_deaths as int)) as most_death, location
  from CovidDeaths
    where location is not null
  group by location, population, total_deaths
  order by most_death 

select max(cast(total_deaths as int)) as most_death, location
  from CovidDeaths
    where continent is  null
  group by location
  order by most_death desc;

select  date, total_cases ,total_deaths,(total_deaths/total_cases ) * 100 as diff--,COUNT(total_cases) ,sum(total_deaths) over(partition by location)
from CovidDeaths
--where location like '%states%'
where total_cases is not null and total_deaths is not null
--group by   date, total_cases ,total_deaths
order by 1; 
 
 SELECT sum(cast(total_cases as int)),  SUM(cast(total_deaths_per_million as int))--,sum(cast(d.total_vaccinations as int))
 from coviddeaths d
 join CovidVaccinations v on d.iso_code = v.iso_code
 where total_cases is not null and d.total_vaccinations is not null and total_deaths_per_million is not null;

 SELECT  SUM(new_cases) as totalcase, SUM(CAST(new_deaths as int)) as totaldeath, SUM(CAST(new_deaths as int))/SUM(new_cases) * 100 as deathpercentage
 from CovidDeaths
 WHERE continent is not null
-- GROUP by [date]--,new_cases,new_deaths
 order by 1; 

 select d.continent, d.[location], d.date,[population], v.new_vaccinations, SUM(cast(v.new_vaccinations as int)) over(partition by d.location order by d.date, d.location) as rolling_vaccination_count
 FROM PotfolioProject..CovidDeaths d
 inner JOIN PotfolioProject..CovidVaccinations v on   d.[location] = v.[location] and d.[date] = v.[date]
 --where d.location ='nigeria'
 where d.continent is not null
order by 2,3

with popvsvac (continent, location,date,population,new_vaccinations,rolling_vaccination_count)
as 
( select d.continent, d.[location], d.date,[population], v.new_vaccinations, SUM(cast(v.new_vaccinations as int)) over(partition by d.location order by d.date) as rolling_vaccination_count
 FROM PotfolioProject..CovidDeaths d
 inner JOIN PotfolioProject..CovidVaccinations v on   d.[location] = v.[location] and d.[date] = v.[date]
 --where d.location ='nigeria'
 where d.continent is not null
--order by 2,3
)
select * , (rolling_vaccination_count/population) * 100  as rolling_count_percentage
from popvsvac


Drop table if EXISTS popvsvar
Create table popvsvar
(
  continent NVARCHAR(255),
  LOCATION NVARCHAR(255),
  date datetime, 
  population numeric,
  New_vaccination numeric,
  rolling_vaccination_count numeric
)

INSERT into popvsvar
 select d.continent, d.[location], d.date,[population], v.new_vaccinations, SUM(cast(v.new_vaccinations as int)) over(partition by d.location order by d.date) as rolling_vaccination_count
 FROM PotfolioProject..CovidDeaths d
 inner JOIN PotfolioProject..CovidVaccinations v on   d.[location] = v.[location] and d.[date] = v.[date]
 --where d.location ='nigeria'
 --where d.continent is not null
--order by 2,3
select * , (rolling_vaccination_count/population) * 100  as rolling_count_percentage
from popvsvar


 SELECT  SUM(new_cases) as totalcase, SUM(CAST(new_deaths as int)) as totaldeath, SUM(CAST(new_deaths as int))/SUM(new_cases) * 100 as deathpercentage
 from CovidDeaths
 WHERE continent is not null
-- GROUP by [date]--,new_cases,new_deaths
 order by 1,2; 

 Create VIEW popvar as
 select d.continent, d.[location], d.date,[population], v.new_vaccinations, SUM(cast(v.new_vaccinations as int)) over(partition by d.location order by d.date) as rolling_vaccination_count
 FROM PotfolioProject..CovidDeaths d
 inner JOIN PotfolioProject..CovidVaccinations v on   d.[location] = v.[location] and d.[date] = v.[date]
 --where d.location ='nigeria'
 where d.continent is not null