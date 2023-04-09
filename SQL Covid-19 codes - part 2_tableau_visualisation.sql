use covid_analysis;

# 1. Find the total no. of cases, deaths and death rate of covid-19 in the world
 SELECT 
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    SUM(new_deaths) / SUM(new_cases) * 100 AS death_rate
FROM
    covid_analysis.covid_deaths_data
WHERE
    continent IS NOT NULL
ORDER BY 1 , 2;

# 3. Sort death count per population by continent
SELECT 
    continent, sum(new_deaths) AS TotalDeathCount
FROM
    covid_analysis.covid_deaths_data
WHERE
    continent IS not NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC;
 
 
#4a. Find the infection rate time series of major countries 
select location, population, date, max(total_cases) as highestInfectionCount, max(total_cases/population)*100 as infection_rate
from covid_analysis.covid_deaths_data
where  continent is not null
AND location IN ('United States', 'China', 'Japan', 'India', 'Mexico', 'Germany', 'United Kingdom', 'France')
Group by Location, Population, date
order by infection_rate desc;

#4b. Find the death rate time series of people contracting Covid-19 in major countries 
select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_rate
from covid_analysis.covid_deaths_data
where continent is not null
and location IN ('United States', 'China', 'Japan', 'India', 'Mexico', 'Germany', 'United Kingdom', 'France')
order by 1,2;

#5a. Find the infection rate of covid-19 in all countries
Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as infection_rate
From covid_analysis.covid_deaths_data
Group by Location, Population
order by infection_rate desc;
 
#5b. Find the mortality rate of covid-19 in all countries
SELECT 
    location,
    population,
    MAX(total_deaths) AS highestdeathCount,
    MAX((total_deaths / population)) * 100 AS death_rate
FROM
    covid_analysis.covid_deaths_data
    where continent is not null
group by location, population
ORDER BY death_rate desc;
 