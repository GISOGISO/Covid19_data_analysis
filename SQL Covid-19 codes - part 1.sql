#data source:
#https://ourworldindata.org/covid-deaths

#Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

#create database

create database if not exists covid_analysis;
use covid_analysis;


#create table that contains vaccination data from data source
	DROP TABLE IF EXISTS covid_vac_data;
CREATE TABLE covid_vac_data (
  iso_code VARCHAR(10),
  continent VARCHAR(50),
  location VARCHAR(100),
  date DATE,
  new_tests BIGINT,
  total_tests_per_thousand FLOAT,
  new_tests_per_thousand FLOAT,
  new_tests_smoothed BIGINT,
  new_tests_smoothed_per_thousand FLOAT,
  positive_rate FLOAT,
  tests_per_case FLOAT,
  tests_units VARCHAR(20),
  total_vaccinations BIGINT,
  people_vaccinated BIGINT,
  people_fully_vaccinated BIGINT,
  total_boosters BIGINT,
  new_vaccinations BIGINT,
  new_vaccinations_smoothed BIGINT,
  total_vaccinations_per_hundred FLOAT,
  people_vaccinated_per_hundred FLOAT,
  people_fully_vaccinated_per_hundred FLOAT,
  total_boosters_per_hundred FLOAT,
  new_vaccinations_smoothed_per_million FLOAT,
  new_people_vaccinated_smoothed BIGINT,
  new_people_vaccinated_smoothed_per_hundred FLOAT,
  stringency_index FLOAT,
  population_density FLOAT,
  median_age FLOAT,
  aged_65_older FLOAT,
  aged_70_older FLOAT,
  gdp_per_capita FLOAT,
  extreme_poverty FLOAT,
  cardiovasc_death_rate FLOAT,
  diabetes_prevalence FLOAT,
  female_smokers FLOAT,
  male_smokers FLOAT,
  handwashing_facilities FLOAT,
  hospital_beds_per_thousand FLOAT,
  life_expectancy FLOAT,
  human_development_index FLOAT,
  excess_mortality_cumulative_absolute BIGINT,
  excess_mortality_cumulative FLOAT,
  excess_mortality FLOAT,
  excess_mortality_cumulative_per_million FLOAT
);
#load data into the table
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidVaccinations.csv' 
INTO TABLE covid_vac_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

#create table that contains covid deaths data from data source
DROP TABLE IF EXISTS covid_deaths_data;

CREATE TABLE covid_deaths_data (
  iso_code VARCHAR(10),
  continent VARCHAR(50),
  location VARCHAR(100),
  date DATE,
  population INT,
  total_cases INT,
  new_cases INT,
  new_cases_smoothed INT,
  total_deaths INT,
  new_deaths INT,
  new_deaths_smoothed INT,
  total_cases_per_million DECIMAL(10,2),
  new_cases_per_million DECIMAL(10,2),
  new_cases_smoothed_per_million DECIMAL(10,2),
  total_deaths_per_million DECIMAL(10,2),
  new_deaths_per_million DECIMAL(10,2),
  new_deaths_smoothed_per_million DECIMAL(10,2),
  reproduction_rate DECIMAL(5,2),
  icu_patients INT,
  icu_patients_per_million DECIMAL(10,2),
  hosp_patients INT,
  hosp_patients_per_million DECIMAL(10,2),
  weekly_icu_admissions INT,
  weekly_icu_admissions_per_million DECIMAL(10,2),
  weekly_hosp_admissions INT,
  weekly_hosp_admissions_per_million DECIMAL(10,2)
);
#load data into the table
LOAD DATA LOCAL   INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidDeaths.csv' 
INTO TABLE covid_deaths_data 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

#data cleaning, remove unneccesary data
DELETE FROM covid_deaths_data WHERE location LIKE '%income%';
DELETE FROM covid_vac_data WHERE location LIKE '%income%';
DELETE FROM covid_deaths_data 
WHERE location IN ('world', 'Asia', 'Africa', 'Europe', 'North America', 'South America', 'European Union', 'Oceania');
DELETE FROM covid_vac_data 
WHERE location IN ('world', 'Asia', 'Africa', 'Europe', 'North America', 'South America', 'European Union', 'Oceania');

DELETE FROM covid_deaths_data
WHERE continent = 'continent';
DELETE FROM covid_vac_data
WHERE continent = 'continent';

#data checking
select *
from covid_analysis.covid_deaths_data
where continent is not null
order by 3,4;

select *
from covid_analysis.covid_vac_data
where continent is not null
order by 3,4;


#join the 2 tables to find the new vaccinations/ day and accumulated vaccinations in different countries 

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM covid_analysis.covid_deaths_data dea
JOIN covid_analysis.covid_vac_data vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL 
ORDER BY 2,3;

# Find the rolling vaccination rates in different countries Using CTE to perform Calculation on Partition By 
WITH PopVsVac (continent, location, date, population, new_vaccinations, rollingPeopleVaccinated) AS (
    SELECT 
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rollingPeopleVaccinated
    FROM
        covid_analysis.covid_deaths_data dea
    JOIN
        covid_analysis.covid_vac_data vac ON dea.location = vac.location
        AND dea.date = vac.date
    WHERE 
        dea.continent IS NOT NULL
)
SELECT 
*, (rollingPeopleVaccinated/population)*100 
FROM 
    PopVsVac;

#Notes: more than 100% - more than 1 vax/ person, i.e. boosters etc


-- create temp table  

	DROP TABLE IF EXISTS vac_rate;
    create table  vac_rate
    (
    continent nvarchar(255),
    location nvarchar(255),
    date date,
    population numeric,
    new_vaccinations numeric,
    vac_count BIGINT 
    );
    insert into vac_rate
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS vac_count
    FROM
        covid_analysis.covid_deaths_data dea
    JOIN
        covid_analysis.covid_vac_data vac ON dea.location = vac.location
        AND dea.date = vac.date;

     select *  , (vac_count/population)*100
    from vac_rate;

    
        
    
-- Creating View to store data for later visualizations
	DROP TABLE IF EXISTS vac_rate;

Create View vac_rate as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
        SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS vac_count
    FROM
        covid_analysis.covid_deaths_data dea
    JOIN
        covid_analysis.covid_vac_data vac ON dea.location = vac.location
        AND dea.date = vac.date
        where dea.continent is not null
        ;

        SELECT USER(), CURRENT_USER();
        

