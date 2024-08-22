-- About Dataset:

-- Environment Data collected from UNdata. Dataset contains CO2 Emissions, Land, Water, Threatened Species.
-- Carbon Dioxide Emissions:Columns: Country(Area), Year, Series, Value
-- Land:Columns: Country(Area), Year, Series, Value
-- Threatened Species:Columns: Country(Area), Year, Series, Value
-- Water and Sanitation Services:Columns: Country(Area), Year, Series, Value

use carbon_emission;
select * from `carbon dioxide emission estimates`;

-- Any column having null values 

select * from `carbon dioxide emission estimates`
where `CO2 emission estimates` is null;

-- Checking if there are null values in the dataset `carbon dioxide emission estimates`

select * from `carbon dioxide emission estimates`
where `CO2 emission estimates` is null;

select * from `carbon dioxide emission estimates`
where Year is null;

select * from `carbon dioxide emission estimates`
where Series is null;

select * from `carbon dioxide emission estimates`
where Value is null;

-- There are no null values in the dataset `carbon dioxide emission estimates`
-- No I will examine smaller part of the dataset including Year,Value,Series with different functions.

select min(Year), max(Year)
from `carbon dioxide emission estimates`;

select distinct Series
from `carbon dioxide emission estimates`;

select min(Value), max(Value)
from `carbon dioxide emission estimates`
where Series = 'Emissions (thousand metric tons of carbon dioxide)';

select min(Value), max(Value)
from `carbon dioxide emission estimates`
Where Series = 'Emissions per capita (metric tons of carbon dioxide)';

-- I will check  min and max value. of co2 for countries 

select *
FROM `carbon dioxide emission estimates`
WHERE Series = 'Emissions per capita (metric tons of carbon dioxide)';

-- All years for 'Emissions per capita (metric tons of carbon dioxide)'

SELECT 
DISTINCT (`CO2 emission estimates`) AS country,
SUM(Value) AS emission_Years,
year
FROM `carbon dioxide emission estimates`
WHERE Series = 'Emissions per capita (metric tons of carbon dioxide)'
GROUP BY country,year;

-- All year for 'Emissions (thousand metric tons of carbon dioxide)'

SELECT 
DISTINCT (`CO2 emission estimates`) AS country,
SUM(Value) AS emission,
year
FROM `carbon dioxide emission estimates`
WHERE Series = 'Emissions (thousand metric tons of carbon dioxide)'
GROUP BY country,year;

# Top popluters by year

SELECT e1.Year, e1.`CO2 emission estimates`,e1.Value
FROM `Carbon Dioxide Emission Estimates` AS e1
JOIN (
    SELECT Year, MAX(Value) AS max_emissions
    FROM `Carbon Dioxide Emission Estimates`
    GROUP BY Year
) e2
ON e1.Year = e2.Year AND e1.Value = e2.max_emissions
ORDER BY e1.Year;

# Count of country who are popluting the most

SELECT Year, GROUP_CONCAT(`CO2 emission estimates`) AS Countries, COUNT(DISTINCT `CO2 emission estimates`) AS Polluter_Count
FROM (
SELECT e1.Year, e1.`CO2 emission estimates`, e1.Value AS Max_Emissions
FROM `Carbon Dioxide Emission Estimates` AS e1
JOIN (SELECT Year, MAX(Value) AS Max_Emissions
FROM `Carbon Dioxide Emission Estimates`
GROUP BY Year) AS e2
ON e1.Year = e2.Year AND e1.Value = e2.Max_Emissions
) AS S
GROUP BY Year;

# Lowest Pollution - which country ? -- Least Emissions Per Capita

SELECT `CO2 emission estimates`
FROM `Carbon Dioxide Emission Estimates`
WHERE 'Emissions (thousand metric tons of carbon dioxide)' IS NOT NULL
GROUP BY `CO2 emission estimates`
ORDER BY AVG(Value) ASC
LIMIT 1;

SELECT `CO2 emission estimates`
FROM `Carbon Dioxide Emission Estimates`
WHERE 'Emissions per capita (metric tons of carbon dioxide)' IS NOT NULL
GROUP BY `CO2 emission estimates`
ORDER BY SUM(Value) ASC
LIMIT 1;

-- Min amd Max Value for all tables;

select `water supply and sanitation services`,Series,
Max(Value),Min(Value)
from `water and sanitation services`
group by `water supply and sanitation services`,Series
order by Min(Value) asc;

select land,Series,
Max(Value), Min(Value)
from land
group by Land,Series
order by Min(Value) asc;

select 'CO2 emission estimates', Series,
Max(Value),Min(Value)
from  `Carbon Dioxide Emission Estimates`
group by 'CO2 emission estimates',Series
order by Min(Value) asc;

-- Land area quartile

select * from Land;

select distinct(Year)
from land;

select *
from land
where Series = "Land area (thousand hectares)" and Land !="Total, all countries or areas" and Year = 2017
order by Value asc;

-- where Series = "Forest cover (thousand hectares)" and Land !="Total, all countries or areas";

-- percentile of land (CTE)

-- Initialize row number variable
SET @row_num := 0;

-- Subquery to rank rows and calculate total rows
WITH Ranked AS (
    SELECT 
        Land,
        Series,
        Value,
        @row_num := @row_num + 1 AS row_num,
        @total_rows := (SELECT COUNT(*) 
                        FROM Land 
                        WHERE Land = 'Total, all countries or areas' 
                        AND Series = 'Land area (thousand hectares)') AS total_rows
    FROM 
        Land
    WHERE 
        Land = 'Total, all countries or areas'
        AND Series = 'Land area (thousand hectares)'
    ORDER BY 
        Value
)
-- Main query to select the percentiles
SELECT 
    Land,
    Series,
    MAX(CASE WHEN row_num = CEIL(total_rows * 0.20) THEN Value END) AS percentile_20,
    MAX(CASE WHEN row_num = CEIL(total_rows * 0.50) THEN Value END) AS percentile_50,
    MAX(CASE WHEN row_num = CEIL(total_rows * 0.70) THEN Value END) AS percentile_70
FROM 
Ranked
GROUP BY 
Land, Series;

-- 1. Which country has the highest forest cover area?
-- 2. Perform hypothesis testing based on (relation if forest is inc and is pollution also inc)
-- 3. Country having least co2 after 1975 based on year? Did they work on co2 reduction ?
-- which countries are least & most worried about pollution control ? - lead lag
-- which country has lost most of its forest area ?
-- which country has lost most of its water resources ?
-- based on water , land & emssion conculude which country has improved / worstened their emission
-- 4. Perform 2nd hypothsis testing (water and sanitisation)
-- checkmif water is related with Pollution or not
-- 5. Forest relation with Pollution ? How to check?
-- 6. Statement based on analysis.
-- 7. Make Project document

-- Forest -- which country has lost most of its forest area

WITH Forest_Change AS (
    SELECT
        l.Land,
        l.Series,
        (MIN(l.Value) - MAX(l.Value)) AS Change_V
    FROM
        land AS l
    GROUP BY
        l.Land,
        l.Year,
        l.Series
)
SELECT
    Land,
    Series,
    Change_V
FROM
    Forest_Change
Where Series = 'Forest cover (% of total land area)'
ORDER BY
    Change_V asc
LIMIT 100;

select * from land;

-- which country has lost most of its Water Area?

WITH Water_Area_Change AS (
    SELECT
        w.`Water supply and sanitation services`,
        w.Year,
        w.Series,
        w.Value AS Water,
        LAG(w.Value) OVER (PARTITION BY w.`Water supply and sanitation services` ORDER BY w.Year) AS Prev_Year_Water
    FROM
        `water and sanitation services` AS w
    WHERE
        w.Series = 'Safely managed drinking water sources, urban (Proportion of population with access)'
),
Water_Loss AS (
    SELECT
        `Water supply and sanitation services`,
        Year,
        Water,
        Prev_Year_Water,
        (Prev_Year_Water - Water) AS Yearly_Loss
    FROM
        Water_Area_Change
    WHERE
        Prev_Year_Water IS NOT NULL
)
SELECT
    `Water supply and sanitation services`,
    SUM(Yearly_Loss) AS Total_Loss
FROM
    Water_Loss
GROUP BY
    `Water supply and sanitation services`,
    Year
ORDER BY
    Total_Loss DESC
LIMIT 50;

-- Water -- which country has lost most of its water resources ?

WITH Water_Resource_Change AS (
    SELECT
        w.country AS Country,
        MIN(w.year) AS Start_Year,
        MAX(w.year) AS End_Year,
        MIN(w.Water_supply_and_sanitation_services) AS Start_Water_Resources,
        MAX(w.Water_supply_and_sanitation_services) AS End_Water_Resources,
        (MIN(w.Water_supply_and_sanitation_services) - MAX(w.Water_supply_and_sanitation_services)) AS Water_Resources_Loss
    FROM
        water_and_sanitation_services AS w
    GROUP BY
        w.country
)
SELECT
    Country,
    Start_Year,
    End_Year,
    Start_Water_Resources,
    End_Water_Resources,
    Water_Resources_Loss
FROM
    Water_Resource_Change
ORDER BY
    Water_Resources_Loss DESC
LIMIT 1;

-- countries are least & most worried about pollution control ? 

WITH CO2_Changes AS (
    SELECT
        c.country AS Country,
        c.year AS Year,
        c.CO2_emission_estimates AS Current_CO2_Emission,
        LAG(c.CO2_emission_estimates) OVER (PARTITION BY c.country ORDER BY c.year) AS Previous_CO2_Emission,
        ((c.CO2_emission_estimates - LAG(c.CO2_emission_estimates) OVER (PARTITION BY c.country ORDER BY c.year)) 
         / LAG(c.CO2_emission_estimates) OVER (PARTITION BY c.country ORDER BY c.year)) * 100 AS CO2_Percentage_Change
    FROM
        carbon_dioxide_emission_estimates AS c
)
, Summary AS (
    SELECT
        Country,
        AVG(CO2_Percentage_Change) AS Avg_CO2_Percentage_Change
    FROM
        CO2_Changes
    WHERE
        Previous_CO2_Emission IS NOT NULL  -- Exclude the first year where there is no previous year to compare
    GROUP BY
        Country
)
SELECT
    Country,
    Avg_CO2_Percentage_Change
FROM
    Summary
ORDER BY
    Avg_CO2_Percentage_Change DESC;  -- Highest positive change indicates most worried, lowest indicates least worried

-- Country having highest forest cover area?
SELECT Land, Series,Value as Forest ,Year
FROM land
WHERE year = 2005 and Series = 'Forest cover (% of total land area)'
ORDER BY Value desc
limit 80;

-- Joining the tables based on emission 
-- Joining water table 

SELECT 
l.Land,l.Series,l.Year,l.Value as Forest,c.Value as C_Emission,W.Value as Water_Count
FROM land as l
JOIN `carbon dioxide emission estimates` as c
on l.Land = C.`CO2 emission estimates` and l.Year = C.Year
Join `water and sanitation services` as W
on l.Land = W.`Water supply and sanitation services` and l.Year = W.Year
WHERE 
l.Year = c.Year AND l.Year = w.Year and l.Series = 'Forest cover (thousand hectares)' and W.Series = 'Safely managed drinking water sources, total (Proportion of population with access)'
limit 80;

-- emission 
select *
from  `carbon dioxide emission estimates`
where Series = 'Emissions per capita (metric tons of carbon dioxide)' and Year = 2005;

-- Countries having least co2

SELECT 
l.Land,l.Series,l.Year,l.Value as Forest,c.Value as C_Emission,W.Value as Water_Count
FROM land AS l
JOIN
    `carbon dioxide emission estimates` AS c
    ON l.Land = c.`CO2 emission estimates` AND l.Year = c.Year
JOIN
    `water and sanitation services`AS w
    ON l.Land = w.`Water supply and sanitation services` AND l.Year = w.Year
WHERE
    l.Series = 'Forest cover (thousand hectares)'
    AND w.Series = 'Safely managed drinking water sources, total (Proportion of population with access)'
    And c.Series = 'Emissions per capita (metric tons of carbon dioxide)'
LIMIT 80;  

select * from Land;

-- Countries doing aggressive consump ?

SELECT 
l.Land,l.Series,l.Year,l.Value as Forest,c.Value as C_Emission,(c.value/l.value) as Aggressive_Consump
FROM land AS l
JOIN
    `carbon dioxide emission estimates` AS c
    ON l.Land = c.`CO2 emission estimates` AND l.Year = c.Year
WHERE
    l.Series = 'Forest cover (thousand hectares)'
    AND c.Series = 'Emissions per capita (metric tons of carbon dioxide)'
ORDER BY
     Aggressive_Consump asc  -- High CO2 emissions per unit of forest cover
LIMIT 10;

-- Same with water 
SELECT 
w.`Water supply and sanitation services`,w.Value, w.Year,c.Value as C_Emission,(c.value/w.value) as Aggressive_Consump
FROM `carbon dioxide emission estimates` AS c
JOIN
    `water and sanitation services` AS w
    ON w.`Water supply and sanitation services` = c.`CO2 emission estimates` AND w.Year = c.Year
WHERE
c.Series = 'Emissions per capita (metric tons of carbon dioxide)'
And w.series = 'Safely managed drinking water sources, total (Proportion of population with access)'
ORDER BY
Aggressive_Consump asc -- High CO2 emissions per unit of forest cover
LIMIT 20;

-- Checking if Forest is related to pollution? Direct relationship co2 and forest 
SELECT 
l.Land,l.Series,l.Year,l.Value as Forest,c.Value as C_Emission
FROM land as l
JOIN `carbon dioxide emission estimates` as c
on l.Land = C.`CO2 emission estimates` and l.Year = C.Year
WHERE 
l.Year = c.Year 
and l.Series = 'Forest cover (thousand hectares)' and c.Series = 'Emissions (thousand metric tons of carbon dioxide)'
limit 80;

-- Checking if water is related with Pollution  -- No relationship with pollution
SELECT 
w.`Water supply and sanitation services`,w.Series,w.Year,w.Value as Water,c.Value as C_Emission
FROM `water and sanitation services` as w
JOIN `carbon dioxide emission estimates` as c
on w.`Water supply and sanitation services` = C.`CO2 emission estimates` and w.Year = c.Year
WHERE 
w.Year = c.Year 
and w.Series = 'Safely managed drinking water sources, total (Proportion of population with access)' 
and c.Series = 'Emissions (thousand metric tons of carbon dioxide)'
limit 20;

-- Countries having more usage of Water

WITH Water_Usage AS (
    SELECT
        `Water supply and sanitation services` AS country,
        Year,
        Value AS Emission_Value
    FROM
        `water and sanitation services`
    WHERE
        Series = 'Safely managed drinking water sources, total (Proportion of population with access)'
)

SELECT
    country,
    MAX(Emission_Value) AS Max_Usage
FROM
    Water_Usage
GROUP BY
    country
ORDER BY
    Max_Usage DESC;

-- Based on water land and co2 .. which country has improved/or worsened?
-- CTE Method

WITH CTE_EmissionChange AS (
    SELECT
        c.`CO2 emission estimates` AS country,
        c.Year,
        c.Value AS Emission,
        LAG(c.Value) OVER(PARTITION BY c.`CO2 emission estimates` ORDER BY c.Year) AS Prev_Emission
    FROM
        `carbon dioxide emission estimates` AS c
    WHERE
        c.Series = 'Emissions (thousand metric tons of carbon dioxide)'
),
CTE_WaterChange AS (
    SELECT
        w.`Water supply and sanitation services` AS country,
        w.Year,
        w.Value AS Water_Cover
    FROM
        `water and sanitation services` AS w
    WHERE
        w.Series = 'Safely managed drinking water sources, total (Proportion of population with access)'
),
CTE_LandChange AS (
    SELECT
        l.Land AS country,
        l.Year,
        l.Value AS Forest_Cover
    FROM
        land AS l
    WHERE
        l.Series = 'Forest cover (thousand hectares)'
),
CTE_Combined AS (
    SELECT
        e.country,
        e.Year,
        e.Emission,
        e.Prev_Emission,
        e.Emission - e.Prev_Emission AS Emission_Change,
        w.Water_Cover,
        l.Forest_Cover
    FROM
        CTE_EmissionChange AS e
    JOIN
        CTE_WaterChange AS w
    ON
        e.country = w.country
        AND e.Year = w.Year
    JOIN
        CTE_LandChange AS l
    ON
        e.country = l.country
        AND e.Year = l.Year
),
CTE_PercentageChange AS (
    SELECT
        country,
        Year,
        Emission,
        Prev_Emission,
        ((Emission - Prev_Emission) / (Prev_Emission)) * 100 AS Emission_Percentage,
        Water_Cover,
        ((Water_Cover - LAG(Water_Cover) OVER(PARTITION BY country ORDER BY Year)) / (LAG(Water_Cover) OVER(PARTITION BY country ORDER BY Year))) * 100 AS Water_Percentage,
        Forest_Cover,
        ((Forest_Cover - LAG(Forest_Cover) OVER(PARTITION BY country ORDER BY Year)) / (LAG(Forest_Cover) OVER(PARTITION BY country ORDER BY Year))) * 100 AS Forest_Percentage
    FROM
CTE_Combined
)
SELECT
    country,
    Year,
    Emission_Percentage,
    Water_Percentage,
    Forest_Percentage,
    CASE 
        WHEN Emission_Percentage <-1
             AND Water_Percentage < -1
             AND Forest_Percentage < -1
             THEN 'Improved'
        WHEN Emission_Percentage > 0
             OR Water_Percentage > 0
             OR Forest_Percentage > 0
             THEN 'Worsened'
        ELSE 'Mixed'
    END AS Status
FROM 
CTE_PercentageChange;