-- ============================================
-- SIMPLE ANALYSIS QUERIES
-- Project: Weather Data Quality Pipeline
-- Purpose: Run basic analysis on validated weather data in Athena
-- ============================================


-- --------------------------------------------
-- CLEAN DATA ANALYSIS
-- --------------------------------------------

-- Analysis 1: Average temperature
-- Measures the average temperature across the clean dataset
SELECT AVG(temperature_2m) AS avg_temperature_2m
FROM weather_clean;

-- Analysis 2: Maximum wind speed
-- Finds the highest recorded wind speed in the clean dataset
SELECT MAX(wind_speed_10m) AS max_wind_speed_10m
FROM weather_clean;

-- Analysis 3: Total precipitation
-- Measures total precipitation across the clean dataset
SELECT SUM(precipitation) AS total_precipitation
FROM weather_clean;
