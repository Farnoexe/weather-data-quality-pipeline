-- ============================================
-- DATA QUALITY VALIDATION CHECKS
-- Project: Weather Data Quality Pipeline
-- Purpose: Validate clean and quarantine datasets in Athena
-- ============================================


-- --------------------------------------------
-- CLEAN DATA VALIDATIONS
-- --------------------------------------------

-- Validation 1: Count rows in clean table
-- Confirms total number of validated records available for analysis
SELECT COUNT(*) AS clean_row_count
FROM weather_clean;

-- Validation 2: Check for duplicate timestamps
-- Expected: 0 rows
SELECT time, COUNT(*) AS duplicate_count
FROM weather_clean
GROUP BY time
HAVING COUNT(*) > 1;

-- Validation 3: Check for invalid numeric ranges
-- Expected: 0 rows
SELECT *
FROM weather_clean
WHERE temperature_2m < -50
   OR temperature_2m > 60
   OR relative_humidity_2m < 0
   OR relative_humidity_2m > 100
   OR precipitation < 0
   OR wind_speed_10m < 0;


-- --------------------------------------------
-- QUARANTINE DATA VALIDATIONS
-- --------------------------------------------

-- Validation 4: Count rows in quarantine table
-- Confirms number of rejected records
SELECT COUNT(*) AS quarantine_row_count
FROM weather_quarantine;

-- Validation 5: Rejection reason breakdown
-- Shows distribution of validation failures
SELECT rejection_reason, COUNT(*) AS rejected_row_count
FROM weather_quarantine
GROUP BY rejection_reason
ORDER BY rejected_row_count DESC, rejection_reason;
