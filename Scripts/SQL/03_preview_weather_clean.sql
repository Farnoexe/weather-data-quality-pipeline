-- Preview clean weather records from Athena
-- This confirms the clean table can read validated weather data from S3
SELECT *
FROM weather_clean
LIMIT 10
