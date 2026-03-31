-- Preview quarantined weather records from Athena
-- This confirms the quarantine table can read rejected records from S3
SELECT *
FROM weather_quarantine
LIMIT 10
