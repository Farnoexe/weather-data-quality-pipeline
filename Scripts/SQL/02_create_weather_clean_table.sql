-- Create external table over clean weather CSV data stored in S3
-- This table represents validated and accepted records after data quality checks
CREATE EXTERNAL TABLE weather_clean (
  time string,
  temperature_2m double,
  relative_humidity_2m double,
  precipitation double,
  wind_speed_10m double,
  latitude double,
  longitude double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project3-weather-quality/clean/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
)
