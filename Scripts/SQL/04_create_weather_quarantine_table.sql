-- Create external table over quarantine weather CSV data stored in S3
-- This table represents rejected records and includes the rejection reason
CREATE EXTERNAL TABLE weather_quarantine (
  time string,
  temperature_2m double,
  relative_humidity_2m double,
  precipitation double,
  wind_speed_10m double,
  latitude double,
  longitude double,
  rejection_reason string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project3-weather-quality/quarantine/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
)
