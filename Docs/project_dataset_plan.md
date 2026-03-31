# Project Dataset Plan

## API
Open-Meteo Forecast API

## City
Taipei

## Granularity
Hourly

## Fields
- time
- temperature_2m
- relative_humidity_2m
- precipitation
- wind_speed_10m
- latitude
- longitude

## Why this dataset
- Public API
- JSON response
- No authentication required
- Real-world weather data
- Good numeric fields for validation
- Good fit for S3 and Athena analytics

## Raw Data Format
- One JSON file per pipeline run
- Filename pattern: weather_raw_YYYYMMDD_HHMMSS.json

## Notes
- The API may return the nearest weather grid coordinates rather than the exact requested latitude and longitude.
- Raw data is saved before validation or cleaning.
