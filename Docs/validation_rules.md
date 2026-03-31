# Validation Rules

## Required Fields
- time
- temperature_2m
- relative_humidity_2m
- precipitation
- wind_speed_10m
- latitude
- longitude

## Validation Checks
1. Required fields must not be empty.
2. temperature_2m must be between -50 and 60.
3. relative_humidity_2m must be between 0 and 100.
4. precipitation must be greater than or equal to 0.
5. wind_speed_10m must be greater than or equal to 0.
6. time must be unique within a file.

## Output Rule
- Valid rows go to Clean.
- Invalid rows go to Quarantine.
- Duplicate rows go to Quarantine.
- Quarantined rows must include a rejection_reason column.

## Notes
- Duplicate detection is based on the time field.
- The first valid occurrence of a time value is kept.
- Later duplicate occurrences of the same time value are quarantined with rejection_reason = duplicate_time.
