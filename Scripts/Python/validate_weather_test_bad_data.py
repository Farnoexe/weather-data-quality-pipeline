import csv
from pathlib import Path


FIELDNAMES = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "latitude",
    "longitude",
]

QUARANTINE_FIELDNAMES = FIELDNAMES + ["rejection_reason"]


def get_project_root():
    return Path(__file__).resolve().parents[2]


def get_test_bad_data_file():
    test_file = (
        get_project_root()
        / "Data"
        / "Bad_Data_Test"
        / "weather_raw_flattened_test_bad_data.csv"
    )

    if not test_file.exists():
        raise FileNotFoundError(
            "Test bad data file not found in Data/Bad_Data_Test."
        )

    return test_file


def get_clean_dir():
    clean_dir = get_project_root() / "Data" / "Clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    return clean_dir


def get_quarantine_dir():
    quarantine_dir = get_project_root() / "Data" / "Quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    return quarantine_dir


def load_flattened_data(file_path):
    with file_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


def is_blank(value):
    return value is None or str(value).strip() == ""


def validate_row(row):
    rejection_reasons = []

    for field in FIELDNAMES:
        if is_blank(row.get(field)):
            rejection_reasons.append(f"missing_{field}")

    if rejection_reasons:
        return False, "; ".join(rejection_reasons)

    try:
        temperature = float(row["temperature_2m"])
        if temperature < -50 or temperature > 60:
            rejection_reasons.append("invalid_temperature_2m")
    except ValueError:
        rejection_reasons.append("invalid_temperature_2m")

    try:
        relative_humidity = float(row["relative_humidity_2m"])
        if relative_humidity < 0 or relative_humidity > 100:
            rejection_reasons.append("invalid_relative_humidity_2m")
    except ValueError:
        rejection_reasons.append("invalid_relative_humidity_2m")

    try:
        precipitation = float(row["precipitation"])
        if precipitation < 0:
            rejection_reasons.append("invalid_precipitation")
    except ValueError:
        rejection_reasons.append("invalid_precipitation")

    try:
        wind_speed = float(row["wind_speed_10m"])
        if wind_speed < 0:
            rejection_reasons.append("invalid_wind_speed_10m")
    except ValueError:
        rejection_reasons.append("invalid_wind_speed_10m")

    if rejection_reasons:
        return False, "; ".join(rejection_reasons)

    return True, ""


def split_valid_invalid_and_duplicates(rows):
    valid_rows = []
    invalid_rows = []
    duplicate_count = 0
    seen_times = set()

    for row in rows:
        is_valid, rejection_reason = validate_row(row)

        if not is_valid:
            invalid_row = row.copy()
            invalid_row["rejection_reason"] = rejection_reason
            invalid_rows.append(invalid_row)
            continue

        time_value = row["time"].strip()

        if time_value in seen_times:
            duplicate_count += 1
            invalid_row = row.copy()
            invalid_row["rejection_reason"] = "duplicate_time"
            invalid_rows.append(invalid_row)
            continue

        seen_times.add(time_value)
        valid_rows.append(row)

    return valid_rows, invalid_rows, duplicate_count


def generate_clean_output_filepath():
    file_name = "weather_clean_test_bad_data.csv"
    return get_clean_dir() / file_name


def generate_quarantine_output_filepath():
    file_name = "weather_quarantine_test_bad_data.csv"
    return get_quarantine_dir() / file_name


def save_csv(rows, output_path, fieldnames):
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    try:
        print("Starting weather data validation test...")

        flattened_file_path = get_test_bad_data_file()
        rows = load_flattened_data(flattened_file_path)

        if not rows:
            print("No rows found in test CSV. Skipping output files.")
            return

        valid_rows, invalid_rows, duplicate_count = split_valid_invalid_and_duplicates(rows)

        clean_output_path = generate_clean_output_filepath()
        quarantine_output_path = generate_quarantine_output_filepath()

        save_csv(valid_rows, clean_output_path, FIELDNAMES)
        save_csv(invalid_rows, quarantine_output_path, QUARANTINE_FIELDNAMES)

        print("Weather data validation test completed.")
        print(f"Source test file: {flattened_file_path}")
        print(f"Total rows checked: {len(rows)}")
        print(f"Valid rows: {len(valid_rows)}")
        print(f"Rejected rows: {len(invalid_rows)}")
        print(f"Duplicates quarantined: {duplicate_count}")
        print(f"Clean CSV saved to: {clean_output_path}")
        print(f"Quarantine CSV saved to: {quarantine_output_path}")

    except FileNotFoundError as e:
        print("Test file error:")
        print(e)

    except OSError as e:
        print("Error reading or saving CSV files:")
        print(e)

    except Exception as e:
        print("Unexpected error:")
        print(e)


if __name__ == "__main__":
    main()
