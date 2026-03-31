import csv
import json
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


def get_project_root():
    return Path(__file__).resolve().parents[2]


def get_raw_dir():
    raw_dir = get_project_root() / "Data" / "Raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def get_raw_flatten_dir():
    raw_flatten_dir = get_project_root() / "Data" / "Raw_Flatten"
    raw_flatten_dir.mkdir(parents=True, exist_ok=True)
    return raw_flatten_dir


def get_latest_raw_file():
    raw_dir = get_raw_dir()
    raw_files = sorted(raw_dir.glob("weather_raw_*.json"))

    if not raw_files:
        raise FileNotFoundError(
            "No raw JSON files found in Data/Raw. Run the ingestion step first."
        )

    return raw_files[-1]


def load_raw_data(file_path):
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def flatten_weather_data(data):
    hourly_data = data.get("hourly", {})

    times = hourly_data.get("time", [])
    temperatures = hourly_data.get("temperature_2m", [])
    humidities = hourly_data.get("relative_humidity_2m", [])
    precipitations = hourly_data.get("precipitation", [])
    wind_speeds = hourly_data.get("wind_speed_10m", [])

    latitude = data.get("latitude")
    longitude = data.get("longitude")

    list_lengths = [
        len(times),
        len(temperatures),
        len(humidities),
        len(precipitations),
        len(wind_speeds),
    ]

    if len(set(list_lengths)) != 1:
        raise ValueError("Hourly data lists do not have matching lengths.")

    records = []

    for index in range(len(times)):
        record = {
            "time": times[index],
            "temperature_2m": temperatures[index],
            "relative_humidity_2m": humidities[index],
            "precipitation": precipitations[index],
            "wind_speed_10m": wind_speeds[index],
            "latitude": latitude,
            "longitude": longitude,
        }
        records.append(record)

    return records


def generate_raw_flatten_output_filepath(raw_file_path):
    raw_flatten_dir = get_raw_flatten_dir()

    timestamp = raw_file_path.stem.replace("weather_raw_", "")
    file_name = f"weather_raw_flattened_{timestamp}.csv"

    return raw_flatten_dir / file_name


def save_raw_flattened_csv(records, output_path):
    if not records:
        raise ValueError("No records available to save to CSV.")

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)


def main():
    try:
        print("Starting weather data flattening...")

        raw_file_path = get_latest_raw_file()
        data = load_raw_data(raw_file_path)
        records = flatten_weather_data(data)

        if not records:
            print("No records found in raw file. Skipping CSV save.")
            return

        output_path = generate_raw_flatten_output_filepath(raw_file_path)
        save_raw_flattened_csv(records, output_path)

        print("Weather data flattened successfully.")
        print(f"Source raw file: {raw_file_path}")
        print(f"Flattened records: {len(records)}")
        print(f"Raw flattened CSV saved to: {output_path}")

    except FileNotFoundError as e:
        print("Raw file error:")
        print(e)

    except json.JSONDecodeError as e:
        print("Error reading JSON data:")
        print(e)

    except (OSError, ValueError) as e:
        print("Error processing or saving flattened data:")
        print(e)

    except Exception as e:
        print("Unexpected error:")
        print(e)


if __name__ == "__main__":
    main()
