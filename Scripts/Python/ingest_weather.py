import json
from datetime import datetime
from pathlib import Path

import requests


def fetch_weather_data():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 25.0330,
        "longitude": 121.5654,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ],
        "forecast_days": 1,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def generate_output_filepath():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"weather_raw_{timestamp}.json"

    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "Data" / "Raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir / file_name


def save_raw_data(data, output_path):
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def get_record_count(data):
    return len(data.get("hourly", {}).get("time", []))


def main():
    try:
        print("Starting weather data ingestion...")

        data = fetch_weather_data()
        record_count = get_record_count(data)

        if record_count == 0:
            print("No hourly records were returned by the API. Skipping save.")
            return

        output_path = generate_output_filepath()
        save_raw_data(data, output_path)

        print("Weather data fetched successfully.")
        print(f"Records fetched: {record_count}")
        print(f"Raw data saved to: {output_path}")

    except requests.exceptions.RequestException as e:
        print("Error fetching data from API:")
        print(e)

    except OSError as e:
        print("Error saving raw data to file:")
        print(e)

    except Exception as e:
        print("Unexpected error:")
        print(e)


if __name__ == "__main__":
    main()
