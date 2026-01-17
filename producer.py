
import csv
import time
import json
import re
from kafka import KafkaProducer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
csv_path = BASE_DIR / "input" / "Electric_Vehicle_Population_Data.csv"

if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found at {csv_path}")

# Make sure this matches the filename you downloaded from Kaggle
CSV_FILE_PATH = csv_path
TOPIC_NAME = 'electric-cars'
BOOTSTRAP_SERVERS = ['localhost:9092']


def clean_string(value):
    """Clean string values - handle null/empty and strip whitespace."""
    if value is None or value.strip() == '':
        return None
    return value.strip()


def clean_numeric(value, default=0):
    """Convert to integer, handle null/empty values."""
    if value is None or value.strip() == '':
        return default
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return default


def parse_vehicle_location(location_str):
    """Extract latitude and longitude from POINT format."""
    if location_str is None or location_str.strip() == '':
        return None, None

    # Format: POINT (-122.34301 47.659185)
    match = re.match(r'POINT \((-?\d+\.?\d*) (-?\d+\.?\d*)\)', location_str)
    if match:
        longitude = float(match.group(1))
        latitude = float(match.group(2))
        return latitude, longitude
    return None, None


def clean_row(row):
    """
    Clean and preprocess a single row of EV data.
    Handles null values, type conversions, and data validation.
    """
    cleaned = {}

    # String fields - clean and handle nulls
    cleaned['vin'] = clean_string(row.get('VIN (1-10)'))
    cleaned['county'] = clean_string(row.get('County'))
    cleaned['city'] = clean_string(row.get('City'))
    cleaned['state'] = clean_string(row.get('State'))
    cleaned['postal_code'] = clean_string(row.get('Postal Code'))
    cleaned['make'] = clean_string(row.get('Make'))
    cleaned['model'] = clean_string(row.get('Model'))
    cleaned['electric_vehicle_type'] = clean_string(row.get('Electric Vehicle Type'))
    cleaned['cafv_eligibility'] = clean_string(row.get('Clean Alternative Fuel Vehicle (CAFV) Eligibility'))
    cleaned['electric_utility'] = clean_string(row.get('Electric Utility'))

    # Numeric fields - convert to int with defaults
    cleaned['model_year'] = clean_numeric(row.get('Model Year'), default=0)
    cleaned['electric_range'] = clean_numeric(row.get('Electric Range'), default=0)
    cleaned['base_msrp'] = clean_numeric(row.get('Base MSRP'), default=0)
    cleaned['legislative_district'] = clean_numeric(row.get('Legislative District'), default=0)
    cleaned['dol_vehicle_id'] = clean_numeric(row.get('DOL Vehicle ID'), default=0)
    cleaned['census_tract'] = clean_string(row.get('2020 Census Tract'))

    # Parse vehicle location into lat/long
    latitude, longitude = parse_vehicle_location(row.get('Vehicle Location'))
    cleaned['latitude'] = latitude
    cleaned['longitude'] = longitude

    # Derived fields for analysis
    # Categorize EV type (BEV vs PHEV)
    ev_type = cleaned['electric_vehicle_type']
    if ev_type:
        cleaned['is_bev'] = 'Battery Electric Vehicle' in ev_type
    else:
        cleaned['is_bev'] = None

    # Categorize range (short, medium, long range)
    electric_range = cleaned['electric_range']
    if electric_range == 0:
        cleaned['range_category'] = 'Unknown'
    elif electric_range < 100:
        cleaned['range_category'] = 'Short Range'
    elif electric_range < 200:
        cleaned['range_category'] = 'Medium Range'
    else:
        cleaned['range_category'] = 'Long Range'

    # CAFV eligibility simplified
    cafv = cleaned['cafv_eligibility']
    if cafv:
        if 'Eligible' in cafv and 'Not eligible' not in cafv:
            cleaned['is_cafv_eligible'] = True
        elif 'Not eligible' in cafv:
            cleaned['is_cafv_eligible'] = False
        else:
            cleaned['is_cafv_eligible'] = None
    else:
        cleaned['is_cafv_eligible'] = None

    return cleaned


def is_valid_row(cleaned_row):
    """Check if the cleaned row has minimum required data."""
    # Must have at least VIN, Make, and Model
    return (
        cleaned_row.get('vin') is not None and
        cleaned_row.get('make') is not None and
        cleaned_row.get('model') is not None
    )


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"--- Starting Producer for Topic: {TOPIC_NAME} ---")

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            count = 0
            skipped = 0
            for row in reader:
                # Clean and preprocess the row
                cleaned_row = clean_row(row)

                # Skip invalid rows
                if not is_valid_row(cleaned_row):
                    skipped += 1
                    continue

                producer.send(TOPIC_NAME, value=cleaned_row)

                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} vehicles... (Last: {cleaned_row.get('make')} {cleaned_row.get('model')} in {cleaned_row.get('city')})")

                #time.sleep(0.01)

        producer.flush()
        print(f"--- Finished! Total records sent: {count}, Skipped: {skipped} ---")

    except FileNotFoundError:
        print(f"ERROR: The file '{CSV_FILE_PATH}' was not found.")
        print("Make sure the CSV file is in the same folder as this script.")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
