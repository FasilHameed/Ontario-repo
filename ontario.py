import os
import json
import requests
import pandas as pd
from datetime import datetime
import tempfile
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ✅ Read credentials and API key from environment variables (used in GitHub secrets)
SERVICE_ACCOUNT_JSON = json.loads(os.environ.get("SERVICE_ACCOUNT_JSON"))
MOTIVE_API_KEY = os.environ.get("MOTIVE_API_KEY")

BASE_URL = "https://api.gomotive.com"
SPREADSHEET_ID = '1LAmSMJj-RLvZAUTtyG_jTXw1XhUHcS9mHxNMVK7d7So'

start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")
HEADERS = {"x-api-key": MOTIVE_API_KEY}

# 🛠️ API Endpoints
ENDPOINTS = [
    {"url": "/v1/hours_of_service", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Hours_of_Service", "data_key": "hours_of_services"},
    {"url": "/v1/logs", "params": {}, "sheet_name": "Driver_Logs", "data_key": "logs"},
    {"url": "/v1/inspection_reports", "params": {}, "sheet_name": "Inspection_Reports_v1", "data_key": "inspection_reports"},
    {"url": "/v2/inspection_reports", "params": {}, "sheet_name": "Inspection_Reports_v2", "data_key": "inspection_reports"},
    {"url": "/v2/logs", "params": {}, "sheet_name": "Driver_Logs_v2", "data_key": "logs"},
    {"url": "/v1/speeding_events", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Speeding_Events", "data_key": "speeding_events"},
    {"url": "/v1/hos_violations", "params": {"min_start_time": start_date, "max_start_time": end_date}, "sheet_name": "hos_violations", "data_key": "hos_violations"},
    {"url": "/v1/available_time", "params": {}, "sheet_name": "Available_Time", "data_key": "users"},
    {"url": "/v1/driver_utilization", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Driver_Utilization_v1", "data_key": "driver_idle_rollups"},
    {"url": "/v2/driver_utilization", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Driver_Utilization_v2", "data_key": "driver_idle_rollups"},
    {"url": "/v1/vehicle_utilization", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Vehicle_Utilization", "data_key": "vehicle_idle_rollups"},
    {"url": "/v1/idle_events", "params": {"start_date": start_date, "end_date": end_date}, "sheet_name": "Idle_Events", "data_key": "idle_events"},
]

def flatten_json(nested_json, parent_key='', sep='.'):
    """Flatten nested JSON objects into a single-level dictionary."""
    items = {}
    if isinstance(nested_json, list):
        for i, item in enumerate(nested_json):
            new_key = f"{parent_key}[{i}]" if parent_key else str(i)
            items.update(flatten_json(item, new_key, sep=sep))
    elif isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.update(flatten_json(value, new_key, sep=sep))
    else:
        items[parent_key] = nested_json
    return items

def fetch_and_process_data(endpoint):
    """Fetch data from the API and return it as a DataFrame."""
    print(f"📡 Fetching: {endpoint['url']}")
    url = f"{BASE_URL}{endpoint['url']}"
    try:
        response = requests.get(url, headers=HEADERS, params=endpoint['params'])
        response.raise_for_status()
        data = response.json()
        if endpoint['data_key'] in data:
            records = data[endpoint['data_key']]
            flat_records = [flatten_json(item) for item in records]
            return pd.DataFrame(flat_records)
        else:
            print(f"⚠️ Missing expected data key: {endpoint['data_key']}")
    except Exception as e:
        print(f"❌ Error fetching {endpoint['url']}: {e}")
    return pd.DataFrame()

def get_temp_credentials_file():
    """Create a temporary JSON file from service account credentials."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as temp:
        json.dump(SERVICE_ACCOUNT_JSON, temp)
        return temp.name

def main():
    temp_creds_path = get_temp_credentials_file()

    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file(temp_creds_path, scopes=SCOPES)
        client = gspread.authorize(credentials)

        for endpoint in ENDPOINTS:
            df = fetch_and_process_data(endpoint)
            if not df.empty:
                try:
                    worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(endpoint['sheet_name'])
                    worksheet.clear()
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title=endpoint['sheet_name'], rows=100, cols=20)
                set_with_dataframe(worksheet, df)
                print(f"✅ Updated: {endpoint['sheet_name']}")
            else:
                print(f"⚠️ No data returned for: {endpoint['sheet_name']}")
    finally:
        os.remove(temp_creds_path)

if __name__ == "__main__":
    main()
