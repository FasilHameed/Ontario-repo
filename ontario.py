import requests
import pandas as pd
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import gspread
import tempfile
import os

# Store credentials directly in a variable instead of uploading a file
SERVICE_ACCOUNT_JSON = {
    "type": "service_account",
    "project_id": "ontaria",
    "private_key_id": "47d2cfc19ddf645a87b51437398cc0760389d8ac",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCbEHQVHxojjBw/\nPp0cDfLViJrk5jKDil/tuCZDVjWjteT71qfwps5YuC+HtQ1tmJ6skcy2d4XWFGUp\ncLykKIKcaFaiaNOonl8J49cwdZpdcm/3NjLhCs4nHwjcEa47Y0ypUiSFBtyeO80R\n85tHPBvW9+2NEeIEB2Dx5uRsdZCTsO0ppL8T5Tw89U+2KNQBvJiC88kYk+7mc5di\noltxv+Q0z7ny9AM5lJdWw/WIguF42u0wIswU1Y6UqjDGQph2RXe5rKInC3EDM5vN\nmV1ti+Q2m2ufMmag4pRSSmV6xVyYiuVo0F0ttnyA9JeJPbOVCEaewpxG457tvUFR\nhfCS+mWNAgMBAAECggEAEvAbfUjjJFxERuUawwZhFfstD6+dk7sepCXNZoPs4SWh\n5a/9qsJ0iRlVlLlj/nKZTnIwEkjeq5qqEDmYkGPyL6/+hK9QylMtQEP3UA/M3oTP\nb93KsPlk3BpKNrZiFRa3kiZaF3UTFLAxB3Q9DqIktEhrVsNMWzmbeW+9jhF9qsvM\nQEHEfSQmvl5DI8aQsBpWqC9gSOdIzEDC4SGckWCyZsR5nVMKxTla9FUCIDcSHsNA\nYpYRAOsOw2yorhwVM9UzT7+WohdPvhU6D1MhqH+kpywWPzj/3x1NorMMlnM32ou/\nagTutgjg6Eech0tH2SINCXGwqmWad+wRJdmbhFQCgQKBgQDIkvnGnTZeiFPV4Xrs\n9tdosG5OK3OUNpa90UWqveOXUZ9jFSbkUNKq8brHhMxqrga86Wcmn6MwWBzeK7tz\nvNTaSk2CLFnU227dQOnnkmI/AwFW1JW3fLzAnMJ7wIvk/C+vjb482cUEsy/Osk9/\nWnuXgXGt3+n8jT1mYuKG7n7CxQKBgQDF6gSAfpg5nGsuy4qTzH5dq7EXYhLd1hDR\n1MTGnKnmjkZ1xPXt5rTeRIqSiHjaI9QWgJwPKloqmVL7Ab91EiaRQDLIatTclyt4\n70k1Qp8Nw2DZMgIqmIgvkSzYaHrE0+StzaBeJ29DUB5xHEcloXoxHRnZ7gDFYfQd\nZcldrrukKQKBgQC18FySRVlkNtWVVYtkGCUd2ay1S8Tz1PC4DnTbhJRGVsv13OIC\niS4P0mZTRasHugRyqGXhKz2kRMkq3xCS099gg7X7Nq/l3YabPJ7waGCmN9unH/8P\nCh9NuOTRzL8ZX4kB/dlq6T9GHCRpomVqaHFj5Q9xYYOmi5f+oARL0Vs64QKBgHiw\njZBCItg2/9Gog9g/guviUHr+7pxi9xzOUDUBwkX7ixI0SviJkNBeIdbb7D6yTJpw\nUTqaTCPgHg89cKCWsfgvmwhGxYnDkdoMqasV9mJxO7UXXuTU4W+Iaz2I8RzoTnKC\ni4H/MEVvLTEy2lwjTZ13rpUMI2I6qp/mu1YqwPiJAoGAAnGW/mmW9deMnZ06Tpsu\nLyh0Xc+mNyAaQywlAtixEQ4ksGye8B8eHRXzJgfZbnyjSSjxTc47F4pUHzVQuVHB\nltRdySPaAEFKCTEMPR4xW/EElt7soatwE4fnfltrueeSQ7zeW3yoXQ4X5phgo83U\nEAdCh3UTZ2Y7dq6cOGYI+Cw=\n-----END PRIVATE KEY-----\n",
    "client_email": "ontaria@ontaria.iam.gserviceaccount.com",
    "client_id": "110355321260455765835",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ontaria%40ontaria.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# API configuration
SPREADSHEET_ID = '1LAmSMJj-RLvZAUTtyG_jTXw1XhUHcS9mHxNMVK7d7So'
HEADERS = {"x-api-key": "eff7a67f-0f6c-4779-9db4-bbe3274dd63a"}
BASE_URL = "https://api.gomotive.com"
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

# API Endpoints
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

def get_credentials_from_dict():
    """Create a temporary JSON file from the credential dictionary"""
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    temp_file_path = temp_file.name

    # Write the JSON credentials to the temporary file
    with open(temp_file_path, 'w') as f:
        json.dump(SERVICE_ACCOUNT_JSON, f)

    # Return the path to the temporary file
    return temp_file_path

def flatten_json(nested_json, parent_key='', sep='.'):
    """Flatten nested JSON objects into a single level dictionary"""
    flattened_dict = {}
    if isinstance(nested_json, list):
        for i, item in enumerate(nested_json):
            new_key = f"{parent_key}[{i}]" if parent_key else str(i)
            flattened_dict.update(flatten_json(item, new_key, sep=sep))
    elif isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            flattened_dict.update(flatten_json(value, new_key, sep=sep))
    else:
        flattened_dict[parent_key] = nested_json
    return flattened_dict

def fetch_and_process_data(endpoint):
    """Fetch data from API endpoint and process it into a DataFrame"""
    print(f"Fetching: {endpoint['url']}")
    url = f"{BASE_URL}{endpoint['url']}"
    try:
        response = requests.get(url, headers=HEADERS, params=endpoint['params'])
        response.raise_for_status()
        data = response.json()
        if endpoint['data_key'] in data:
            items = data[endpoint['data_key']]
            processed = [flatten_json(item) for item in items]
            return pd.DataFrame(processed)
        else:
            print(f"❌ Missing key '{endpoint['data_key']}'")
    except Exception as e:
        print(f"❌ Error: {e}")
    return pd.DataFrame()

def main():
    # Create temporary credentials file
    temp_creds_path = get_credentials_from_dict()

    try:
        # Setup Google Sheets API client
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file(temp_creds_path, scopes=SCOPES)
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)

        # Process each endpoint
        for endpoint in ENDPOINTS:
            df = fetch_and_process_data(endpoint)
            sheet_name = endpoint['sheet_name']
            print(f"📊 Writing {len(df)} rows to sheet: {sheet_name}")
            try:
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    worksheet.clear()
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
                set_with_dataframe(worksheet, df)
            except Exception as e:
                print(f"❌ Error writing to sheet {sheet_name}: {e}")
        print("✅ All data saved to Google Sheets.")

    finally:
        # Clean up the temporary file
        if os.path.exists(temp_creds_path):
            os.unlink(temp_creds_path)
            print("🧹 Cleaned up temporary credentials file")

if __name__ == "__main__":
    main()
