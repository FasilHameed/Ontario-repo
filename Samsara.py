import requests
import pandas as pd
from datetime import datetime, timezone
import json
import re
import gspread
import os
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


# API Key and Base URL
API_KEY = os.environ.get("api_key_samsara")
BASE_URL = "https://api.samsara.com"


# Google Sheet ID - Replace with your sheet ID
SHEET_ID = "1-V3uJtqhQTf-v3PIZEf95CLQ3YooDrVoYGljuk-g_ss"


# Load Google Service Account JSON credentials from environment variable
SERVICE_ACCOUNT_JSON = json.loads(os.environ.get("Samsara_json"))
 
# Headers for API authentication
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}


# Function to get current time in ISO 8601 format
def get_current_time():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Function to fetch data from API
def fetch_data(endpoint, params=None):
    url = BASE_URL + endpoint
    response = requests.get(url, headers=HEADERS, params=params)
   
    if response.status_code == 200:
        try:
            return response.json()  # Return parsed JSON response
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {endpoint}")
            return {}
    else:
        print(f"Error {response.status_code} for {endpoint}: {response.text}")
        return {}


# Function to handle nested structures and extract data generically
def flatten_json(nested_json, prefix=''):
    """Flatten a nested JSON structure into a flat dictionary"""
    out = {}
   
    def flatten(x, name=''):
        # Handle lists differently than dicts
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            # If it's a list of dictionaries, process each item
            if x and isinstance(x[0], dict):
                # We'll return the list as-is for later processing
                out[name[:-1]] = x
            else:
                # For simple lists, join the values
                out[name[:-1]] = ', '.join(str(i) for i in x if i is not None)
        else:
            out[name[:-1]] = x
   
    flatten(nested_json, prefix)
    return out


# Function to handle HOS violations specific structure
def handle_hos_violations(data):
    """Extract violations from the specific HOS violations API structure"""
    violations_list = []
   
    # Check if we have the expected structure
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        # Iterate through each data item
        for item in data["data"]:
            # Check if this item contains a violations array
            if "violations" in item and isinstance(item["violations"], list):
                for violation in item["violations"]:
                    violation_data = {}
                   
                    # Extract driver information
                    if "driver" in violation and isinstance(violation["driver"], dict):
                        violation_data["driver_id"] = violation["driver"].get("id", "")
                        violation_data["driver_name"] = violation["driver"].get("name", "")
                   
                    # Extract day information
                    if "day" in violation and isinstance(violation["day"], dict):
                        violation_data["day_startTime"] = violation["day"].get("startTime", "")
                        violation_data["day_endTime"] = violation["day"].get("endTime", "")
                   
                    # Extract other violation fields
                    violation_data["type"] = violation.get("type", "")
                    violation_data["description"] = violation.get("description", "")
                    violation_data["violationStartTime"] = violation.get("violationStartTime", "")
                    violation_data["durationMs"] = violation.get("durationMs", "")
                   
                    # Add any other fields present in the violation
                    for key, value in violation.items():
                        if key not in ["driver", "day"] and key not in violation_data:
                            if isinstance(value, (str, int, float, bool)):
                                violation_data[key] = value
                   
                    violations_list.append(violation_data)
   
    # If we found violations, return them as a DataFrame
    if violations_list:
        return pd.DataFrame(violations_list)
   
    # Fallback to generic handling if our specific approach didn't work
    return normalize_api_response(data)


# Function to normalize API response to DataFrame regardless of structure
def normalize_api_response(data):
    """Convert any API response format to a pandas DataFrame"""
    # Check if data is in a common wrapper format
    if isinstance(data, dict):
        # Many APIs use a "data" field to contain the actual results
        if "data" in data and isinstance(data["data"], list):
            data = data["data"]
        # Some APIs use "results" field
        elif "results" in data and isinstance(data["results"], list):
            data = data["results"]
        # Some may use "items"
        elif "items" in data and isinstance(data["items"], list):
            data = data["items"]
       
    # Handle list of objects (most common API response format)
    if isinstance(data, list):
        if data:
            # Check if this is a list of simple values or list of objects
            if isinstance(data[0], dict):
                # Process each item to handle nested structures
                flattened_data = []
                for item in data:
                    flat_item = flatten_json(item)
                   
                    # Process embedded lists of dictionaries
                    for key, value in list(flat_item.items()):
                        if isinstance(value, list) and value and isinstance(value[0], dict):
                            # Remove the original nested list
                            del flat_item[key]
                           
                            # If there's just one item, flatten it into the parent
                            if len(value) == 1:
                                for sub_key, sub_value in flatten_json(value[0], key + '_').items():
                                    flat_item[sub_key] = sub_value
                   
                    flattened_data.append(flat_item)
               
                return pd.DataFrame(flattened_data)
            else:
                # For a list of simple values, convert to single column DataFrame
                return pd.DataFrame({f"value": data})
        else:
            # Empty list
            return pd.DataFrame()
   
    # Handle single object response
    elif isinstance(data, dict):
        # Flatten the nested dictionary
        flat_data = flatten_json(data)
       
        # Process embedded lists of dictionaries
        for key, value in list(flat_data.items()):
            if isinstance(value, list) and value and isinstance(value[0], dict):
                # Create a separate DataFrame for this nested list
                nested_df = pd.DataFrame(value)
               
                # For simplicity in the final output, we'll stringify this nested data
                flat_data[key] = f"[{len(value)} items]"
       
        return pd.DataFrame([flat_data])
   
    # Fallback for unknown formats
    return pd.DataFrame()


# Format DataFrame for better Excel presentation
def format_dataframe(df):
    """Clean up and format the DataFrame for better presentation"""
    if df.empty:
        return df
   
    # Format column names
    df.columns = [format_column_name(col) for col in df.columns]
   
    # Handle timestamp columns
    for col in df.columns:
        # Check if column contains timestamp strings
        if df[col].dtype == 'object':
            sample_values = df[col].dropna().head(5).astype(str)
            timestamp_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
           
            if sample_values.str.contains(timestamp_pattern).any():
                try:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
           
            # Check for duration in milliseconds and convert to readable format
            ms_pattern = r'^[0-9]+$'
            if col.lower().endswith('ms') or 'duration' in col.lower():
                if sample_values.str.match(ms_pattern).all():
                    try:
                        df[col] = df[col].apply(
                            lambda x: str(int(int(x)/3600000)) + ":" +
                                    str(int((int(x)%3600000)/60000)).zfill(2)
                                    if pd.notnull(x) and str(x).isdigit() else x
                        )
                    except:
                        pass
   
    return df


# Function to format column names
def format_column_name(col_name):
    if isinstance(col_name, str):
        # Replace camelCase with spaces
        col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', col_name)
        # Replace underscores with spaces
        col_name = col_name.replace('_', ' ')
        # Title case
        col_name = col_name.title()
    return col_name


# API Endpoints and Parameters
endpoints = {
    "drivers": "/fleet/drivers",
    "hos_clocks": "/fleet/hos/clocks",
    "hos_logs": "/fleet/hos/logs",
    "hos_violations": "/fleet/hos/violations",
    "hos_daily_logs": "/fleet/hos/daily-logs",
   
    # New endpoints from previous document
    "dvir_defect_types": "/defect-types",
    "safety_events": "/fleet/safety-events",
    "safety_events_audit_logs": "/fleet/safety-events/audit-logs/feed"
}


# Required parameters for specific APIs
params_dict = {
    "hos_violations": {
        "startTime": "2024-01-01T00:00:00Z",
        "endTime": get_current_time()
    },
    "hos_daily_logs": {
        "startDate": "2024-01-01",
        "endDate": datetime.now().strftime("%Y-%m-%d")
    },
   
    # Parameters for new endpoints
    "safety_events": {
        "startTime": "2024-01-01T00:00:00Z",
        "endTime": get_current_time()
    },
    "safety_events_audit_logs": {
        "startTime": "2024-01-01T00:00:00Z"
    }
}


# Setup Google Sheets connection
def setup_gspread():
    """Set up Google Sheets API connection"""
    # Define the scopes
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
   
    # Create credentials from the service account info
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=scopes)
   
    # Authenticate with gspread
    client = gspread.authorize(creds)
   
    # Open the spreadsheet
    spreadsheet = client.open_by_key(SHEET_ID)
   
    return spreadsheet


# Function to update or create worksheet with data
def update_sheet(spreadsheet, sheet_name, df):
    """Update or create a worksheet with the given dataframe"""
   
    # Try to get the worksheet, create it if it doesn't exist
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Found existing worksheet: {sheet_name}")
       
        # Clear the existing worksheet content
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        # Create a new worksheet
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        print(f"Created new worksheet: {sheet_name}")
   
    # Write the dataframe to the worksheet
    if not df.empty:
        # Set the dataframe to the worksheet
        set_with_dataframe(worksheet, df)
        print(f"Updated worksheet {sheet_name} with {len(df)} rows")
    else:
        print(f"No data available for {sheet_name}")


# Main execution
def main():
    # Check if API key and Service Account JSON are available
    if not API_KEY:
        print("Error: API Key not found in environment variables. Please set 'api_key_samsara'.")
        return
    
    if not os.environ.get("Samsara.json"):
        print("Error: Service Account JSON not found in environment variables. Please set 'Samsara.json'.")
        return
        
    # Setup Google Sheets connection
    try:
        spreadsheet = setup_gspread()
        print(f"Connected to Google Sheet: {spreadsheet.title}")
       
        # Fetch data for each endpoint and update the corresponding sheet
        for sheet_name, endpoint in endpoints.items():
            print(f"\nProcessing: {sheet_name}")
            params = params_dict.get(sheet_name, None)  # Get params if required
            data = fetch_data(endpoint, params)
           
            # Use special handler for HOS violations
            if sheet_name == "hos_violations":
                df = handle_hos_violations(data)
            else:
                # Convert to DataFrame using the generic approach
                df = normalize_api_response(data)
           
            # Format DataFrame for better presentation
            df = format_dataframe(df)
           
            if not df.empty:
                print(f"Found {len(df)} rows for {sheet_name}")
               
                # Check if dataframe has too many columns for Google Sheets (limit is 18,278)
                if len(df.columns) > 18000:
                    print(f"Warning: {sheet_name} has too many columns ({len(df.columns)}). Truncating to 18000 columns.")
                    df = df.iloc[:, :18000]
               
                # Update Google Sheet
                update_sheet(spreadsheet, sheet_name, df)
            else:
                print(f"No valid data for {sheet_name}")
               
        print("\nAll sheets have been updated successfully.")
   
    except Exception as e:
        print(f"Error in main execution: {str(e)}")


if __name__ == "__main__":
    main()
