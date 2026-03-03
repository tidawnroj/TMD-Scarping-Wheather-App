import pandas as pd
import pdfplumber
import requests
import datetime
import io
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from functools import wraps

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def with_retry(max_retries=3, base_delay=2, max_delay=30, backoff_factor=2):
    """Decorator to retry a function with exponential backoff upon gspread errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Specifically catch gspread exceptions, particularly 503 HTTP errors
                    if isinstance(e, gspread.exceptions.APIError):
                        error_msg = str(e)
                        if "503" in error_msg or "502" in error_msg or "500" in error_msg or "429" in error_msg:
                            if attempt == max_retries:
                                logging.error(f"Failed after {max_retries} retries: {e}")
                                raise
                            logging.warning(f"API Error ({error_msg}). Retrying in {delay}s (Attempt {attempt+1}/{max_retries})...")
                            time.sleep(delay)
                            delay = min(delay * backoff_factor, max_delay)
                            continue
                    
                    # If it's another type of Google API network failure
                    if "Connection" in str(e) or "Timeout" in str(e):
                        if attempt == max_retries:
                            logging.error(f"Network error after {max_retries} retries: {e}")
                            raise
                        logging.warning(f"Network Error. Retrying in {delay}s (Attempt {attempt+1}/{max_retries})...")
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                        continue
                        
                    # If it's not a transient/retriable error, raise immediately
                    logging.error(f"Non-retriable error: {e}")
                    raise
        return wrapper
    return decorator

def clean_thai_text(text):
    if not isinstance(text, str):
        return text
    # Replace known CID font issues from TMD PDFs
    text = text.replace("(cid:286)า", "ำ")
    text = text.replace("(cid:286)", "ํ")
    text = text.replace("(cid:270)", "ู")
    text = text.replace("(cid:269)", "ุ")
    text = text.replace("(cid:268)", "ื")
    text = text.replace("(cid:273)", "เ")
    text = text.replace("(cid:214)", "ก")
    text = text.replace("(cid:356)", "ฐ")
    return text

# --- Google Sheets Config ---
# Path to your Service Account JSON file
CREDENTIALS_FILE = "credentials/credentials.json"
# The exact name of your Google Sheet
SHEET_NAME = "TMD Weather Data" 
# ----------------------------

def authenticate_google_sheets():
    """Authenticate with Google Sheets API and return the sheet object."""
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: Credentials file not found at {CREDENTIALS_FILE}")
        return None
        
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scopes)
    client = gspread.authorize(creds)
    
    @with_retry(max_retries=4, base_delay=3)
    def open_sheet_with_retry():
        return client.open(SHEET_NAME)

    try:
        # Open the full spreadsheet instead of just sheet1
        spreadsheet = open_sheet_with_retry()
        return spreadsheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Google Sheet '{SHEET_NAME}' not found.")
        print("Make sure you have shared it with the service account email.")
        return None

def scrape_tmd_weather_data(url="https://www.tmd.go.th/uploads/ReportsGenMetnet/Daily/DailyObserved7AM.pdf"):
    print("Step 1: Authenticating with Google Sheets...")
    spreadsheet = authenticate_google_sheets()
    if not spreadsheet:
        return
        
    try:
        print(f"Step 2: Downloading PDF from {url}...")
        response = requests.get(url, timeout=15)
        response.raise_for_status() 
        
        pdf_file = io.BytesIO(response.content)
        
        print("Step 3: Parsing PDF for data...")
        all_data = []
        extraction_date = datetime.datetime.now().strftime("%Y-%m-%d")

        with pdfplumber.open(pdf_file) as pdf:
            current_region = ""
            for i, page in enumerate(pdf.pages):
                table = page.extract_table()
                if not table:
                    continue
                
                start_row = 3 if i == 0 else 1
                
                for row_idx in range(start_row, len(table)):
                    row = table[row_idx]
                    if not row or len(row) < 3:
                        continue
                        
                    col0 = clean_thai_text(str(row[0]).strip()) if row[0] else ""
                    col1 = clean_thai_text(str(row[1]).strip()) if row[1] else ""
                    col2 = clean_thai_text(str(row[2]).strip()) if row[2] else ""
                    
                    if col0 and "ภาค" in col0 and not col1 and not col2:
                        current_region = col0
                        continue
                        
                    station = clean_thai_text(col0)
                    pressure = str(row[1]).strip() if len(row) > 1 else ""
                    temp = row[2] if len(row) > 2 else ""
                    tmax = row[3] if len(row) > 3 else ""
                    tx_dif = row[4] if len(row) > 4 else ""
                    tmin = row[5] if len(row) > 5 else ""
                    tn_dif = row[6] if len(row) > 6 else ""
                    rain = row[7] if len(row) > 7 else ""
                    r1jan = row[8] if len(row) > 8 else ""
                    rh = row[9] if len(row) > 9 else ""
                    wind_dir = row[10] if len(row) > 10 else ""
                    wind_knot = row[11] if len(row) > 11 else ""
                    
                    
                    safe_station_name = station.replace("/", "_").strip()
                    if not safe_station_name or safe_station_name == 'None' or safe_station_name == 'สถานี':
                        continue
                        
                    # Skip the footer remarks at the bottom of the PDF
                    if "หมายเหตุ" in safe_station_name or len(safe_station_name) > 50:
                        continue
                        
                    all_data.append([safe_station_name, [
                        current_region, station, pressure, temp, tmax, tmin, 
                        tx_dif, tn_dif, rain, r1jan, rh, wind_dir, wind_knot, extraction_date
                    ]])
                    
        if all_data:
            print(f"Step 4: Uploading data for {len(all_data)} stations to Google Sheet '{SHEET_NAME}'...")
            
            header = ["Region", "Station", "Pressure_hPa", "Temp_C", "Tmax_C", "Tmin_C", 
                      "Tx_dif", "Tn_dif", "Rain_mm", "R1Jan_mm", "RH_percent", "Wind_dir", "Wind_knot", "Extraction_Date"]
            
            # Fetch all existing worksheets to see which stations already have tabs
            @with_retry(max_retries=3)
            def fetch_worksheets():
                return {ws.title: ws for ws in spreadsheet.worksheets()}
                
            existing_worksheets = fetch_worksheets()
            
            # --- AGGREGATE "ALL_STATIONS" TAB ---
            print("Uploading aggregated data to 'ALL_STATIONS' tab...")
            all_stations_title = "ALL_STATIONS"
            @with_retry(max_retries=3)
            def update_or_create_all_stations():
                if all_stations_title in existing_worksheets:
                    ws_all = existing_worksheets[all_stations_title]
                    ws_all.append_rows([row for _, row in all_data])
                else:
                    ws_all = spreadsheet.add_worksheet(title=all_stations_title, rows="2000", cols="20")
                    existing_worksheets[all_stations_title] = ws_all
                    ws_all.append_rows([header] + [row for _, row in all_data])
            
            update_or_create_all_stations()
            # ------------------------------------
            
            @with_retry(max_retries=2)
            def update_station_sheet(sheet_title, row, headers):
                if sheet_title in existing_worksheets:
                    ws = existing_worksheets[sheet_title]
                    ws.append_row(row)
                else:
                    print(f"  -> Creating new tab for {sheet_title}...")
                    ws = spreadsheet.add_worksheet(title=sheet_title, rows="1000", cols="20")
                    existing_worksheets[sheet_title] = ws
                    # Append header and row for the new sheet
                    ws.append_rows([headers, row])

            count = 0
            for safe_station_name, row in all_data:
                try:
                    # Truncate title if extremely long, Google Sheets max tab name length is 100
                    sheet_title = safe_station_name[:100]
                    
                    update_station_sheet(sheet_title, row, header)
                        
                    count += 1
                    # Avoid hitting Google Sheets API rate limits (60 req/min/user)
                    time.sleep(1)
                    
                    if count % 10 == 0:
                        print(f"Progress: Uploaded {count}/{len(all_data)} stations...", flush=True)
                        
                except Exception as e:
                    print(f"Failed to upload data for {sheet_title}: {e}", flush=True)
                
            print(f"Done! Successfully updated {count} tabs. 🎉", flush=True)
        else:
            print("No data extracted from PDF.")
            
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    scrape_tmd_weather_data()
