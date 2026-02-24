import pandas as pd
import pdfplumber
import requests
import datetime
import io
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
    
    try:
        # Open the specific sheet by name
        sheet = client.open(SHEET_NAME).sheet1
        return sheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Google Sheet '{SHEET_NAME}' not found.")
        print("Make sure you have shared it with the service account email.")
        return None

def scrape_tmd_weather_data(url="https://www.tmd.go.th/uploads/ReportsGenMetnet/Daily/DailyObserved7AM.pdf"):
    print("Step 1: Authenticating with Google Sheets...")
    sheet = authenticate_google_sheets()
    if not sheet:
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
                        
                    col0 = str(row[0]).strip() if row[0] else ""
                    col1 = str(row[1]).strip() if row[1] else ""
                    col2 = str(row[2]).strip() if row[2] else ""
                    
                    if col0 and "ภาค" in col0 and not col1 and not col2:
                        current_region = col0
                        continue
                        
                    station = col0
                    pressure = row[2] if len(row) > 2 else ""
                    temp = row[3] if len(row) > 3 else ""
                    tmax = row[4] if len(row) > 4 else ""
                    tmin = row[5] if len(row) > 5 else ""
                    tx_dif = row[6] if len(row) > 6 else ""
                    tn_dif = row[7] if len(row) > 7 else ""
                    rain = row[8] if len(row) > 8 else ""
                    r1jan = row[9] if len(row) > 9 else ""
                    rh = row[10] if len(row) > 10 else ""
                    wind_dir = row[11] if len(row) > 11 else ""
                    wind_knot = row[12] if len(row) > 12 else ""
                    
                    if not station or station == 'None' or station == 'สถานี':
                        continue
                        
                    all_data.append([
                        current_region, station, pressure, temp, tmax, tmin, 
                        tx_dif, tn_dif, rain, r1jan, rh, wind_dir, wind_knot, extraction_date
                    ])
                    
        if all_data:
            print(f"Step 4: Uploading {len(all_data)} rows to Google Sheet '{SHEET_NAME}'...")
            
            # Check if sheet is empty and add headers
            header = ["Region", "Station", "Pressure_hPa", "Temp_C", "Tmax_C", "Tmin_C", 
                      "Tx_dif", "Tn_dif", "Rain_mm", "R1Jan_mm", "RH_percent", "Wind_dir", "Wind_knot", "Extraction_Date"]
            
            existing_data = sheet.get_all_values()
            
            # If completely empty, insert headers and the data
            if not existing_data:
                sheet.update('A1', [header] + all_data)
                print("Added headers and new data.")
            else:
                # Append rows directly to the bottom
                sheet.append_rows(all_data)
                print("Data appended successfully.")
                
            print("Done! 🎉")
        else:
            print("No data extracted from PDF.")
            
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    scrape_tmd_weather_data()
