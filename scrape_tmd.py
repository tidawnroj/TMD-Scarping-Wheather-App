import pandas as pd
import pdfplumber
import requests
import datetime

def scrape_tmd_weather_data(url="https://www.tmd.go.th/uploads/ReportsGenMetnet/Daily/DailyObserved7AM.pdf", output_file="tmd_weather_data.csv"):
    try:
        print(f"Downloading PDF from {url}...")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        pdf_path = "DailyObserved7AM.pdf"
        with open(pdf_path, "wb") as f:
            f.write(response.content)
            
        print("Download successful. Parsing PDF...")
        
        all_data = []
        current_region = "Unknown"
        
        # Define Columns based on typical structure
        columns = ["Region", "Station", "Pressure_hPa", "Temp_C", "Tmax_C", "Tmin_C", 
                   "Tx_dif", "Tn_dif", "Rain_mm", "R1Jan_mm", "RH_percent", "Wind_dir", "Wind_knot"]
        
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables:
                    continue
                    
                for table in tables:
                    for row in table:
                        if not row or len(row) < 3:
                            continue
                            
                        # Clean up row items
                        row_cleaned = [str(x).replace('\n', ' ').strip() if x is not None else '' for x in row]
                        
                        station_or_region = row_cleaned[0]
                        
                        # Skip header rows
                        if "สถานี" in station_or_region or station_or_region == "" or "NaN" in station_or_region or station_or_region == "None":
                            continue
                            
                        # If columns 1 and 2 are empty, it's likely a region header like "ภาคเหนือ"
                        if row_cleaned[1] == "" and row_cleaned[2] == "":
                            current_region = station_or_region
                            continue
                        
                        # Otherwise it's a data row
                        data_row = [current_region] + row_cleaned[:12]
                        
                        # Ensure we have the exact number of columns
                        if len(data_row) < len(columns):
                            data_row.extend([''] * (len(columns) - len(data_row)))
                        elif len(data_row) > len(columns):
                            data_row = data_row[:len(columns)]
                            
                        all_data.append(data_row)
        
        if all_data:
            df = pd.DataFrame(all_data, columns=columns)
            # Add extraction date
            df['Extraction_Date'] = datetime.datetime.now().strftime("%Y-%m-%d")
            
            import os
            if os.path.isfile(output_file):
                df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
                print(f"Data successfully appended to {output_file}. Added {len(df)} rows.")
            else:
                df.to_csv(output_file, index=False, encoding='utf-8-sig') # utf-8-sig helps Excel display Thai characters format correctly
                print(f"Data successfully extracted to {output_file}. Extracted {len(df)} rows.")
            return True
        else:
            print("No data extracted.")
            return False
            
    except Exception as e:
        print(f"Error occurred: {e}")
        return False

if __name__ == "__main__":
    scrape_tmd_weather_data()
