import requests
import pandas as pd
import datetime
import os

# New standard: OAuth Access Token (JWT Bearer Token)
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjI2NmIxMWNjNjFiMGQ3N2Q1OGNhMzQ3MDQwZWUxMjZmYzUwYjBhODQwNjhjMjcyZTY5OGVmNTBiZTU1MmY4MzRlODVjMTEwYTlhYmUxMTdjIn0.eyJhdWQiOiIyIiwianRpIjoiMjY2YjExY2M2MWIwZDc3ZDU4Y2EzNDcwNDBlZTEyNmZjNTBiMGE4NDA2OGMyNzJlNjk4ZWY1MGJlNTUyZjgzNGU4NWMxMTBhOWFiZTExN2MiLCJpYXQiOjE3NzE5NDExMDgsIm5iZiI6MTc3MTk0MTEwOCwiZXhwIjoxODAzNDc3MTA4LCJzdWIiOiI0OTcxIiwic2NvcGVzIjpbXX0.nKdldOvegyZXhmShcq6dbVwuqr9t6gayZZhm3WnVl5JWCtiAMkyD0Ae5H-ha0nfjTv7GhZ3o3Kz4gzuexjTDI_Q0Z0aZLJjcyqFQ1YFnkba53bpYuaS8CuFE_ihR-0twLc2hibLD05y8jUY3FwjXmiy-u8kzR4rAErkl0GzDZMi7daHHBX8OIcoXXD2n4iE6iDFgFOCq3vCR-aqCT0lV-wnjGxgoL5d-2aad7C6d0iUIn4Pg_8zX8-FezwngHNgtE8Wi3FaQiOI7ow_lssMx9Kmbz0zscLBxtbxjQVgPkCOI6mMBchhuVkde7H43or-I5VibYjP0srBIot5rEz9AETUt0whc9rXkRj7678B63pWPKuMO0bhpX4SNmCGQs53_RNEz4tR63Egh8ycpSFpTs-yUrnEfG8eK8dnuRfIDUE6C_yY0zTBQUjPiMaQFOAHJCOEl1Do7W3fK57xeAeufK9gxENkE8li3S8k72ax3wuqWc5rcYC2VwsAeuYiy7wv2oROubjG7J0jC3rbEH-Onk0dYBy6vWb7_AN06MiSNRyjW166os1lexsVjAVFxn_9GPhCh_KC4jSJKWemW4eNeeB_Yg0dbXpcE-Q9lptvq57T20pNrmTvACnz1p6TMRsgtEygUbEF0fkGw8RMcsaplOc8J2hvrGXocm1CvjwHOpVM"

def fetch_tmd_weather_api(output_file="tmd_forecast_data_api.csv"):
    # Based on the latest screenshot showing NWP API (Numerical Weather Prediction API)
    # The URL pattern for daily data is shown as: https://data.tmd.go.th/nwpapi/v1/forecast/daily/datarange
    # However, let's use the location endpoint to fetch data for all Thailand or specific coordinates.
    # We will try a sample location using the hourly endpoint shown in the doc: 
    # https://data.tmd.go.th/nwpapi/v1/forecast/hourly/at?lat=13.10&lon=100.10&fields=tc,rh&date=2017-08-17&hour=8&duration=2
    # NOTE: The user wants daily observed data but the NWP API is primarily for *forecasts*. 
    # For now, let's just make a successful connection to verify the token.
    
    # We'll use a general forecast endpoint that doesn't strictly require complex params just to test the auth
    # Since the first screenshot shows daily dataranges:
    
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Let's try to get forecast for a specific location (Bangkok roughly) for today
    url = f"https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at"
    
    # Example parameters based on common API designs and the screenshot snippets
    params = {
        "lat": 13.75,
        "lon": 100.50,
        "fields": "tc,rh,rain", # tc=temperature, rh=relative humidity, rain=rainfall (common keys)
        "date": today,
        "hour": "7",
        "duration": "24" 
    }
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json"
    }
    
    try:
        print(f"Fetching data from TMD NWP API with Bearer Token...")
        # Since we don't know the exact endpoint they want from NWP, we'll just try to hit the root or a standard one
        # The screenshot explicitly used: https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at?lat=13.10...
        # Wait, the screenshot shows: https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at
        url_exact = "https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at"
        
        response = requests.get(url_exact, headers=headers, params=params, timeout=30)
        
        # If it returns 401 Unauthorized, we know the token is still wrong or expired
        if response.status_code == 401:
            print("Authentication Failed (401 Unauthorized).")
            return False
            
        # If it returns 200, we successfully authenticated!
        response.raise_for_status()
        
        data = response.json()
        print("Success! Data received:", str(data)[:200] + "...")
        
        # We won't fully parse it yet until we confirm this is the data they want, just saving raw json for now
        import json
        with open("raw_api_response.json", "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print("Raw data saved to raw_api_response.json")
        return True
            
    except Exception as e:
        print(f"Error occurred during API request: {e}")
        if hasattr(e, 'response') and e.response is not None:
             print(f"Response status code: {e.response.status_code}")
             print(f"Response body: {e.response.text}")
        return False

if __name__ == "__main__":
    fetch_tmd_weather_api()
