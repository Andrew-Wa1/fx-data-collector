import requests
import pandas as pd
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (API key)
load_dotenv()

API_KEY = os.getenv("API_KEY")  # Your API key from FastForex
URL = "https://api.fastforex.io/fetch-all?api_key=" + API_KEY
SAVE_FOLDER = "live_data"

# Ensure the save folder exists
os.makedirs(SAVE_FOLDER, exist_ok=True)

def fetch_and_save():
    try:
        response = requests.get(URL)
        data = response.json()
        rates = data.get("results", {})
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # UTC time format
        
        # Iterate through each currency pair and save data
        for pair, price in rates.items():
            filename = os.path.join(SAVE_FOLDER, f"{pair}_live.csv")
            df = pd.DataFrame([{"Date": timestamp, "Price": price}])
            
            # Append to the file if it exists; otherwise, create a new one
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False)
            else:
                df.to_csv(filename, index=False)
        
        print(f"[{timestamp}] Data saved.")
    except Exception as e:
        print("Error fetching data:", e)

while True:
    fetch_and_save()  # Fetch and save the data
    time.sleep(60)  # Wait 60 seconds before the next data fetch
