import os
import time
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("API_KEY")
SAVE_FOLDER = "data"  # local folder to save files

os.makedirs(SAVE_FOLDER, exist_ok=True)

def fetch_fx():
    url = f"https://api.fastforex.io/fetch-one?from=EUR&to=USD&api_key={API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200 and data.get("result"):
            price = data["result"]["USD"]
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"✅ {timestamp} | EUR/USD: {price}")
            return timestamp, price
        else:
            print(f"❌ API error: {data}")
            return None, None
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        return None, None

def save_data(timestamp, price):
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(SAVE_FOLDER, f"EUR_USD_{today}.csv")
    df = pd.DataFrame([[timestamp, price]], columns=["Date", "Price"])
    if os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
    else:
        df.to_csv(filepath, index=False)

if __name__ == "__main__":
    while True:
        timestamp, price = fetch_fx()
        if timestamp and price:
            save_data(timestamp, price)
        time.sleep(60)
