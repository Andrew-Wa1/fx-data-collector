import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FASTFOREX_API_KEY")
CURRENCY_PAIR = "EURUSD"  # Adjust as needed
BASE_URL = "https://api.fastforex.io/fetch-one"

def get_fx_rate():
    params = {
        "from": "EUR",
        "to": "USD",
        "api_key": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    if "result" in data and "USD" in data["result"]:
        timestamp = datetime.now().replace(second=0, microsecond=0)
        price = float(data["result"]["USD"])
        return {"Date": timestamp.isoformat(), "Price": price}
    else:
        print("API error:", data)
        return None

def save_to_csv(data):
    filename = f"EURUSD_live_{datetime.now().date()}.csv"
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        df = pd.DataFrame(columns=["Date", "Price"])
    df = pd.concat([df, pd.DataFrame([data])])
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    data = get_fx_rate()
    if data:
        save_to_csv(data)
        print("✅ Data saved:", data)
    else:
        print("❌ Failed to fetch data.")
