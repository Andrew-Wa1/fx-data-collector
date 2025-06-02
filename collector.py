import requests
import pandas as pd
import time
import os
import subprocess
from datetime import datetime

print("🔁 Starting FX Data Collector...")

# Load API key from environment
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    print("❌ No API key found! Set API_KEY in your environment.")
    exit()

DATA_FILE = "EUR_USD_live.csv"

def fetch_fx_rate():
    url = f"https://api.fastforex.io/fetch-one?from=EUR&to=USD&api_key={API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("result") and data["result"].get("USD"):
            price = float(data["result"]["USD"])
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return timestamp, price
        else:
            print(f"⚠️ API response missing rate: {data}")
            return None, None
    except Exception as e:
        print(f"❌ Error fetching FX rate: {e}")
        return None, None

def save_to_csv(timestamp, price):
    row = {"Date": timestamp, "Price": price}
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_csv(DATA_FILE, index=False)
    print(f"✅ {timestamp} | EUR/USD: {price}")

def commit_and_push():
    try:
        subprocess.run(["git", "add", DATA_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "📈 Auto-update EUR/USD live data"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("🚀 Changes pushed to GitHub.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git error: {e}")

# Main loop
while True:
    timestamp, price = fetch_fx_rate()
    if timestamp and price:
        save_to_csv(timestamp, price)
        commit_and_push()
    time.sleep(60)
