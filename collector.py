import os
import time
import requests
import pandas as pd
from datetime import datetime
from git import Repo
import time
from datetime import datetime

print("✅ Collector started...")

while True:
    print(f"🕒 Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # Simulate work here
    time.sleep(60)

# === CONFIGURATION ===
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.fastforex.io/fetch-one"
FROM = "EUR"
TO = "USD"
INTERVAL = 60  # seconds
DATA_DIR = "historical_data"
CSV_FILE = os.path.join(DATA_DIR, f"{FROM}{TO}_live.csv")
REPO_DIR = os.getcwd()

# === Ensure data directory exists ===
os.makedirs(DATA_DIR, exist_ok=True)

# === Git Repo Setup ===
repo = Repo(REPO_DIR)

def fetch_rate():
    try:
        params = {"from": FROM, "to": TO, "api_key": API_KEY}
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        if "result" in data and TO in data["result"]:
            price = float(data["result"][TO])
            return price
        else:
            print("API error:", data)
            return None
    except Exception as e:
        print("Request failed:", e)
        return None

def save_rate(price):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df_new = pd.DataFrame([{"Date": timestamp, "Price": price}])

    if os.path.exists(CSV_FILE):
        df_existing = pd.read_csv(CSV_FILE)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(CSV_FILE, index=False)
    print(f"✅ Saved: {timestamp}, {price}")

def commit_to_git():
    repo.git.add(all=True)
    repo.index.commit(f"Update FX data at {datetime.utcnow()}")
    origin = repo.remote(name="origin")
    origin.push()
    print("📤 Pushed to GitHub.")

# === Loop ===
while True:
    fx_price = fetch_rate()
    if fx_price:
        save_rate(fx_price)
        commit_to_git()
    else:
        print("⚠️ Failed to fetch FX rate.")
    time.sleep(INTERVAL)
