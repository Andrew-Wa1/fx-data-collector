import os
import time
import requests
import pandas as pd
from datetime import datetime
from git import Repo

# === CONFIGURATION ===
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.fastforex.io/fetch-one"
FROM = "EUR"
TO = "USD"
INTERVAL = 60  # in seconds
DATA_DIR = "historical_data"
CSV_FILE = os.path.join(DATA_DIR, f"{FROM}{TO}_live.csv")
REPO_DIR = os.getcwd()

# === SETUP ===
os.makedirs(DATA_DIR, exist_ok=True)
repo = Repo(REPO_DIR)

def fetch_rate():
    try:
        params = {"from": FROM, "to": TO, "api_key": API_KEY}
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if "result" in data and TO in data["result"]:
            price = float(data["result"][TO])
            print(f"💱 Price fetched: {price}")
            return price
        else:
            print("❌ API returned unexpected data:", data)
            return None
    except Exception as e:
        print("❌ Fetch failed:", e)
        return None

def save_rate(price):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    new_data = pd.DataFrame([{"Date": timestamp, "Price": price}])
    
    if os.path.exists(CSV_FILE):
        existing_data = pd.read_csv(CSV_FILE)
        combined = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        combined = new_data

    combined.to_csv(CSV_FILE, index=False)
    print(f"✅ Saved to CSV: {timestamp}, {price}")

def commit_to_git():
    try:
        repo.git.add(A=True)
        repo.index.commit(f"📈 Update {FROM}/{TO} at {datetime.utcnow()}")
        origin = repo.remote(name="origin")
        origin.push()
        print("🚀 Changes pushed to GitHub.")
    except Exception as e:
        print("⚠️ Git commit/push failed:", e)

# === MAIN LOOP ===
while True:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"🕒 Fetching data at {now}")

    rate = fetch_rate()
    if rate:
        save_rate(rate)
        commit_to_git()
    else:
        print("⚠️ No rate to save.")
    
    time.sleep(INTERVAL)
