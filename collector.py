import requests
import pandas as pd
from datetime import datetime
import os
import subprocess

# Environment variables (set these in Render)
API_KEY = os.getenv("FASTFOREX_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_DIR = "/home/render/fx-data-collector"  # Adjust if your path is different
PAIR = "USD_EUR"
FILENAME = f"{PAIR}_live.csv"

def fetch_live_fx():
    url = f"https://api.fastforex.io/fetch-one?from=USD&to=EUR&api_key={API_KEY}"
    response = requests.get(url)
    data = response.json()
    rate = data.get("result", {}).get("EUR")
    if rate:
        return rate, datetime.utcnow()
    return None, None

def save_to_csv(rate, timestamp):
    path = os.path.join(REPO_DIR, FILENAME)
    df = pd.DataFrame([{"Date": timestamp.isoformat(), "Price": rate}])
    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)
    print(f"✅ Saved FX data: {rate} at {timestamp}")

def git_commit_and_push():
    try:
        subprocess.run(["git", "-C", REPO_DIR, "add", FILENAME], check=True)
        subprocess.run(["git", "-C", REPO_DIR, "commit", "-m", "Update FX data"], check=True)
        subprocess.run(["git", "-C", REPO_DIR, "push", "origin", "main"], check=True)
        print("🚀 Git push complete.")
    except subprocess.CalledProcessError as e:
        print("⚠️ Git push failed:", e)

if __name__ == "__main__":
    rate, timestamp = fetch_live_fx()
    if rate:
        save_to_csv(rate, timestamp)
        git_commit_and_push()
    else:
        print("❌ Failed to fetch FX rate.")
