import os
import requests
import pandas as pd
import time
from datetime import datetime
from git import Repo

# GitHub Repository Info (make sure these are set as environment variables in Render)
GIT_USERNAME = os.getenv("GIT_USERNAME")
GIT_EMAIL = os.getenv("GIT_EMAIL")
GIT_TOKEN = os.getenv("GIT_TOKEN")
REPO_DIR = "/home/render/fx-data-collector"  # Update based on your project structure
REPO_NAME = "fx-data-collector"
REPO_URL = f"https://{GIT_USERNAME}:{GIT_TOKEN}@github.com/{GIT_USERNAME}/{REPO_NAME}.git"

# API URL and Params for fetching data
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"  # Example endpoint, replace with actual
PAIR = "EUR"
FILE_NAME = f"{PAIR}_exchange_rate_data.csv"
FILE_PATH = os.path.join(REPO_DIR, FILE_NAME)

def fetch_fx_data():
    try:
        response = requests.get(API_URL)
        data = response.json()
        price = data['rates'][PAIR]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"✅ {PAIR} rate: {price} at {timestamp}")
        return timestamp, price
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None, None

def update_git_repo():
    try:
        repo = Repo(REPO_DIR)
        repo.git.add(FILE_NAME)
        repo.index.commit("Update exchange rate data")
        origin = repo.remotes.origin
        origin.push()
        print("✅ Successfully pushed data to GitHub!")
    except Exception as e:
        print(f"❌ Error pushing data to GitHub: {e}")

def write_to_csv(timestamp, price):
    try:
        # Load existing data or create a new dataframe
        if os.path.exists(FILE_PATH):
            df = pd.read_csv(FILE_PATH)
        else:
            df = pd.DataFrame(columns=["Date", "Price"])

        # Append new data
        new_data = pd.DataFrame([[timestamp, price]], columns=["Date", "Price"])
        df = pd.concat([df, new_data], ignore_index=True)

        # Save to CSV
        df.to_csv(FILE_PATH, index=False)
        print(f"✅ Data saved to {FILE_PATH}")
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")

def main():
    # Make sure to set your Git config for GitHub
    os.system(f"git config --global user.email {GIT_EMAIL}")
    os.system(f"git config --global user.name {GIT_USERNAME}")

    while True:
        timestamp, price = fetch_fx_data()
        if timestamp and price:
            write_to_csv(timestamp, price)
            update_git_repo()
        time.sleep(60)  # Wait for a minute before fetching again

if __name__ == "__main__":
    main()
