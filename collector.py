import time
import requests
from datetime import datetime, timezone
from supabase_py import create_client
from dotenv import load_dotenv
import os

# Load .env values locally or from Render environment
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_DB_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_KEY = os.getenv("API_KEY")  # FastForex API Key
API_URL = "https://api.fastforex.io/fetch-one"

# Initialize Supabase client
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_client()

# Define currency pairs (56 total: 8 × 7)
currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]
pairs = [(base, quote) for base in currencies for quote in currencies if base != quote]

# Fetch rate from FastForex
def fetch_rate(base, quote):
    try:
        response = requests.get(API_URL, params={
            "from": base,
            "to": quote,
            "api_key": API_KEY
        })
        response.raise_for_status()
        data = response.json()
        return data["result"][quote]
    except Exception as e:
        print(f"[ERROR] Failed to fetch {base}/{quote}: {e}")
        return None

# Collector loop
def run_collector_loop(interval=60):
    print("🔁 Starting FX data collector loop...")
    while True:
        for base, quote in pairs:
            rate = fetch_rate(base, quote)
            if rate is None:
                continue

            timestamp = datetime.now(timezone.utc).isoformat()

            row = {
                "timestamp": timestamp,
                "base_currency": base,
                "quote_currency": quote,
                "rate": rate
            }

            try:
                supabase.table("fx_rates").insert([row]).execute()
                print(f"✅ Inserted {base}/{quote} @ {rate:.5f} ({timestamp})")
            except Exception as e:
                print(f"[ERROR] Failed to insert {base}/{quote}: {e}")

            time.sleep(0.1)  # small delay to avoid hammering API/Supabase

        print("⏳ Sleeping before next cycle...\n")
        time.sleep(interval)

if __name__ == "__main__":
    run_collector_loop()
