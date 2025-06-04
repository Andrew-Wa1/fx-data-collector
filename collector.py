import time
import requests
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
import os

print("🚀 Starting FX Collector script...")

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_DB_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_KEY = os.getenv("API_KEY")
API_URL = "https://api.fastforex.io/fetch-one"

# Debug: Show loaded env vars
print(f"SUPABASE_URL: {SUPABASE_URL}")
print(f"SUPABASE_KEY: {SUPABASE_KEY[:5]}...{SUPABASE_KEY[-5:]}")
print(f"API_KEY: {API_KEY[:5]}...{API_KEY[-5:]}")

# Create Supabase client
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_client()

# Currency pairs (8x7 = 56)
currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]
pairs = [(base, quote) for base in currencies for quote in currencies if base != quote]

# Fetch FX rate from FastForex
def fetch_rate(base, quote):
    try:
        print(f"🌐 Fetching {base}/{quote}...")
        response = requests.get(API_URL, params={
            "from": base,
            "to": quote,
            "api_key": API_KEY
        })
        response.raise_for_status()
        data = response.json()
        rate = data["result"][quote]
        print(f"📈 Received rate: {rate}")
        return rate
    except Exception as e:
        print(f"[ERROR] Failed to fetch {base}/{quote}: {e}")
        print("↪️ Response:", response.text if 'response' in locals() else "No response")
        return None

# Collector main loop
def run_collector_loop(interval=60):
    print("🔁 Entering collection loop...")
    while True:
        rows = []

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

            rows.append(row)
            time.sleep(0.05)  # Optional: adjust for API load

        # Batch insert after collecting all
        try:
            supabase.table("fx_rates").insert(rows).execute()
            print(f"✅ Inserted {len(rows)} FX rows in batch")
        except Exception as e:
            print(f"[ERROR] Failed batch insert: {e}")

        print("✅ Completed one full cycle of FX data collection.\n")
        time.sleep(interval)

# Run the loop if this is the main script
if __name__ == "__main__":
    run_collector_loop()
