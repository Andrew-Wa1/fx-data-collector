import time
import requests
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
import os

# Load env vars
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_DB_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_KEY = os.getenv("API_KEY")
API_URL = "https://api.fastforex.io/fetch-one"

# Create Supabase client
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_client()

# Currency pairs (56 total)
currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]
pairs = [(base, quote) for base in currencies for quote in currencies if base != quote]

# Fetch FX rate from FastForex
def fetch_rate(base, quote):
    try:
        response = requests.get(API_URL, params={
            "from": base,
            "to": quote,
            "api_key": API_KEY
        })
        response.raise_for_status()
        return response.json()["result"][quote]
    except Exception:
        return None

# Collector main loop
def run_collector_loop(interval=60):
    print("🚀 Collector running...")
    while True:
        loop_start = time.time()
        rows = []

        for base, quote in pairs:
            rate = fetch_rate(base, quote)
            if rate is None:
                continue

            row = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "base_currency": base,
                "quote_currency": quote,
                "rate": rate
            }
            rows.append(row)

        try:
            supabase.table("fx_rates").insert(rows).execute()
            print(f"✅ Inserted {len(rows)} rows at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"[ERROR] Supabase insert failed: {e}")

        loop_time = time.time() - loop_start
        print(f"⏱️ Loop time: {loop_time:.2f}s | Sleeping {max(0, interval - loop_time):.2f}s\n")

        time.sleep(max(0, interval - loop_time))

if __name__ == "__main__":
    run_collector_loop()
