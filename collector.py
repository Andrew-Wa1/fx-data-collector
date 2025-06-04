import time
import requests
from datetime import datetime, timezone
from supabase_client import get_client
from dotenv import load_dotenv
import os

load_dotenv()
supabase = get_client()

API_KEY = os.getenv("API_KEY")  # FastForex key
API_URL = "https://api.fastforex.io/fetch-one"

# List of currencies to cover (8x7 = 56 pairs)
currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]
pairs = [(base, quote) for base in currencies for quote in currencies if base != quote]

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

def run_collector_loop(interval=60):
    print("🔁 Starting FX data collector loop...")
    while True:
        for base, quote in pairs:
            rate = fetch_rate(base, quote)
            if rate is None:
                continue  # Skip failed fetches

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

            # Optional sleep to reduce strain (optional: 0.1–0.5s)
            time.sleep(0.1)

        print("⏳ Sleeping before next cycle...\n")
        time.sleep(interval)

if __name__ == "__main__":
    run_collector_loop()
