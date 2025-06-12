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
API_URL = "https://api.fastforex.io/multi"

# Create Supabase client
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_client()

# Only 12 pairs you care about
PAIRS = [
    ("EUR", "USD"), ("USD", "JPY"), ("GBP", "USD"),
    ("AUD", "USD"), ("USD", "CAD"), ("USD", "CHF"),
    ("NZD", "USD"), ("EUR", "GBP"), ("EUR", "JPY"),
    ("GBP", "JPY"), ("AUD", "JPY"), ("USD", "MXN")
]

# Group by base for batch requests
from collections import defaultdict
grouped = defaultdict(list)
for base, quote in PAIRS:
    grouped[base].append(quote)

# Collector main loop
def run_collector_loop(interval=60):
    print("🚀 Collector running...")
    while True:
        loop_start = time.time()
        rows = []

        for base, quotes in grouped.items():
            try:
                response = requests.get(API_URL, params={
                    "from": base,
                    "to": ",".join(quotes),
                    "api_key": API_KEY
                })
                response.raise_for_status()
                data = response.json().get("results", {})
                for quote, rate in data.items():
                    rows.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "base_currency": base,
                        "quote_currency": quote,
                        "rate": rate
                    })
            except Exception as e:
                print(f"[ERROR] API fetch failed for {base}->{quotes}: {e}")

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

