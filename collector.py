import os
import time
import requests
import psycopg2
from datetime import datetime

print("🚀 Collector script starting up...")

# === LOAD ENV VARS DIRECTLY ===
DB_URL = os.environ.get("SUPABASE_DB_URL")
API_KEY = os.environ.get("API_KEY")

# === DEBUG LOGGING ===
print(f"✅ Loaded SUPABASE_DB_URL: {'yes' if DB_URL else 'NO!'}")
print(f"✅ Loaded API_KEY: {'yes' if API_KEY else 'NO!'}")

# === FX PAIRS ===
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD"]
PAIRS = [(base, quote) for base in CURRENCIES for quote in CURRENCIES if base != quote]

def connect_db():
    return psycopg2.connect(DB_URL)

def fetch_rate(base, quote):
    try:
        url = "https://api.fastforex.io/fetch-one"
        params = {"from": base, "to": quote, "api_key": API_KEY}
        response = requests.get(url, params=params)
        data = response.json()
        if "result" in data and quote in data["result"]:
            return float(data["result"][quote])
        print(f"⚠️ Unexpected data for {base}/{quote}: {data}")
        return None
    except Exception as e:
        print(f"❌ Error fetching {base}/{quote}: {e}")
        return None

def save_to_db(conn, base, quote, rate):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
                VALUES (%s, %s, %s, %s);
            """, (datetime.utcnow(), base, quote, rate))
        conn.commit()
        print(f"✅ Saved {base}/{quote}: {rate}")
    except Exception as e:
        print(f"❌ DB error for {base}/{quote}: {e}")

# === MAIN LOOP ===
while True:
    print(f"\n🕒 Collecting at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        conn = connect_db()
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}")
        time.sleep(60)
        continue

    for base, quote in PAIRS:
        rate = fetch_rate(base, quote)
        if rate:
            save_to_db(conn, base, quote, rate)
        time.sleep(0.3)

    conn.close()
    time.sleep(60)
# Force redeploy
