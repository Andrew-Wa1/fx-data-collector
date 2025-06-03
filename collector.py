import time
import requests
import psycopg2
from datetime import datetime

print("🚀 Collector script starting up...", flush=True)

# === HARDCODED CREDENTIALS ===
DB_URL = "postgresql://postgres:j0WZQwxI1OdPzs98@db.efddyekfalqaohssgkmi.supabase.co:5432/postgres"
API_KEY = "4764d9864f-f6aeb54c65-sx6y19"

# === DEBUG LOGGING ===
print(f"✅ Loaded DB_URL: {'yes' if DB_URL else 'NO!'}", flush=True)
print(f"✅ Loaded API_KEY: {'yes' if API_KEY else 'NO!'}", flush=True)

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
        print(f"⚠️ Unexpected data for {base}/{quote}: {data}", flush=True)
        return None
    except Exception as e:
        print(f"❌ Error fetching {base}/{quote}: {e}", flush=True)
        return None

def save_batch_to_db(conn, records):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
                VALUES (%s, %s, %s, %s);
            """, records)
        conn.commit()
        print(f"✅ Saved batch of {len(records)} rates", flush=True)
    except Exception as e:
        print(f"❌ Batch DB error: {e}", flush=True)

# === MAIN LOOP ===
while True:
    print(f"\n🕒 Collecting at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    try:
        conn = connect_db()
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}", flush=True)
        time.sleep(60)
        continue

    records = []
    for base, quote in PAIRS:
        rate = fetch_rate(base, quote)
        if rate:
            records.append((datetime.utcnow(), base, quote, rate))
        time.sleep(0.3)  # Gentle pacing to avoid API overload

    if records:
        save_batch_to_db(conn, records)

    conn.close()
    time.sleep(10)  # <- Adjusted for upgraded plan (up to 10/minute)
