import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv
from collections import defaultdict

# ─── Load env vars ────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = "https://api.fastforex.io/multi"

# ─── DB Connection Setup ─────────────────────────────────────────────────
# Try EXDBURL first (your Render external URL), fallback to DATABASE_URL
DATABASE_URL = os.getenv("EXDBURL") or os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing EXDBURL / DATABASE_URL environment variable")

# Connect over SSL (Render Postgres requires this)
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cursor = conn.cursor()

# ─── Define your 12 pairs ─────────────────────────────────────────────────
PAIRS = [
    ("EUR","USD"), ("USD","JPY"), ("GBP","USD"), ("AUD","USD"),
    ("USD","CAD"), ("USD","CHF"), ("NZD","USD"), ("EUR","GBP"),
    ("EUR","JPY"), ("GBP","JPY"), ("AUD","JPY"), ("USD","MXN"),
]

# Group by base for batch requests
grouped = defaultdict(list)
for base, quote in PAIRS:
    grouped[base].append(quote)

# ─── Collector Loop ──────────────────────────────────────────────────────
def run_collector_loop(interval=60, trim_threshold=70_000_000):
    print("🚀 Collector running against Postgres...")
    while True:
        start = time.time()
        rows = []

        # 1) Fetch batch rates
        for base, quotes in grouped.items():
            try:
                r = requests.get(API_URL, params={
                    "from": base,
                    "to": ",".join(quotes),
                    "api_key": API_KEY
                })
                r.raise_for_status()
                data = r.json().get("results", {})
                ts = datetime.now(timezone.utc)
                for q, rate in data.items():
                    rows.append((ts, base, q, rate))
            except Exception as e:
                print(f"[ERROR] Fetch {base}->{quotes}: {e}")

        # 2) Insert all at once
        if rows:
            sql = """
            INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
            VALUES %s
            """
            try:
                execute_values(cursor, sql, rows)
                conn.commit()
                print(f"✅ Inserted {len(rows)} rows @ {datetime.now(timezone.utc).isoformat()}")
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Insert failed: {e}")

        # 3) Trim oldest if exceeding row count threshold
        cursor.execute("SELECT COUNT(*) FROM fx_rates;")
        total_rows = cursor.fetchone()[0]
        if total_rows > trim_threshold:
            to_delete = total_rows - trim_threshold
            delete_sql = """
            DELETE FROM fx_rates
            WHERE ctid IN (
              SELECT ctid FROM fx_rates
              ORDER BY timestamp ASC
              LIMIT %s
            );
            """
            try:
                cursor.execute(delete_sql, (to_delete,))
                conn.commit()
                print(f"🗑️  Deleted {to_delete} old rows, now at {trim_threshold} rows")
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Trim failed: {e}")

        # 4) Sleep until next interval
        elapsed = time.time() - start
        sleep_for = max(0, interval - elapsed)
        print(f"⏱️ Loop time: {elapsed:.2f}s | Sleeping {sleep_for:.2f}s\n")
        time.sleep(sleep_for)

if __name__ == "__main__":
    run_collector_loop()



