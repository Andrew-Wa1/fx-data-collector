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
# Correct batch endpoint:
API_URL = "https://api.fastforex.io/fetch-multi"

# ─── DB Connection Setup ─────────────────────────────────────────────────
DATABASE_URL = os.getenv("EXDBURL") or os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing EXDBURL / DATABASE_URL environment variable")
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cursor = conn.cursor()

# ─── Ensure table exists ──────────────────────────────────────────────────
cursor.execute("""
CREATE TABLE IF NOT EXISTS fx_rates (
  timestamp TIMESTAMPTZ NOT NULL,
  base_currency TEXT NOT NULL,
  quote_currency TEXT NOT NULL,
  rate DOUBLE PRECISION NOT NULL
);
""")
conn.commit()

# ─── Define your 12 pairs ─────────────────────────────────────────────────
PAIRS = [
    ("EUR","USD"), ("USD","JPY"), ("GBP","USD"), ("AUD","USD"),
    ("USD","CAD"), ("USD","CHF"), ("NZD","USD"), ("EUR","GBP"),
    ("EUR","JPY"), ("GBP","JPY"), ("AUD","JPY"), ("USD","MXN"),
]
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
                resp = requests.get(API_URL, params={
                    "from": base,
                    "to": ",".join(quotes),
                    "api_key": API_KEY
                })
                resp.raise_for_status()
                data = resp.json().get("results", {})
                ts = datetime.now(timezone.utc)
                for q, rate in data.items():
                    rows.append((ts, base, q, rate))
            except Exception as e:
                print(f"[ERROR] Fetch {base}->{quotes}: {e}")

        # 2) Bulk insert
        if rows:
            insert_sql = """
              INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
              VALUES %s
            """
            try:
                execute_values(cursor, insert_sql, rows)
                conn.commit()
                print(f"✅ Inserted {len(rows)} rows @ {datetime.now(timezone.utc).isoformat()}")
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Insert failed: {e}")

        # 3) Trim oldest if exceeding row count
        cursor.execute("SELECT COUNT(*) FROM fx_rates;")
        total = cursor.fetchone()[0]
        if total > trim_threshold:
            to_delete = total - trim_threshold
            cursor.execute("""
              DELETE FROM fx_rates
              WHERE ctid IN (
                SELECT ctid FROM fx_rates
                ORDER BY timestamp ASC
                LIMIT %s
              );
            """, (to_delete,))
            conn.commit()
            print(f"🗑️ Deleted {to_delete} rows; kept {trim_threshold}")

        # 4) Sleep
        elapsed = time.time() - start
        sleep_for = max(0, interval - elapsed)
        print(f"⏱️ Loop: {elapsed:.2f}s | Sleeping {sleep_for:.2f}s\n")
        time.sleep(sleep_for)

if __name__ == "__main__":
    run_collector_loop()



