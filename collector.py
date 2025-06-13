import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── CONFIG ────────────────────────────────────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("EXDBURL") or os.getenv("DATABASE_URL")
API_KEY      = os.getenv("API_KEY")
API_MULTI    = "https://api.fastforex.io/multi"
INTERVAL_S   = 60  # seconds

if not DATABASE_URL:
    raise RuntimeError("Missing EXDBURL / DATABASE_URL")

# ── PAIRS SETUP ────────────────────────────────────────────────────────────────
CURRENCIES = ["USD","EUR","GBP","JPY","CHF","AUD","CAD","NZD"]
PAIRS_MAP = {base: [q for q in CURRENCIES if q != base] for base in CURRENCIES}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def align_to_minute():
    """Sleep until the next exact minute boundary."""
    now = datetime.now(timezone.utc)
    delay = 60 - now.second - now.microsecond/1e6
    if delay > 0:
        time.sleep(delay)

def fetch_all_rates():
    """
    Hit the multi endpoint once per base currency,
    collect (timestamp, base, quote, rate) tuples.
    """
    ts = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
    rows = []
    for base, quotes in PAIRS_MAP.items():
        try:
            resp = requests.get(
                API_MULTI,
                params={"from": base, "to": ",".join(quotes), "api_key": API_KEY},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get("result", {})
        except Exception as e:
            print(f"[ERROR] Fetch {base}→{quotes}: {e}")
            continue

        for quote, rate in data.items():
            rows.append((ts, base, quote, rate))

    return rows

def insert_into_db(rows):
    """Batch insert into fx_rates, dedupe on (ts,base,quote)."""
    if not rows:
        return

    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    sql = """
        INSERT INTO fx_rates
          (timestamp, base_currency, quote_currency, rate)
        VALUES %s
        ON CONFLICT (timestamp, base_currency, quote_currency) DO NOTHING
    """
    execute_values(cur, sql, rows)
    conn.commit()
    conn.close()

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def run_collector(interval_s=INTERVAL_S):
    print(f"🚀 Starting collector: every {interval_s}s")
    # first align to the next minute boundary
    align_to_minute()

    while True:
        start = time.time()

        rows = fetch_all_rates()
        if rows:
            insert_into_db(rows)
            print(f"✅ Inserted {len(rows)} rows @ {rows[0][0]} UTC")
        else:
            print("⚠️ No data fetched this cycle")

        elapsed = time.time() - start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏱ Cycle took {elapsed:.2f}s; sleeping {to_sleep:.2f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⏱ Cycle took {elapsed:.2f}s; behind schedule, restarting immediately\n")

if __name__ == "__main__":
    run_collector()

