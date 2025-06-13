#!/usr/bin/env python3
import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── CONFIG ──────────────────────────────────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("EXDBURL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL or EXDBURL environment variable")

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.fastforex.io/fetch-one"

# 12 major currency pairs
PAIRS = [
    ("EUR","USD"),("GBP","USD"),("USD","JPY"),("USD","CHF"),
    ("AUD","USD"),("NZD","USD"),("USD","CAD"),("EUR","GBP"),
    ("EUR","JPY"),("GBP","JPY"),("AUD","JPY"),("CHF","JPY"),
]

# ── FETCH A SINGLE RATE ─────────────────────────────────────────────────────
def fetch_rate(base: str, quote: str) -> float | None:
    try:
        r = requests.get(
            API_URL,
            params={"from": base, "to": quote, "api_key": API_KEY},
            timeout=10
        )
        r.raise_for_status()
        return r.json()["result"].get(quote)
    except Exception as e:
        print(f"[WARN] fetch_rate {base}/{quote} failed: {e}")
        return None

# ── BULK INSERT INTO POSTGRES ────────────────────────────────────────────────
def insert_rows(rows: list[tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
    VALUES %s
    ON CONFLICT (timestamp, base_currency, quote_currency) DO NOTHING
    """
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    execute_values(cur, sql, rows, template="(%s,%s,%s,%s)")
    conn.commit()
    cur.close()
    conn.close()

# ── HELPERS ─────────────────────────────────────────────────────────────────
def align_to_minute():
    now = datetime.now(timezone.utc)
    # sleep until the top of the next minute
    to_sleep = 60 - now.second - now.microsecond/1e6
    if to_sleep > 0:
        print(f"⏱ Aligning to minute boundary (sleeping {to_sleep:.2f}s)")
        time.sleep(to_sleep)

# ── MAIN LOOP ────────────────────────────────────────────────────────────────
def run_collector(interval_s: float = 60.0):
    print(f"🚀 Collector will fetch every {interval_s:.0f}s")
    align_to_minute()
    while True:
        start = time.time()
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        rows = []
        for base, quote in PAIRS:
            rate = fetch_rate(base, quote)
            if rate is not None:
                rows.append((ts, base, quote, rate))

        insert_rows(rows)
        print(f"✅ Inserted {len(rows)} rows @ {ts.isoformat()} UTC")

        elapsed = time.time() - start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏲ Loop took {elapsed:.2f}s, sleeping {to_sleep:.2f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⚠️ Behind schedule by {(-to_sleep):.2f}s, continuing immediately\n")

if __name__ == "__main__":
    run_collector(60)
