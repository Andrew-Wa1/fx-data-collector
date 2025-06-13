#!/usr/bin/env python3
import os
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from dotenv import load_dotenv

# ── Load config ──────────────────────────────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("EXDBURL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL / EXDBURL environment variable")
API_KEY      = os.getenv("API_KEY")
API_URL      = "https://api.fastforex.io/fetch-one"

# ── Postgres setup ────────────────────────────────────────────────────────────
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
conn.autocommit = True
cur = conn.cursor()

# ── The 12 pairs you care about ────────────────────────────────────────────────
pairs = [
    ("EUR", "USD"), ("GBP", "USD"), ("USD", "JPY"), ("USD", "CHF"),
    ("AUD", "USD"), ("NZD", "USD"), ("USD", "CAD"), ("EUR", "GBP"),
    ("EUR", "JPY"), ("GBP", "JPY"), ("AUD", "JPY"), ("CHF", "JPY"),
]

def fetch_rate(base: str, quote: str) -> float | None:
    """Fetch a single FX rate."""
    try:
        r = requests.get(
            API_URL,
            params={"from": base, "to": quote, "api_key": API_KEY},
            timeout=10
        )
        r.raise_for_status()
        return r.json()["result"][quote]
    except Exception as e:
        print(f"[ERROR] fetch_rate {base}/{quote} → {e}")
        return None

def run_collector_loop(interval_s: float = 60):
    # 1) align to the next exact minute
    now = datetime.now(timezone.utc)
    # seconds + fractional → how long until second==0
    wait = interval_s - (now.second + now.microsecond/1e6)
    if wait > 0:
        print(f"⏳ Aligning to minute boundary: sleeping {wait:.2f}s")
        time.sleep(wait)
    print("🚀 Starting collector on minute marks...")

    while True:
        cycle_start = time.time()
        # set ts to the exact minute
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        # 2) fetch all rates in parallel
        rows = []
        with ThreadPoolExecutor(max_workers=len(pairs)) as exe:
            futures = {
                exe.submit(fetch_rate, base, quote): (base, quote)
                for base, quote in pairs
            }
            for fut in as_completed(futures):
                base, quote = futures[fut]
                rate = fut.result()
                if rate is not None:
                    rows.append((ts, base, quote, rate))

        # 3) bulk-insert with ON CONFLICT DO NOTHING
        if rows:
            args_str = ",".join(
                cur.mogrify("(%s,%s,%s,%s)", row).decode()
                for row in rows
            )
            sql = f"""
                INSERT INTO fx_rates(timestamp, base_currency, quote_currency, rate)
                VALUES {args_str}
                ON CONFLICT (timestamp, base_currency, quote_currency) DO NOTHING;
            """
            try:
                cur.execute(sql)
                print(f"✅ Inserted {len(rows)} rows for {ts.isoformat()}")
            except Exception as e:
                print(f"[ERROR] insert failed → {e}")

        # 4) sleep until the next minute tick
        elapsed = time.time() - cycle_start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏱ Cycle took {elapsed:.1f}s; sleeping {to_sleep:.1f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⏱ Cycle took {elapsed:.1f}s (behind), restarting immediately\n")

if __name__ == "__main__":
    run_collector_loop(interval_s=60)



