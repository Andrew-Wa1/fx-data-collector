#!/usr/bin/env python3
import time
import requests
from datetime import datetime, timezone
import psycopg2
import os
from dotenv import load_dotenv

# ── Load config ──────────────────────────────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("EXDBURL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL / EXDBURL environment variable")
API_KEY      = os.getenv("API_KEY")
API_URL      = "https://api.fastforex.io/fetch-one"

# ── Set up Postgres connection ────────────────────────────────────────────────
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

# ── The 12 pairs you actually need ────────────────────────────────────────────
pairs = [
    ("EUR", "USD"), ("GBP", "USD"), ("USD", "JPY"), ("USD", "CHF"),
    ("AUD", "USD"), ("NZD", "USD"), ("USD", "CAD"), ("EUR", "GBP"),
    ("EUR", "JPY"), ("GBP", "JPY"), ("AUD", "JPY"), ("CHF", "JPY"),
]

def fetch_rate(base: str, quote: str) -> float | None:
    """Fetches a single FX rate from FastForex."""
    try:
        r = requests.get(
            API_URL,
            params={"from": base, "to": quote, "api_key": API_KEY},
            timeout=10
        )
        r.raise_for_status()
        return r.json()["result"][quote]
    except Exception as e:
        print(f"[ERROR] Fetch {base}/{quote} failed: {e}")
        return None

def run_collector_loop(interval_s: float = 60):
    """Main loop: fetches rates for each pair and inserts into fx_rates every minute."""
    print(f"🚀 Collector starting—polling every {interval_s:.0f}s")
    while True:
        loop_start = time.time()
        # use the exact minute (no seconds/microseconds) as timestamp
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        rows = []
        for base, quote in pairs:
            rate = fetch_rate(base, quote)
            if rate is not None:
                rows.append((ts, base, quote, rate))

        if rows:
            # build a bulk-insert with ON CONFLICT DO NOTHING
            args_str = ",".join(
                cur.mogrify("(%s,%s,%s,%s)", row).decode("utf8")
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
                print(f"[ERROR] Insert failed: {e}")

        # sleep so that each cycle is ~interval_s seconds
        elapsed = time.time() - loop_start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏱ Loop took {elapsed:.1f}s; sleeping {to_sleep:.1f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⏱ Loop took {elapsed:.1f}s (behind schedule); restarting immediately\n")

if __name__ == "__main__":
    run_collector_loop(interval_s=60)




