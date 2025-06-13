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
API_KEY      = os.getenv("API_KEY")
if not DATABASE_URL or not API_KEY:
    raise RuntimeError("Need DATABASE_URL (or EXDBURL) and API_KEY in env")

API_MULTI_URL = "https://api.fastforex.io/fetch-multi"

# ── Postgres setup ────────────────────────────────────────────────────────────
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
conn.autocommit = True
cur = conn.cursor()

# ── The exact 12 pairs we want ────────────────────────────────────────────────
pairs = [
    ("EUR", "USD"), ("GBP", "USD"), ("USD", "JPY"), ("USD", "CHF"),
    ("AUD", "USD"), ("NZD", "USD"), ("USD", "CAD"), ("EUR", "GBP"),
    ("EUR", "JPY"), ("GBP", "JPY"), ("AUD", "JPY"), ("CHF", "JPY"),
]
# Build a map: base → list of quotes
grouped = {}
for b, q in pairs:
    grouped.setdefault(b, []).append(q)

def run_collector_loop(interval_s: float = 60):
    # ① Align to next exact minute boundary
    now = datetime.now(timezone.utc)
    wait = interval_s - (now.second + now.microsecond/1e6)
    if wait > 0:
        print(f"⏳ Aligning to minute boundary: sleeping {wait:.2f}s")
        time.sleep(wait)
    print("🚀 Collector running at exact minute marks now…")

    while True:
        cycle_start = time.time()
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        rows = []
        # ② Fetch per-base in one call each
        for base, quotes in grouped.items():
            try:
                r = requests.get(
                    API_MULTI_URL,
                    params={
                        "from": base,
                        "to":    ",".join(quotes),
                        "api_key": API_KEY
                    },
                    timeout=10
                )
                r.raise_for_status()
                data = r.json().get("result", {})
            except Exception as e:
                print(f"[ERROR] Multi-fetch {base}→{quotes} failed: {e}")
                continue

            # ③ Collect only our 12 pairs
            for quote, rate in data.items():
                if (base, quote) in pairs and rate is not None:
                    rows.append((ts, base, quote, rate))

        # ④ Bulk INSERT with ON CONFLICT DO NOTHING
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
                print(f"✅ Inserted {len(rows)} rows for {ts:%Y-%m-%d %H:%M:%S} UTC")
            except Exception as e:
                print(f"[ERROR] INSERT failed: {e}")

        # ⑤ Sleep exactly until the next minute tick
        elapsed = time.time() - cycle_start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏱ Cycle took {elapsed:.2f}s; sleeping {to_sleep:.2f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⏱ Cycle took {elapsed:.2f}s (behind); restarting immediately\n")

if __name__ == "__main__":
    run_collector_loop(interval_s=60)



