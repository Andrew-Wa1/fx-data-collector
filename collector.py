#!/usr/bin/env python3
import os
import time
import logging
import requests

from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv

# ── CONFIG ────────────────────────────────────────────────────────────────────
load_dotenv()

DB_URL  = os.getenv("EXDBURL") or os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY")
API_URL = "https://api.fastforex.io/fetch-multi"    # fastforex multi-endpoint

if not DB_URL or not API_KEY:
    raise RuntimeError("Missing EXDBURL (or DATABASE_URL) and API_KEY in env")

# Exactly the twelve G10 pairs you track:
PAIRS = [
    ("EUR","USD"), ("GBP","USD"), ("USD","JPY"), ("USD","CHF"),
    ("AUD","USD"), ("NZD","USD"), ("USD","CAD"), ("EUR","GBP"),
    ("EUR","JPY"), ("GBP","JPY"), ("AUD","JPY"), ("CHF","JPY"),
]

# Group quotes by base for a single “multi” call per base:
GROUPED = {}
for b,q in PAIRS:
    GROUPED.setdefault(b, []).append(q)


# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()


# ── DB SETUP ──────────────────────────────────────────────────────────────────
conn = psycopg2.connect(DB_URL, sslmode="require")
conn.autocommit = True
cur = conn.cursor()


# ── FETCH UTIL ─────────────────────────────────────────────────────────────────
def fetch_multi(base, quotes):
    """Fetch multiple quotes in one API call."""
    try:
        resp = requests.get(
            API_URL,
            params={
                "from":    base,
                "to":      ",".join(quotes),
                "api_key": API_KEY
            },
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("result", {})
    except Exception as e:
        logger.error(f"API multi fetch failed for {base}->{quotes}: {e}")
        return {}


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def run_collector_loop(interval_s: float = 60.0):
    # 1) Align to next exact minute boundary:
    now   = datetime.now(timezone.utc)
    wait  = interval_s - (now.second + now.microsecond/1e6)
    if wait > 1e-3:
        logger.info(f"Aligning to minute mark: sleeping {wait:.2f}s")
        time.sleep(wait)

    logger.info("🚀 Collector started at exact minute marks.")

    while True:
        cycle_start = time.time()
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        try:
            # 2) Gather rows for all 12 pairs:
            rows = []
            for base, quotes in GROUPED.items():
                result = fetch_multi(base, quotes)
                for quote, rate in result.items():
                    if (base, quote) in PAIRS and rate is not None:
                        rows.append((ts, base, quote, rate))

            # 3) Bulk-insert:
            if rows:
                # build VALUES list safely via mogrify
                vals = ",".join(
                    cur.mogrify("(%s,%s,%s,%s)", row).decode()
                    for row in rows
                )
                sql = f"""
                    INSERT INTO fx_rates
                      (timestamp, base_currency, quote_currency, rate)
                    VALUES {vals}
                    ON CONFLICT (timestamp, base_currency, quote_currency) DO NOTHING;
                """
                cur.execute(sql)
                logger.info(f"Inserted {len(rows)} rows for {ts.isoformat()} UTC")
            else:
                logger.warning(f"No data fetched at {ts.isoformat()} UTC")

        except Exception:
            logger.exception("Unexpected error in collector loop — continuing:")

        # 4) Sleep until the next tick:
        elapsed = time.time() - cycle_start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            logger.info(f"Cycle took {elapsed:.2f}s; sleeping {to_sleep:.2f}s\n")
            time.sleep(to_sleep)
        else:
            logger.warning(f"Cycle took {elapsed:.2f}s (> {interval_s:.0f}s); restarting immediately\n")


if __name__ == "__main__":
    run_collector_loop(interval_s=60.0)




