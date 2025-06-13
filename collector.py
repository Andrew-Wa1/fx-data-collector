import logging
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from dotenv import load_dotenv
import os

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# ── Config ────────────────────────────────────────────────────────────────────
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
# Group quotes by base for fetch-multi
grouped = {}
for b, q in pairs:
    grouped.setdefault(b, []).append(q)

def fetch_rate_multi(base, quotes):
    """ Fetch multiple quotes in one call """
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
        return r.json().get("result", {})
    except Exception as e:
        logger.error(f"fetch-multi {base}→{quotes} failed: {e}")
        return {}

def run_collector_loop(interval_s: float = 60):
    # ① Align to next exact minute
    now = datetime.now(timezone.utc)
    wait = interval_s - (now.second + now.microsecond/1e6)
    if wait > 0:
        logger.info(f"Aligning to minute boundary: sleeping {wait:.2f}s")
        time.sleep(wait)
    logger.info("🚀 Collector running at exact minute marks…")

    while True:
        cycle_start = time.time()
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        try:
            # ② Fetch all rates base→quotes
            rows = []
            for base, quotes in grouped.items():
                result = fetch_rate_multi(base, quotes)
                for quote, rate in result.items():
                    if (base, quote) in pairs and rate is not None:
                        rows.append((ts, base, quote, rate))

            # ③ Bulk INSERT
            if rows:
                args = ",".join(
                    cur.mogrify("(%s,%s,%s,%s)", row).decode()
                    for row in rows
                )
                sql = f"""
                INSERT INTO fx_rates(timestamp, base_currency, quote_currency, rate)
                VALUES {args}
                ON CONFLICT (timestamp, base_currency, quote_currency) DO NOTHING;
                """
                cur.execute(sql)
                logger.info(f"Inserted {len(rows)} rows for {ts:%Y-%m-%d %H:%M:%S} UTC")

        except Exception as exc:
            # Catch *anything* so the loop never dies
            logger.exception("Unhandled exception in collector loop:")

        # ④ Sleep until the next minute tick
        elapsed = time.time() - cycle_start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            logger.info(f"Cycle took {elapsed:.2f}s; sleeping {to_sleep:.2f}s")
            time.sleep(to_sleep)
        else:
            logger.warning(f"Cycle took {elapsed:.2f}s (behind schedule); restarting immediately")

if __name__ == "__main__":
    run_collector_loop(interval_s=60)




