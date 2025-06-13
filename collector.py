# ── Main loop ─────────────────────────────────────────────
def run_collector_loop(interval_s: float = 60.0):
    # Align to next exact minute boundary
    now = datetime.now(timezone.utc)
    next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1))
    wait = (next_minute - now).total_seconds()
    print(f"⏳ Aligning to minute boundary, sleeping {wait:.1f}s")
    time.sleep(wait)

    print(f"🚀 Collector running every {interval_s:.0f}s")
    while True:
        start = time.time()
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        rates = fetch_all_rates()
        rows = [
            (ts, b, q, rate)
            for (b, q), rate in rates.items()
        ]

        if rows:
            try:
                conn = get_conn()
                cur = conn.cursor()
                sql = """
                    INSERT INTO fx_rates (timestamp, base_currency, quote_currency, rate)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """
                execute_values(cur, sql, rows)
                cur.close()
                print(f"✅ Inserted {len(rows)} rows @ {ts.isoformat()}")
            except Exception as e:
                print(f"[ERROR] DB insert failed: {e}")
        else:
            print(f"⚠️  No data fetched at {ts.isoformat()}")

        # sleep exactly so that next loop starts ~on the next minute
        elapsed = time.time() - start
        to_sleep = interval_s - elapsed
        if to_sleep > 0:
            print(f"⏱ Loop took {elapsed:.1f}s; sleeping {to_sleep:.1f}s\n")
            time.sleep(to_sleep)
        else:
            print(f"⏱ Loop took {elapsed:.1f}s; behind schedule, restarting immediately\n")

if __name__ == "__main__":
    run_collector_loop(interval_s=60.0)


