import time  
import requests  
from datetime import datetime, timezone  
from supabase import create_client  
from dotenv import load_dotenv  
import os  

# ── Load config ──────────────────────────────────────────────────────────────  
load_dotenv()  
SUPABASE_URL = os.getenv("SUPABASE_DB_URL")  
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  
API_KEY      = os.getenv("API_KEY")  
API_URL      = "https://api.fastforex.io/fetch-one"  

def get_supabase():  
    return create_client(SUPABASE_URL, SUPABASE_KEY)  

supabase = get_supabase()  

# ── Which pairs to fetch ───────────────────────────────────────────────────────  
currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]  
pairs = [(b, q) for b in currencies for q in currencies if b != q]  

# ── Fetch one FX rate ─────────────────────────────────────────────────────────  
def fetch_rate(base, quote):  
    try:  
        r = requests.get(API_URL,  
                         params={"from": base, "to": quote, "api_key": API_KEY},  
                         timeout=10)  
        r.raise_for_status()  
        return r.json()["result"][quote]  
    except Exception as e:  
        print(f"[ERROR] Fetch failed for {base}/{quote}: {e}")  
        return None  

# ── Main loop ─────────────────────────────────────────────────────────────────  
def run_collector_loop(interval_s: float = 60):  
    print(f"🚀 Collector running every {interval_s:.0f}s")  
    while True:  
        start = time.time()  
        rows = []  
        for base, quote in pairs:  
            rate = fetch_rate(base, quote)  
            if rate is None:  
                continue  
            rows.append({  
                "timestamp": datetime.now(timezone.utc).isoformat(),  
                "base_currency": base,  
                "quote_currency": quote,  
                "rate": rate  
            })  
  
        if rows:  
            try:  
                supabase.table("fx_rates").insert(rows).execute()  
                print(f"✅ Inserted {len(rows)} rows @ {datetime.now(timezone.utc).isoformat()}")  
            except Exception as e:  
                print(f"[ERROR] Insert failed: {e}")  
  
        # how long did we actually take?  
        elapsed = time.time() - start  
        # compute exactly how long to sleep so that  
        # start_of_next_loop = start_of_this_loop + interval_s  
        to_sleep = interval_s - elapsed  
        if to_sleep > 0:  
            print(f"⏱ Loop took {elapsed:.1f}s; sleeping {to_sleep:.1f}s\n")  
            time.sleep(to_sleep)  
        else:  
            # if inserts ran long, immediately start next  
            print(f"⏱ Loop took {elapsed:.1f}s; behind schedule, restarting immediately\n")  
  
if __name__ == "__main__":  
    # enforce always 60s  
    run_collector_loop(interval_s=60)  




