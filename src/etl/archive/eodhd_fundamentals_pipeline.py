
import os
import time
import json
import requests
import pandas as pd
from dotenv import load_dotenv

# Load Environment
load_dotenv()
API_KEY = os.getenv("EODHD_API_KEY")

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
UNIVERSE_FILE = os.path.join(BASE_DIR, "bronze", "universe", "eodhd_master_universe.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "bronze", "fundamentals", "eodhd", "json")

# Safety Configuration
MAX_CREDITS_PER_RUN = 60000    # Increased to cover ~6000 tickers (Silver layer targets)
COST_PER_CALL = 10             # Fundamentals cost 10 credits
BATCH_SIZE = 5                 # Process 5 at a time
SLEEP_BETWEEN_BATCHES = 0.5    # Reduce sleep slightly for faster processing

def setup_directories():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

def load_universe():
    # Priority 1: Targeted list from Silver layer
    target_file = os.path.join(BASE_DIR, "silver_tickers.txt")
    if os.path.exists(target_file):
        print(f"Loading targeted tickers from {target_file}...")
        with open(target_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(tickers)} targeted tickers.")
        return list(sorted(set(tickers)))

    # Priority 2: Full Universe (fallback)
    if not os.path.exists(UNIVERSE_FILE):
        print(f"Error: Universe file not found at {UNIVERSE_FILE}")
        return []
    
    df = pd.read_csv(UNIVERSE_FILE)
    if 'Code' not in df.columns:
        print("Error: Universe file missing 'Code' column")
        return []
        
    tickers = []
    # Match logic from eodhd_pipeline.py: simple Code + .US suffix
    # The previous pipeline proved this works for this universe file.
    # FILTER: Exclude Mutual Funds as requested
    skipped_funds = 0
    
    for _, row in df.iterrows():
        type_ = str(row.get('Type', '')).strip()
        
        # Exclude Non-Corporate Equities
        if type_ in ['FUND', 'Mutual Fund', 'ETF', 'BOND', 'Notes', 'Preferred Stock', 'ETC']:
            skipped_funds += 1
            continue
            
        code = str(row['Code']).strip().upper()
        if code and code != 'NAN':
            tickers.append(f"{code}.US")
            
    print(f"Loaded {len(tickers)} tickers (Skipped {skipped_funds} Mutual Funds).")

    return list(sorted(set(tickers)))

def setup_session():
    """Create a requests session with retry logic and connection pooling."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10, 
        pool_maxsize=10,
        max_retries=3
    )
    session.mount('https://', adapter)
    return session

def get_credits_used(response):
    # EODHD returns X-RateLimit-Remaining / X-RateLimit-Limit headers
    # We can infer usage, but safer to assume fixed cost per call
    return COST_PER_CALL

def fetch_fundamentals_safe(ticker, session):
    url = f"https://eodhd.com/api/fundamentals/{ticker}"
    params = {"api_token": API_KEY, "fmt": "json"}
    
    try:
        # Use session for connection pooling (reuse TCP/SSL)
        r = session.get(url, params=params, timeout=15)
        
        # Check explicit API credit header if available to monitor health
        # remaining = r.headers.get('X-RateLimit-Remaining')
        
        if r.status_code == 200:
            data = r.json()
            # Simple validation: keys must exist
            if not data or 'General' not in data:
                 return False, "EMPTY_DATA"
                 
            safe_ticker = ticker.replace("/", "_")
            filepath = os.path.join(OUTPUT_DIR, f"{safe_ticker}.json")
            with open(filepath, "w") as f:
                json.dump(data, f)
            return True, "SUCCESS"
            
        elif r.status_code == 429:
            return False, "RATE_LIMIT"
        elif r.status_code == 404:
            return False, "NOT_FOUND"
        else:
            return False, f"HTTP_{r.status_code}"
            
    except Exception as e:
        return False, f"ERROR_{str(e)}"

def main():
    setup_directories()
    if not API_KEY:
        print("Error: EODHD_API_KEY missing.")
        return

    all_tickers = load_universe()
    print(f"Total Tickers in Universe: {len(all_tickers)}")
    
    # Filter out already downloaded files
    existing_files = set(f.replace(".json", "") for f in os.listdir(OUTPUT_DIR) if f.endswith(".json"))
    tickers_to_process = [t for t in all_tickers if t.replace("/", "_") not in existing_files]
    print(f"Tickers remaining to download: {len(tickers_to_process)}")
    
    credits_spent = 0
    processed_count = 0
    
    print(f"Safety Cap: {MAX_CREDITS_PER_RUN} credits (~{int(MAX_CREDITS_PER_RUN/COST_PER_CALL)} tickers)")
    print("Starting Safe Download with Persistent Session...")

    # Initialize Session
    session = setup_session()
    
    # Process in chunks
    try:
        for i in range(0, len(tickers_to_process), BATCH_SIZE):
            batch = tickers_to_process[i : i + BATCH_SIZE]
            
            for ticker in batch:
                if credits_spent >= MAX_CREDITS_PER_RUN:
                    print(f"\n[STOP] Safety limit reached: {credits_spent} credits used.")
                    return
    
                print(f"Fetching {ticker}...", end=" ")
                success, status = fetch_fundamentals_safe(ticker, session)
                
                credits_spent += COST_PER_CALL
                processed_count += 1
                
                print(f"[{status}] (Est. Credits: {credits_spent})")
    
                if status == "RATE_LIMIT":
                    print("!! RATE LIMIT HIT !! SLEEPING 60s...")
                    time.sleep(60)
                
                # Throttling to prevent I/O saturation (User requested safety)
                time.sleep(1.5)
                
            # Batch delay
            time.sleep(5.0)
            
    finally:
        session.close()

    print("\nRun Complete.")
    print(f"Total Tickers Processed: {processed_count}")
    print(f"Estimated Credits Used: {credits_spent}")

if __name__ == "__main__":
    main()
