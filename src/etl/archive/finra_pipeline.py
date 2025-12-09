
import os
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
import datetime
from dotenv import load_dotenv

load_dotenv()

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "bronze", "finra")
MARGIN_DIR = os.path.join(DATA_DIR, "margin")
SHORT_INTEREST_DIR = os.path.join(DATA_DIR, "short_interest")
REG_SHO_DIR = os.path.join(DATA_DIR, "reg_sho")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
}

# FINRA API Configuration
FINRA_CLIENT_ID = os.getenv("FINRA_CLIENT_ID")
FINRA_CLIENT_PASSWORD = os.getenv("FINRA_CLIENT_PASSWORD")
FINRA_TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"

def get_finra_token():
    """
    Generates OAuth2 Access Token for FINRA API.
    Requires FINRA_CLIENT_ID and FINRA_CLIENT_PASSWORD in .env.
    """
    if not FINRA_CLIENT_ID:
        print("Error: Missing FINRA Client ID in .env")
        return None
        
    try:
        # Standard FINRA OAuth flow (Grant Type: Client Credentials)
        # Some setups might use ID as both user/pass or allow empty pass
        pwd = FINRA_CLIENT_PASSWORD if FINRA_CLIENT_PASSWORD else ""
        auth = (FINRA_CLIENT_ID, pwd)
        data = {"grant_type": "client_credentials"}
        
        r = requests.post(FINRA_TOKEN_URL, auth=auth, data=data)
        r.raise_for_status()
        
        token = r.json().get("access_token")
        return token
    except requests.exceptions.HTTPError as e:
        print(f"Authentication Failed: {e}")
        print(f"Server Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Authentication Failed: {e}")
        return None

def setup_directories():
    for d in [DATA_DIR, MARGIN_DIR, SHORT_INTEREST_DIR, REG_SHO_DIR]:
        if not os.path.exists(d):
            os.makedirs(d)
            print(f"Created directory: {d}")

def download_margin_statistics():
    """
    Downloads FINRA Margin Statistics.
    Source: https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics
    Note: The data is usually embedded in a table or a downloadable CSV link.
    """
    print("\n[Margin Statistics] Fetching...")
    url = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Look for the table
        # FINRA usually puts this in a standard HTML table
        tables = pd.read_html(str(soup))
        
        if not tables:
            print("No tables found for Margin Statistics.")
            return

        # Assuming the first table is the main one (Debit Balances, etc.)
        df = tables[0]
        
        # Save raw
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        filepath = os.path.join(MARGIN_DIR, f"margin_statistics_{timestamp}.csv")
        df.to_csv(filepath, index=False)
        print(f"Saved Margin Statistics to {filepath}")
        print(f"Rows: {len(df)}")
        
    except Exception as e:
        print(f"Error fetching Margin Statistics: {e}")

def download_short_interest_dates():
    """
    Downloads the Short Interest Reporting Dates.
    Useful as metadata.
    """
    print("\n[Short Interest Dates] Fetching Schedule...")
    url = "https://www.finra.org/filing-reporting/short-interest/schedule"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        # Wrap in StringIO if needed, or pass string direct depending on pandas version
        # Pandas 2.x prefers StringIO
        from io import StringIO
        tables = pd.read_html(StringIO(response.text))
        
        if tables:
            df = tables[0]
            filepath = os.path.join(SHORT_INTEREST_DIR, "short_interest_schedule.csv")
            df.to_csv(filepath, index=False)
            print(f"Saved Short Interest Schedule to {filepath}")
    except Exception as e:
         print(f"Error fetching Short Interest Schedule: {e}")

def download_equity_short_interest(token):
    """
    Downloads Equity Short Interest data using the authenticated token.
    """
    print("\n[Equity Short Interest] Fetching Data...")
    url = "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Fetching a larger batch. API pagination might be needed for full history.
    payload = {
        "limit": 5000  # Adjust as needed or loop for pagination
    }
    
    try:
        r = requests.post(url, headers=headers, json=payload)
        r.raise_for_status()
        
        # Determine content type (JSON vs CSV) based on response or request
        # The probe showed CSV-like headers in text, but let's check content-type header if possible
        # We'll save raw response for now to ensure we get data.
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        output_dir = SHORT_INTEREST_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as CSV (Response is typically CSV-formatted text)
        output_file = os.path.join(output_dir, f"equity_short_interest_{timestamp}.csv")
        with open(output_file, "w") as f:
            f.write(r.text)
            
        line_count = len(r.text.splitlines())
        print(f"Saved {line_count} lines to {output_file}")
            
    except Exception as e:
        print(f"Error downloading Equity Short Interest: {e}")

def download_reg_sho_daily(token):
    """
    Downloads Reg SHO Daily Short Sale Volume data.
    """
    print("\n[Reg SHO Daily Volume] Fetching Data...")
    url = "https://api.finra.org/data/group/otcMarket/name/RegShoDaily"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "limit": 5000
    }
    
    try:
        r = requests.post(url, headers=headers, json=payload)
        r.raise_for_status()
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        output_dir = REG_SHO_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as CSV
        output_file = os.path.join(output_dir, f"reg_sho_daily_{timestamp}.csv")
        with open(output_file, "w") as f:
            f.write(r.text)
            
        line_count = len(r.text.splitlines())
        print(f"Saved {line_count} lines to {output_file}")
            
    except Exception as e:
        print(f"Error downloading Reg SHO Daily: {e}")

def main():
    setup_directories()
    
    # Test Authentication
    print("\n[Authentication Test]")
    token = get_finra_token()
    if token:
        print(f"Success! Token received: {token[:10]}...")
        # Download Authenticated Data
        download_equity_short_interest(token)
        download_reg_sho_daily(token)
    else:
        print("Authentication Failed. Skipping authenticated downloads.")

    # Public Data (Always run)
    download_margin_statistics()
    # download_short_interest_dates() # This endpoint is currently 404ing
    
    print("\nFINRA Public Data Download Complete (Scope: Margin Stats, Short Interest Schedule).")
    print("Note: Detailed stock-level Short Interest data usually requires an API Key/Subscription or browsing individual pages (which is hard to scrape in bulk without a specific target list).")

if __name__ == "__main__":
    main()
