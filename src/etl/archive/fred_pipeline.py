import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load Environment
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "bronze", "macro", "fred")

# Core Risk & Macro Series
SERIES_MAP = {
    "GDP": "Gross Domestic Product (Quarterly)",
    "GDPC1": "Real Gross Domestic Product (Quarterly)",
    "CPIAUCSL": "Consumer Price Index for All Urban Consumers (Monthly)",
    "FEDFUNDS": "Federal Funds Effective Rate (Monthly)",
    "DGS10": "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity (Daily)",
    "DGS2": "Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity (Daily)",
    "T10Y2Y": "10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity (Daily)",
    "UNRATE": "Unemployment Rate (Monthly)",
    "VIXCLS": "CBOE Volatility Index (VIX) (Daily)",
    "M2SL": "M2 Money Stock (Monthly)",
    
    # Credit Spreads
    "BAMLC0A0CM": "ICE BofA US Corp Master Option-Adjusted Spread (Daily)",
    "BAMLH0A0HYM2": "ICE BofA US High Yield Index Option-Adjusted Spread (Daily)",
    
    # Business Cycle & Production
    "INDPRO": "Industrial Production: Total Index (Monthly)",
    "TCU": "Capacity Utilization: Total Industry (Monthly)",
    "HOUST": "Housing Starts: Total: New Privately Owned Housing Units Started (Monthly)",
    
    # Consumer & Sentiment
    "UMCSENT": "University of Michigan: Consumer Sentiment (Monthly)",
    "RRSFS": "Real Retail and Food Services Sales (Monthly)",
    
    # Inflation Expectations
    "T10YIE": "10-Year Breakeven Inflation Rate (Daily)",
    "T5YIE": "5-Year Breakeven Inflation Rate (Daily)",
    
    # Liquidity & Markets
    "WALCL": "Assets: Total Assets: Total Assets (Weekly - Fed Balance Sheet)",
    "DTWEXBGS": "Nominal Broad U.S. Dollar Index (Daily)",
    "DCOILWTICO": "Crude Oil Prices: West Texas Intermediate (WTI) (Daily)",
    
    # Institutional Risk & Recession
    "STLFSI3": "St. Louis Fed Financial Stress Index (Weekly)",
    "TEDRATE": "TED Spread (Daily)",
    "BAA10Y": "Moody's Seasoned Baa Corporate Bond Yield Relative to Yield on 10-Year Treasury Constant Maturity (Daily)",
    "SAHMREALTIME": "Real-time Sahm Rule Recession Indicator (Monthly)",
    "WEI": "Weekly Economic Index (Lewis-Mertens-Stock) (Weekly)",
    
    # Global & Commodities (Phase 2 Fallback)
    "GOLDPMGBD228NLBM": "Gold Fixing Price 3:00 P.M. (London time) in London Bullion Market, based in U.S. Dollars (Daily)",
    "PCOPPUSDM": "Global price of Copper (Monthly)",
    "DEXJPUS": "Japanese Yen to U.S. Dollar Spot Exchange Rate (Daily)",
    "DTB3": "3-Month Treasury Bill Secondary Market Rate, Discount Basis (Daily)",
    "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average in the United States (Weekly)"
}

def setup_directories():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")

def fetch_series(series_id, description):
    """
    Fetches full history for a FRED series.
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "1776-07-04" # Get everything
    }
    
    print(f"Fetching {series_id} ({description})...")
    
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        observations = data.get("observations", [])
        if not observations:
            print(f"Warning: No observations found for {series_id}")
            return
            
        # Parse into DataFrame
        df = pd.DataFrame(observations)
        
        # Clean up: value, date. Drop realtime_start/end
        df = df[["date", "value"]]
        
        # FRED returns '.' for missing values in some daily series
        df = df[df["value"] != "."]
        
        # Save
        filepath = os.path.join(DATA_DIR, f"{series_id}.csv")
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} rows to {filepath}")
        
    except Exception as e:
        print(f"Error fetching {series_id}: {e}")

def main():
    if not FRED_API_KEY:
        print("Error: FRED_API_KEY not found in .env")
        return

    setup_directories()
    
    print(f"Starting FRED Macro Download ({len(SERIES_MAP)} series)...")
    
    for series_id, description in SERIES_MAP.items():
        fetch_series(series_id, description)
        
    print("\nFRED Download Complete.")

if __name__ == "__main__":
    main()
