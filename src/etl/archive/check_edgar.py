from edgar import Company, set_identity
import os

set_identity('DataStore silviu.savu@example.com')

try:
    c = Company("0000320193") # Apple
    print(f"Tickers for Apple: {c.tickers}")
    
    c2 = Company("0001067983") # Berkshire Hathaway
    print(f"Tickers for Berkshire: {c2.tickers}")
    
except Exception as e:
    print(f"Error: {e}")
