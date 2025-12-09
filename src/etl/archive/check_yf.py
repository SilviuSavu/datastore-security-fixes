import yfinance as yf
import pandas as pd

t = yf.Ticker("AAPL")
hist = t.history(period="1mo", auto_adjust=False)
print("Columns with auto_adjust=False:")
print(hist.columns)
print(hist.head(2))

hist_adj = t.history(period="1mo", auto_adjust=True)
print("\nColumns with auto_adjust=True:")
print(hist_adj.columns)
print(hist_adj.head(2))
