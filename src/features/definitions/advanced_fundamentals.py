
import polars as pl
from src.feature_store.decorators import register_feature

def safe_div(a: str, b: str, alias: str) -> pl.Expr:
    res = pl.col(a) / pl.col(b)
    return pl.when(res.is_infinite()).then(None).otherwise(res).fill_nan(None).alias(alias)

@register_feature(name="fundamental:dupont", description="DuPont Identity Components", category="fundamental", tags=["quality", "dupont"])
def compute_dupont_analysis(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    # Components:
    # 1. Net Profit Margin = Net Income / Revenue (Already in 'fundamental.py' as net_margin)
    # 2. Asset Turnover = Revenue / Total Assets
    if "totalRevenue" in schema and "totalAssets" in schema:
        exprs.append(safe_div("totalRevenue", "totalAssets", "asset_turnover"))
        
    # 3. Equity Multiplier = Total Assets / Shareholder Equity
    if "totalAssets" in schema and "totalStockholderEquity" in schema:
        exprs.append(safe_div("totalAssets", "totalStockholderEquity", "equity_multiplier"))
        
    if exprs:
        return lf.with_columns(exprs)
    return lf

@register_feature(name="fundamental:altman_z", description="Altman Z-Score (Bankruptcy Risk)", category="fundamental", tags=["risk", "distress"])
def compute_altman_z(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    
    # Altman Z-Score = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
    # A = Working Capital / Total Assets
    # B = Retained Earnings / Total Assets
    # C = EBIT / Total Assets
    # D = Market Value of Equity / Total Liabilities
    # E = Sales / Total Assets
    
    # Check coverage
    required = ["totalAssets", "totalLiab", "retainedEarnings", "totalRevenue", "ebit"]
    # We might need to derive Market Value if 'market_cap' isn't present
    # close * commonStockSharesOutstanding
    
    if not all(col in schema for col in required):
        return lf
        
    # Helper to calculate components
    lf_calc = lf
    
    # Ensure Market Cap exists
    if "market_cap" not in schema and "close" in schema and "commonStockSharesOutstanding" in schema:
        lf_calc = lf_calc.with_columns((pl.col("close") * pl.col("commonStockSharesOutstanding")).alias("market_cap"))
    elif "market_cap" not in lf_calc.collect_schema().names():
        return lf # Can't compute D without market value
        
    # Ensure Working Capital exists
    if "netWorkingCapital" in schema:
         lf_calc = lf_calc.with_columns(pl.col("netWorkingCapital").alias("working_capital"))
    elif "totalCurrentAssets" in schema and "totalCurrentLiabilities" in schema:
         lf_calc = lf_calc.with_columns((pl.col("totalCurrentAssets") - pl.col("totalCurrentLiabilities")).alias("working_capital"))
    else:
        return lf
        
    # Apply Formula
    # Fill Nones with 0 for robustness? Or keep None to strictly flag missing data?
    # Strict is better for ML features generally.
    
    return lf_calc.with_columns(
        (
            1.2 * (pl.col("working_capital") / pl.col("totalAssets")) +
            1.4 * (pl.col("retainedEarnings") / pl.col("totalAssets")) +
            3.3 * (pl.col("ebit") / pl.col("totalAssets")) +
            0.6 * (pl.col("market_cap") / pl.col("totalLiab")) +
            1.0 * (pl.col("totalRevenue") / pl.col("totalAssets"))
        ).alias("altman_z_score")
    )

@register_feature(name="fundamental:cash_flow_ratios", description="Operating and Free Cash Flow Ratios", category="fundamental", tags=["quality", "cash_flow"])
def compute_cash_flow_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    # OCF Ratio = Operating Cash Flow / Current Liabilities
    if "totalCashFromOperatingActivities" in schema and "totalCurrentLiabilities" in schema:
        exprs.append(safe_div("totalCashFromOperatingActivities", "totalCurrentLiabilities", "operating_cash_flow_ratio"))
        
    # FCF Yield = Free Cash Flow / Market Cap
    # Assuming 'freeCashFlow' exists or 'totalCashFromOperatingActivities' - 'capitalExpenditures' (usually in investing activities)
    if "freeCashFlow" in schema:
         # Need market cap
         if "market_cap" in schema:
             exprs.append(safe_div("freeCashFlow", "market_cap", "fcf_yield"))
         elif "close" in schema and "commonStockSharesOutstanding" in schema:
             exprs.append((pl.col("freeCashFlow") / (pl.col("close") * pl.col("commonStockSharesOutstanding"))).alias("fcf_yield"))

    if exprs:
        return lf.with_columns(exprs)
    return lf
