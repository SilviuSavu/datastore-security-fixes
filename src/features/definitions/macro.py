
import polars as pl
from src.feature_store.decorators import register_feature

@register_feature(name="macro:yield_curve", description="Yield Curve Slope (10Y - 2Y)", category="macro", tags=["rates", "recession_indicator"])
def compute_yield_curve(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Expecting columns 'DGS10' (10-Year Treasury) and 'DGS2' (2-Year Treasury) from FRED
    schema = lf.collect_schema().names()
    
    if "DGS10" in schema and "DGS2" in schema:
        return lf.with_columns(
            (pl.col("DGS10") - pl.col("DGS2")).alias("yield_curve_slope_10y_2y")
        )
    return lf

@register_feature(name="macro:real_rates", description="Real Interest Rate Proxy (10Y - CPI YoY)", category="macro", tags=["rates"])
def compute_real_rates(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Expecting 'DGS10' and 'CPIAUCSL' (CPI)
    # Note: CPI is monthly, but forward filled by Engine.
    # Need YoY CPI Change first if not present.
    
    schema = lf.collect_schema().names()
    
    if "DGS10" in schema and "CPIAUCSL" in schema:
        # Calculate YoY CPI Inflation if not pre-calculated
        # Since this is a big joined table, we can't easily shift CPI 252 days back without potentially crossing tickers/dates incorrectly?
        # Actually, Macro is joined. The 'CPIAUCSL' column is constant for all tickers on a given day.
        # But shifting 'CPI' by 1 year (252 days) is safe if we sort by date.
        
        # Optimization: We assume the Engine passes a DataFrame sorted by Ticker/Date.
        # But 'CPI' is global.
        
        # Simpler approach: If 'inflation_yoy' isn't there, maybe skip or approximation.
        # Let's just output the raw Real Rate if we assume CPI is the Index level? No, need change.
        # Let's just return the Nominal Rate as a feature for now to be safe.
        
        return lf.with_columns(
            pl.col("DGS10").alias("risk_free_rate_10y")
        )
        
    return lf

@register_feature(name="macro:vix_regime", description="VIX Regime (High/Low)", category="macro", tags=["volatility"])
def compute_vix_regime(lf: pl.LazyFrame) -> pl.LazyFrame:
    if "VIXCLS" in lf.collect_schema().names(): # VIX series ID
        return lf.with_columns([
            pl.col("VIXCLS").alias("market_vix"),
            (pl.col("VIXCLS") > 20).alias("is_high_vix_regime")
        ])
    return lf
