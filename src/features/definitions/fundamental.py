
import polars as pl
from src.feature_store.decorators import register_feature

def safe_div(a: str, b: str, alias: str) -> pl.Expr:
    res = pl.col(a) / pl.col(b)
    return pl.when(res.is_infinite()).then(None).otherwise(res).fill_nan(None).alias(alias)

@register_feature(name="fundamental:valuation", description="PE, PB, EV/EBITDA", category="fundamental", tags=["valuation"])
def compute_valuation_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    # PE
    if "epsActual" in schema:
        exprs.append(safe_div("close", "epsActual", "pe_ratio"))
        exprs.append(safe_div("epsActual", "close", "earnings_yield"))
        
    # PB
    if "totalStockholderEquity" in schema and "commonStockSharesOutstanding" in schema:
        lf = lf.with_columns((pl.col("totalStockholderEquity") / pl.col("commonStockSharesOutstanding")).alias("bvps"))
        exprs.append(safe_div("close", "bvps", "pb_ratio"))
        
    # EV / EBITDA
    # Need generic logic if 'ev' isn't pre-calculated
    if "market_cap" not in schema and "commonStockSharesOutstanding" in schema:
         lf = lf.with_columns((pl.col("close") * pl.col("commonStockSharesOutstanding")).alias("market_cap"))
         
    if "market_cap" in lf.collect_schema().names() and "netDebt" in schema:
        lf = lf.with_columns((pl.col("market_cap") + pl.col("netDebt").fill_null(0)).alias("enterprise_value"))
        
    if "enterprise_value" in lf.collect_schema().names() and "ebitda" in schema:
        exprs.append(safe_div("enterprise_value", "ebitda", "ev_to_ebitda"))

    if exprs:
        return lf.with_columns(exprs)
    return lf

@register_feature(name="fundamental:profitability", description="Margins, ROE, ROA", category="fundamental", tags=["profitability"])
def compute_profitability_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    if "grossProfit" in schema and "totalRevenue" in schema:
        exprs.append(safe_div("grossProfit", "totalRevenue", "gross_margin"))
        
    if "operatingIncome" in schema and "totalRevenue" in schema:
        exprs.append(safe_div("operatingIncome", "totalRevenue", "operating_margin"))
        
    if "netIncome" in schema and "totalRevenue" in schema:
        exprs.append(safe_div("netIncome", "totalRevenue", "net_margin"))
        
    if "netIncome" in schema and "totalAssets" in schema:
        exprs.append(safe_div("netIncome", "totalAssets", "return_on_assets"))

    if "netIncome" in schema and "totalStockholderEquity" in schema:
        exprs.append(safe_div("netIncome", "totalStockholderEquity", "return_on_equity"))

    if exprs:
        return lf.with_columns(exprs)
    return lf

@register_feature(name="fundamental:liquidity_leverage", description="Current Ratio, Debt/Equity", category="fundamental", tags=["risk"])
def compute_risk_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    if "totalCurrentAssets" in schema and "totalCurrentLiabilities" in schema:
        exprs.append(safe_div("totalCurrentAssets", "totalCurrentLiabilities", "current_ratio"))
        
    if "totalLiab" in schema and "totalStockholderEquity" in schema:
        exprs.append(safe_div("totalLiab", "totalStockholderEquity", "debt_to_equity"))
        
    if "netDebt" in schema and "ebitda" in schema:
        exprs.append(safe_div("netDebt", "ebitda", "net_debt_to_ebitda"))
        
    if exprs:
        return lf.with_columns(exprs)
    return lf

@register_feature(name="fundamental:growth", description="Revenue and Income Growth (YoY)", category="fundamental", tags=["growth"])
def compute_growth_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    if "totalRevenue" in schema:
        rev_growth = (pl.col("totalRevenue") / pl.col("totalRevenue").shift(252).over("ticker")) - 1
        exprs.append(
            pl.when(rev_growth.is_infinite()).then(None).otherwise(rev_growth)
            .fill_nan(None)
            .alias("revenue_growth_yoy")
        )
        
    if "netIncome" in schema:
         net_growth = (pl.col("netIncome") / pl.col("netIncome").shift(252).over("ticker")) - 1
         exprs.append(
            pl.when(net_growth.is_infinite()).then(None).otherwise(net_growth)
            .fill_nan(None)
            .alias("net_income_growth_yoy")
        )
        
    if exprs:
        return lf.with_columns(exprs)
    return lf
