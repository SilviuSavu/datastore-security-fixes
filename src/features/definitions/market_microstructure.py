
import polars as pl
from src.feature_store.decorators import register_feature

@register_feature(name="microstructure:short_interest", description="Days to Cover & Short Ratio", category="microstructure", tags=["short_interest"])
def compute_short_interest_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema().names()
    exprs = []
    
    # FINRA Short Interest (Bi-weekly)
    if "short_interest" in schema and "avg_daily_volume" in schema:
        # Avoid div by zero
        exprs.append(
            (pl.col("short_interest") / (pl.col("avg_daily_volume") + 1.0)).alias("days_to_cover")
        )
        
    # FINRA Reg SHO (Daily Volume)
    if "short_volume" in schema and "total_volume" in schema:
         exprs.append(
            (pl.col("short_volume") / (pl.col("total_volume") + 1.0)).alias("short_volume_ratio")
         )

    if exprs:
        return lf.with_columns(exprs)
    return lf
