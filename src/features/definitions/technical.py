
import polars as pl
from src.feature_store.decorators import register_feature

@register_feature(name="technical:log_returns", description="Daily Log Returns", category="technical")
def compute_log_returns(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns([
        (pl.col("close") / pl.col("close").shift(1)).log().over("ticker").alias("log_return_1d")
    ])

@register_feature(name="technical:volatility_yz", description="Yang-Zhang Volatility (20D)", category="technical", tags=["volatility"])
def compute_yang_zhang_volatility(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Requires: open, high, low, close
    # Helper cols
    lf = lf.with_columns([
        (pl.col("open") / pl.col("close").shift(1)).log().alias("ret_overnight"),
        (pl.col("close") / pl.col("open")).log().alias("ret_open_close"),
        (pl.col("high") / pl.col("close")).log().alias("log_hc"),
        (pl.col("high") / pl.col("open")).log().alias("log_ho"),
        (pl.col("low") / pl.col("close")).log().alias("log_lc"),
        (pl.col("low") / pl.col("open")).log().alias("log_lo"),
    ])
    
    N = 20
    k = 0.34 / (1.34 + (N + 1) / (N - 1))
    
    lf = lf.with_columns([
        pl.col("ret_overnight").rolling_var(N).over("ticker").alias("var_overnight"),
        pl.col("ret_open_close").rolling_var(N).over("ticker").alias("var_open_close"),
        ((pl.col("log_hc") * pl.col("log_ho")) + (pl.col("log_lc") * pl.col("log_lo")))
        .rolling_mean(N).over("ticker").alias("var_rs")
    ])
    
    return lf.with_columns([
        ((pl.col("var_overnight") + k * pl.col("var_open_close") + (1 - k) * pl.col("var_rs")).sqrt() * (252**0.5))
        .alias("volatility_yang_zhang_20d")
    ]).drop(["ret_overnight", "ret_open_close", "log_hc", "log_ho", "log_lc", "log_lo", "var_overnight", "var_open_close", "var_rs"])

@register_feature(name="technical:rsi_14", description="Relative Strength Index (14D)", category="technical", tags=["momentum"])
def compute_rsi_14(lf: pl.LazyFrame) -> pl.LazyFrame:
    delta = pl.col("close").diff()
    up = (delta.clip(lower_bound=0))
    down = (delta.clip(upper_bound=0).abs())
    
    avg_gain = up.rolling_mean(14).over("ticker")
    avg_loss = down.rolling_mean(14).over("ticker")
    
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    return lf.with_columns(rsi.alias("rsi_14"))

@register_feature(name="technical:macd", description="MACD (12, 26, 9)", category="technical", tags=["trend"])
def compute_macd(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = lf.with_columns([
        pl.col("close").ewm_mean(span=12, adjust=False).over("ticker").alias("ema_12"),
        pl.col("close").ewm_mean(span=26, adjust=False).over("ticker").alias("ema_26"),
    ])
    
    lf = lf.with_columns([
        (pl.col("ema_12") - pl.col("ema_26")).alias("macd_line")
    ])
    
    lf = lf.with_columns([
        pl.col("macd_line").ewm_mean(span=9, adjust=False).over("ticker").alias("macd_signal")
    ])
    
    return lf.with_columns([
        (pl.col("macd_line") - pl.col("macd_signal")).alias("macd_hist")
    ]).drop(["ema_12", "ema_26"])

@register_feature(name="technical:averages", description="SMA 50/200", category="technical", tags=["trend"])
def compute_moving_averages(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns([
        pl.col("close").rolling_mean(50).over("ticker").alias("sma_50"),
        pl.col("close").rolling_mean(200).over("ticker").alias("sma_200"),
    ])
