import polars as pl
import numpy as np
import mlx.core as mx
import gc
import time

class HybridDataLoader:
    def __init__(self, gold_path, start_year, end_year, price_features, fund_features, lookback=252, batch_size=1024):
        self.gold_path = gold_path
        self.start_year = start_year
        self.end_year = end_year
        self.price_features = price_features
        self.fund_features = fund_features
        self.lookback = lookback
        self.batch_size = batch_size
        
        self.means = {}
        self.stds = {}

    def set_stats(self, means, stds):
        self.means = means
        self.stds = stds

    def load_year(self, year, prev_year_data=None):
        print(f"    Loading year {year}...", end="", flush=True)
        t0 = time.time()
        
        cols = list(set(self.price_features + self.fund_features + ["ticker", "date", "target_20d_fwd", "year"]))
        
        df_curr = pl.scan_delta(self.gold_path).filter(pl.col("year") == year).select(cols).collect()
        
        if year > self.start_year:
            df_prev = pl.scan_delta(self.gold_path).filter(pl.col("year") == year - 1).select(cols).collect()
            if df_prev.height > 0:
                prev_tail = df_prev.sort("date").group_by("ticker").tail(self.lookback)
                df_combined = pl.concat([prev_tail, df_curr], how="vertical")
            else:
                df_combined = df_curr
        else:
            df_combined = df_curr
        
        df_combined = df_combined.sort(["ticker", "date"])
        
        msg_price = []
        msg_fund = []
        
        for f in self.price_features:
            m = self.means.get(f, 0.0)
            s = self.stds.get(f, 1.0)
            msg_price.append((((pl.col(f).fill_nan(0.0).fill_null(0.0) - m) / s).clip(-10.0, 10.0)).cast(pl.Float32))

        for f in self.fund_features:
            m = self.means.get(f, 0.0)
            s = self.stds.get(f, 1.0)
            msg_fund.append((((pl.col(f).fill_nan(0.0).fill_null(0.0) - m) / s).clip(-10.0, 10.0)).cast(pl.Float32))

        df_combined = df_combined.with_columns(msg_price + msg_fund)
        df_combined = df_combined.rename({"target_20d_fwd": "target_return"})
        
        print(f" {df_combined.height} rows ({time.time()-t0:.1f}s)")
        
        return df_combined

    def get_batches(self, df_year):
        if df_year is None or df_year.height == 0:
            return

        dat_price = df_year.select(self.price_features).to_numpy()
        dat_fund = df_year.select(self.fund_features).to_numpy()
        targets = df_year["target_return"].to_numpy()
        
        valid_window = (pl.col("ticker") == pl.col("ticker").shift(self.lookback - 1))
        valid_target = pl.col("target_return").is_not_null()
        
        df_year = df_year.with_columns(
            (valid_window & valid_target).alias("is_valid_row")
        )
        
        valid_indices = df_year.with_row_index().filter(pl.col("is_valid_row"))["index"].cast(pl.Int64).to_numpy()
        
        if len(valid_indices) == 0:
            return

        np.random.shuffle(valid_indices)
        
        for start in range(0, len(valid_indices), self.batch_size):
            end = min(start + self.batch_size, len(valid_indices))
            batch_idx = valid_indices[start:end]
            
            offsets = np.arange(self.lookback)
            all_indices = batch_idx[:, None] - self.lookback + 1 + offsets
            
            X_p = dat_price[all_indices]
            X_f = dat_fund[batch_idx]
            y_b = targets[batch_idx]
            
            yield X_p, X_f, y_b
