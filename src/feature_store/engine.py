
import polars as pl
from deltalake import write_deltalake
import os
import importlib
import sys
import gc

from src.feature_store.client import FeatureStore
from src.feature_store.decorators import function_map

class FeatureEngine:
    def __init__(self, base_dir: str = None):
        if base_dir is None:
             self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        else:
             self.base_dir = base_dir
             
        self.silver_dir = os.path.join(self.base_dir, "silver")
        self.gold_dir = os.path.join(self.base_dir, "gold")
        
        # Initialize Client (manages Catalog)
        self.client = FeatureStore(base_dir=self.base_dir)
        
        # Load Definitions to register them into function_map
        sys.path.insert(0, self.base_dir)
        # Import all definition modules to trigger @register_feature decorators
        self._import_definitions()

    def _import_definitions(self):
        """Dynamically imports all modules in src/features/definitions"""
        def_dir = os.path.join(self.base_dir, "src", "features", "definitions")
        if not os.path.exists(def_dir):
            print(f"Warning: Definitions directory not found at {def_dir}")
            return
            
        for file in os.listdir(def_dir):
            if file.endswith(".py") and not file.startswith("__"):
                module_name = f"src.features.definitions.{file[:-3]}"
                print(f"Loading definitions from {module_name}...")
                importlib.import_module(module_name)

    def load_silver(self):
        """Loads all silver tables lazily."""
        print("Loading Silver Data...")
        self.lf_market = pl.scan_delta(os.path.join(self.silver_dir, "market_data", "eodhd"))
        self.lf_sec = pl.scan_delta(os.path.join(self.silver_dir, "fundamentals", "sec"))
        
        try:
            self.lf_finra = pl.scan_delta(os.path.join(self.silver_dir, "finra", "short_volume"))
        except:
            print("Warning: FINRA data not found.")
            self.lf_finra = None
            
        try:
            self.lf_macro = pl.scan_delta(os.path.join(self.silver_dir, "macro", "fred"))
        except:
            print("Warning: Macro data not found.")
            self.lf_macro = None
            
    def _create_universal_table(self) -> pl.LazyFrame:
        """Joins all sources into one big LazyFrame anchored on Market Data."""
        print("Creating Universal Table (Join Strategies)...")
        
        # 1. Anchor: Market Data
        # Ensure date is properly typed for joins
        lf = self.lf_market.with_columns(pl.col("date").cast(pl.Date).alias("date_key")).sort(["date_key", "ticker"])
        
        # 2. Join Fundamentals (SEC) - AsOf Backward
        if self.lf_sec is not None:
             # Standardizer output typically has 'date' (filing date) and 'cik'
            lf_sec_sorted = self.lf_sec.with_columns(
                pl.col("date").cast(pl.Date).alias("date_sec")
            ).sort("date_sec")
            
            # Market data needs CIK for this join. 
            # Note: standardize_eodhd_market SHOULD have 'cik' column.
            # If not, we rely on 'ticker' if SEC has it, but SEC is usually CIK-keyed.
            # For now, assuming market data has CIK (checked in previous sessions).
            
            lf = lf.join_asof(
                lf_sec_sorted,
                left_on="date_key",
                right_on="date_sec",
                by="cik", 
                strategy="backward"
            )


        # 3. Join Macro (FRED) - AsOf Backward (using date only)
        if self.lf_macro is not None:
             # Pivot Macro: Series ID -> Columns
             df_macro = self.lf_macro.collect()
             # Note: Pivot is eager.
             df_macro_pivoted = df_macro.pivot(index="date", columns="series_id", values="value", aggregate_function="mean").sort("date")
             

             # Create Join Key 
             df_macro_pivoted = df_macro_pivoted.with_columns(pl.col("date").cast(pl.Date).alias("date_macro_join_key"))
             
             # Rename original 'date' to avoid collision/suffix logic completely
             df_macro_pivoted = df_macro_pivoted.rename({"date": "date_macro_garbage"})
             
             # Lazy again
             lf_macro_lazy = df_macro_pivoted.lazy()
             
             # Join using specific key, drop the key and garbage after
             lf = lf.join_asof(
                 lf_macro_lazy,
                 left_on="date_key", 
                 right_on="date_macro_join_key",
                 strategy="backward"
             ).drop(["date_macro_join_key", "date_macro_garbage", "date_right"], strict=False)


        # 4. Join FINRA
        if self.lf_finra is not None:
            # Daily join on Ticker/Date
            
            # Prepare RHS: Select ONLY valid feature columns + Keys
            # We must verify these cols exist. Scan schema first? 
            # Lazy approach: Select cols that we know 'standardize_finra' writes.
            # standardize_finra writes: ticker, date, short_volume, short_exempt_volume, total_volume, short_interest, avg_daily_volume
            
            finra_cols = ["ticker", "short_volume", "short_exempt_volume", "total_volume", "short_interest", "avg_daily_volume"]
            # Filter what actually exists
            available = self.lf_finra.collect_schema().names()
            finra_cols = [c for c in finra_cols if c in available]
            
            lf_finra_clean = self.lf_finra.with_columns(
                pl.col("date").cast(pl.Date).alias("date_finra_key")
            ).select(
                finra_cols + ["date_finra_key"] # Explicitly select only these to avoid 'year' collision
            )
            
            lf = lf.join(
                lf_finra_clean,
                left_on=["ticker", "date_key"],
                right_on=["ticker", "date_finra_key"],
                how="left"
            ).drop(["date_finra_key"], strict=False)

        return lf

    def run(self, year_start: int = 2010):
        """Main execution flow."""
        self.load_silver()
        
        lf_universal = self._create_universal_table()
        
        print(f"Applying {len(function_map)} Registered Features...")
        
        # Execution Strategy:
        # We assume independent feature functions or handled dependencies.
        # Polars lazy graph handles optimization.
        
        for name, func in function_map.items():
            # print(f"  -> Planning {name}...")
            lf_universal = func(lf_universal)
            
        # Materialize by Year
        print("Materializing to Gold...")
        lf_final = lf_universal.with_columns(pl.col("date").dt.year().alias("year"))
        
        output_path = os.path.join(self.gold_dir, "features")
        
        # Process by year to manage RAM
        years = range(year_start, 2026)
        
        for y in years:
            print(f"Processing Year {y}...")
            try:
                # Filter, Collect, Write
                df_year = lf_final.filter(pl.col("year") == y).collect()
                
                if df_year.height > 0:
                    write_deltalake(
                        output_path,
                        df_year.to_arrow(),
                        partition_by=["year"],
                        mode="append",
                        schema_mode="merge"
                    )
                    print(f"  > Wrote {df_year.height} rows.")
                    
                del df_year
                gc.collect()
                
            except Exception as e:
                print(f"Error processing year {y}: {e}")
                # Optional: Import traceback to see full error if debugging
                # import traceback; traceback.print_exc()

        print("Feature Generation Complete.")

if __name__ == "__main__":
    engine = FeatureEngine()
    engine.run(year_start=2010)
