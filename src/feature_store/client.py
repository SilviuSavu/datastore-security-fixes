
import os
import json
import polars as pl
from datetime import datetime

class FeatureStore:
    def __init__(self, base_dir=None):
        if base_dir is None:
            # Default to project root based on file location
            self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        else:
            self.base_dir = base_dir
            
        self.catalog_path = os.path.join(self.base_dir, "src", "feature_store", "catalog.json")
        self.gold_path = os.path.join(self.base_dir, "gold", "features")
        
        self.catalog = self._load_catalog()

    def _load_catalog(self):
        """Loads the metadata catalog."""
        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, "r") as f:
                return json.load(f)
        return {}
    
    def _save_catalog(self):
        """Persists catalog to disk."""
        with open(self.catalog_path, "w") as f:
            json.dump(self.catalog, f, indent=2)

    def register_feature(self, name, description, category="technical", status="production", group="technical", tags=[]):
        """Registers or updates a feature in the catalog."""
        self.catalog[name] = {
            "description": description,
            "category": category,
            "group": group,
            "status": status,
            "tags": tags,
            "last_updated": datetime.now().isoformat()
        }
        self._save_catalog()
        print(f"Registered feature: {name} (Group: {group})")

    def list_features(self, category=None, status=None, group=None):
        """Lists available features, optionally filtered."""
        results = []
        for name, meta in self.catalog.items():
            if category and meta.get("category") != category:
                continue
            if status and meta.get("status") != status:
                continue
            if group and meta.get("group") != group:
                continue
            
            row = {"name": name}
            row.update(meta)
            results.append(row)
            
        return pl.DataFrame(results)

    def get_features(self, tickers=None, start_date=None, end_date=None, features=None, lazy=False):
        """
        Retrieves feature data from the Gold Layer (Monolithic Delta Table).
        
        Args:
            tickers (list): List of ticker symbols (e.g., ['AAPL', 'MSFT'])
            start_date (str): 'YYYY-MM-DD'
            end_date (str): 'YYYY-MM-DD'
            features (list): Specific columns to select. If None, returns all columns.
            lazy (bool): If True, returns a Polars LazyFrame instead of collecting.
        
        Returns:
            pl.DataFrame or pl.LazyFrame
        """
        if not os.path.exists(self.gold_path):
            raise FileNotFoundError(f"Gold layer not found at {self.gold_path}")
        
        # Lazy scan the monolithic table
        lf = pl.scan_delta(self.gold_path)
        
        # Apply filters (predicate pushdown for efficiency)
        if tickers:
            lf = lf.filter(pl.col("ticker").is_in(tickers))
        if start_date:
            lf = lf.filter(pl.col("date") >= pl.lit(start_date).str.to_date())
        if end_date:
            lf = lf.filter(pl.col("date") <= pl.lit(end_date).str.to_date())
        
        # Select columns
        if features:
            # Always include key columns
            cols = list(set(["date", "ticker", "year"] + features))
            # Only select columns that exist in the schema
            available = set(lf.collect_schema().names())
            cols = [c for c in cols if c in available]
            lf = lf.select(cols)
        
        if lazy:
            return lf
        return lf.collect()

