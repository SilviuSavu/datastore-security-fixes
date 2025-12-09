import os
import sys
import polars as pl
from deltalake import DeltaTable

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging import logger, DataProcessingError

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(BASE_DIR, "silver")

MARKET_PATH = os.path.join(SILVER_DIR, "market_data", "eodhd")
SEC_PATH = os.path.join(SILVER_DIR, "fundamentals", "sec")
FINRA_VOL_PATH = os.path.join(SILVER_DIR, "finra", "short_volume")

def check_finra():
    """Check FINRA data integrity and provide diagnostic information."""
    logger.info("=== Checking FINRA Data ===")
    try:
        logger.debug(f"Scanning FINRA data from: {FINRA_VOL_PATH}")
        lf_finra = pl.scan_delta(FINRA_VOL_PATH)
        schema = lf_finra.collect_schema()
        logger.info(f"FINRA Schema: {schema}")

        # Sample data
        logger.debug("Sample data (first 5 rows):")
        sample_data = lf_finra.head(5).collect()
        logger.debug(f"Sample data:\n{sample_data}")

        # Check Date Range and Ticker format
        logger.debug("Computing FINRA statistics...")
        stats = lf_finra.select([
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
            pl.col("ticker").n_unique().alias("n_tickers")
        ]).collect()

        logger.info(f"FINRA Statistics:\n{stats}")

    except FileNotFoundError as e:
        logger.error(f"FINRA data directory not found: {FINRA_VOL_PATH} - {str(e)}")
        raise DataProcessingError(f"FINRA data unavailable: {FINRA_VOL_PATH}") from e
    except Exception as e:
        logger.error(f"Unexpected error reading FINRA data: {str(e)}")
        logger.exception("Full traceback:")
        raise

def check_market():
    """Check market data integrity and provide diagnostic information."""
    logger.info("=== Checking Market Data ===")
    try:
        logger.debug(f"Scanning market data from: {MARKET_PATH}")
        lf = pl.scan_delta(MARKET_PATH)
        schema = lf.collect_schema()
        logger.info(f"Market Schema: {schema}")

        sample_data = lf.head(5).collect()
        logger.debug(f"Market sample data:\n{sample_data}")

    except FileNotFoundError as e:
        logger.error(f"Market data directory not found: {MARKET_PATH} - {str(e)}")
        raise DataProcessingError(f"Market data unavailable: {MARKET_PATH}") from e
    except Exception as e:
        logger.error(f"Unexpected error reading market data: {str(e)}")
        logger.exception("Full traceback:")
        raise

def check_overlap():
    """Check data overlap between market and FINRA datasets."""
    logger.info("=== Checking Data Overlap ===")
    try:
        logger.debug("Loading datasets for overlap analysis")
        lf_m = pl.scan_delta(MARKET_PATH)
        lf_f = pl.scan_delta(FINRA_VOL_PATH)

        # Find intersection
        logger.debug("Computing unique tickers...")
        tickers_m = lf_m.select("ticker").unique().collect()["ticker"]
        tickers_f = lf_f.select("ticker").unique().collect()["ticker"]

        common = set(tickers_m).intersection(set(tickers_f))
        logger.info(f"Common Tickers: {len(common)}")

        if common:
            t = list(common)[0]
            logger.info(f"Analyzing overlap for sample ticker: {t}")

            # Check dates for this ticker
            logger.debug("Computing date ranges...")
            dates_m = lf_m.filter(pl.col("ticker")==t).select("date").collect()["date"].sort()
            dates_f = lf_f.filter(pl.col("ticker")==t).select("date").collect()["date"].sort()

            logger.info(f"Market Dates: {len(dates_m)} ({dates_m[0]} to {dates_m[-1]})")
            logger.info(f"FINRA Dates:  {len(dates_f)} ({dates_f[0]} to {dates_f[-1]})")

            # Check Join keys
            logger.debug("Schema type information:")
            market_schema = lf_m.collect_schema()
            finra_schema = lf_f.collect_schema()
            logger.debug(f"Market date type: {market_schema['date']}, ticker type: {market_schema['ticker']}")
            logger.debug(f"FINRA date type: {finra_schema['date']}, ticker type: {finra_schema['ticker']}")

    except FileNotFoundError as e:
        logger.error(f"Data directory not found during overlap check: {str(e)}")
        raise DataProcessingError("Data overlap check failed due to missing files") from e
    except Exception as e:
        logger.error(f"Unexpected error during overlap analysis: {str(e)}")
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    check_finra()
    check_market()
    check_overlap()
