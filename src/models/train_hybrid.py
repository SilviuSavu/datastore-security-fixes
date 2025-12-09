import os
import sys
import time
import numpy as np
import polars as pl
import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
import mlx.utils
from datetime import datetime
from tqdm import tqdm

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

from src.data.hybrid_loader import HybridDataLoader
from src.models.hybrid_model import FinancialForecaster
from src.models.train_mlx import get_feature_stats # Reuse stat computation

# Config
BATCH_SIZE = 512 # Smaller batch size due to Transformer memory usage
EPOCHS = 10 
LEARNING_RATE = 1e-4
START_YEAR = 1996
END_YEAR = 2025
LOOKBACK = 252 # 1 Year Trading Days
HIDDEN_DIM = 256
NUM_HEADS = 4
NUM_LAYERS = 2

def split_features(schema_names):
    """
    Heuristic split of features into Price/Technical (Time-Series) vs Fundamentals (Static).
    """
    technical_keywords = [
        'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'log_return', 
        'volatility', 'sma_', 'bb_', 'ema_', 'macd', 'momentum', 'rsi', 
        'efficiency', 'amihud', 'stoch', 'skewness'
    ]
    
    exclude = [
        "date", "ticker", "target_1d", "cik", "year", "adsh", "cik_right", "name", 
        "period", "filed", "form", "fy", "fp", "currency_symbol", "filing_date", 
        "date_right", "year_right", "filed_date", "id", "target_return", "target_20d_fwd", "is_valid_row", "index"
    ]
    
    price_feats = []
    fund_feats = []
    
    for c in schema_names:
        if c in exclude:
            continue
        
        # Check blacklist/whitelist
        is_tech = any(k in c for k in technical_keywords)
        if is_tech:
            price_feats.append(c)
        else:
            # Assume everything else numeric is fundamental
            fund_feats.append(c)
            
    return price_feats, fund_feats

def main():
    print(f"MLX Device: {mx.default_device()}")
    
    # 1. Feature Detection
    print("Detecting features...")
    # Using a dummy scan to get schema
    schema = pl.scan_delta(GOLD_PATH).collect_schema()
    all_cols = schema.names()
    
    # Filter only numeric columns from schema for actual usage
    numeric_cols = [c for c in all_cols if schema[c].is_numeric()]
    
    price_feats, fund_feats = split_features(numeric_cols)
    print(f"Price Features: {len(price_feats)} | Fund Features: {len(fund_feats)}")
    
    all_feats = price_feats + fund_feats
    
    # 2. Global Stats (Reuse existing logic)
    means_arr, stds_arr = get_feature_stats(all_feats) 
    
    # Convert to dict for Loader
    means_dict = {f: m for f, m in zip(all_feats, means_arr)}
    stds_dict = {f: s for f, s in zip(all_feats, stds_arr)}

    # 3. Initialize Loader
    loader = HybridDataLoader(
        GOLD_PATH, 
        START_YEAR, 
        END_YEAR, 
        price_feats, 
        fund_feats, 
        lookback=LOOKBACK, 
        batch_size=BATCH_SIZE
    )


        
    loader.set_stats(means_dict, stds_dict)
    
    # 4. Initialize Model
    model = FinancialForecaster(
        price_dim=len(price_feats),
        fund_dim=len(fund_feats),
        hidden_dim=HIDDEN_DIM,
        num_heads=NUM_HEADS,
        num_layers=NUM_LAYERS
    )
    mx.eval(model.parameters())
    
    print(f"Model Initialized.")
    
    # Clip grads in optimizer? MLX doesn't have inplace clip on grads easily in functional?
    # Actually AdamW usually handles it well, but let's reduce LR or clip.
    # We can clip global norm.
    optimizer = optim.AdamW(learning_rate=1e-5) # Reduced LR
    
    # Define Functional Loss for Compilation
    def loss_fn(params, X_p, X_f, y):
        # Update model with params (functional style)
        model.update(params)
        pred = model(X_p, X_f)
        y = y.reshape(-1, 1)
        return mx.mean((pred - y) ** 2)
    
    # Compile the gradient function
    # mx.value_and_grad differentiates wrt first arg (params) by default
    train_step = mx.compile(mx.value_and_grad(loss_fn))
    
    # Initial params (clean)
    params = model.parameters()
    
    # 5. Training Loop
    print("\nStarting Hybrid Training...")
    print(f"Config: {EPOCHS} epochs, {END_YEAR - START_YEAR + 1} years (1996-2025), ~11.5k tickers")
    start_time = time.time()
    
    num_years = END_YEAR - START_YEAR + 1
    
    for epoch in range(EPOCHS):
        e_start = time.time()
        total_loss = 0
        total_steps = 0
        
        prev_year_df = None
        
        for yi, year in enumerate(range(START_YEAR, END_YEAR + 1)):
            y_start = time.time()
            df_year = loader.load_year(year, prev_year_data=prev_year_df)
            
            # Update prev_year_df for NEXT iteration
            if df_year is not None and df_year.height > 0:
                 prev_year_df = df_year 
            else:
                 prev_year_df = None

            if df_year is None or df_year.height == 0:
                continue

            # Stream Batches
            chunk_loss = 0
            chunk_steps = 0
            
            for X_p, X_f, y_b in loader.get_batches(df_year):
                # Convert to MLX
                X_p = mx.array(X_p) 
                X_f = mx.array(X_f) 
                y_b = mx.array(y_b) 
                
                # Step (Functional) - Pass params
                loss, grads = train_step(params, X_p, X_f, y_b)
                
                # Update params directly (not model)
                optimizer.update(params, grads)
                
                # Eval params
                mx.eval(params, optimizer.state)
                
                chunk_loss += loss.item()
                chunk_steps += 1
                
                if chunk_steps % 50 == 0:
                    print(f"      Batch {chunk_steps} | Loss: {chunk_loss/chunk_steps:.4f}", flush=True)
                
            if chunk_steps > 0:
                avg_chunk_loss = chunk_loss / chunk_steps
                total_loss += chunk_loss
                total_steps += chunk_steps
                
                elapsed = time.time() - e_start
                years_done = yi + 1
                eta_epoch = (elapsed / years_done) * (num_years - years_done)
                print(f"  Epoch {epoch+1}/{EPOCHS} | Year {year} ({years_done}/{num_years}) | Loss: {avg_chunk_loss:.4f} | ETA epoch: {eta_epoch:.0f}s")
        
        avg_loss = total_loss / total_steps if total_steps > 0 else 0
        epoch_time = time.time() - e_start
        total_elapsed = time.time() - start_time
        eta_total = (total_elapsed / (epoch + 1)) * (EPOCHS - epoch - 1)
        print(f"Epoch {epoch+1} Done | Avg Loss: {avg_loss:.6f} | Time: {epoch_time:.1f}s | Total ETA: {eta_total/60:.1f}min")
    
    # Restore model for saving
    model.update(params)
        
    print(f"\nTotal Training Time: {time.time() - start_time:.2f}s")
    model.save_weights(os.path.join(MODELS_DIR, "hybrid_model.npz"))
    print("Model saved.")

if __name__ == "__main__":
    main()
