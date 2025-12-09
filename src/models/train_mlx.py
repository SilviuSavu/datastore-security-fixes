import os
import sys
import time
import gc
import numpy as np
import polars as pl
import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
from datetime import datetime
from deltalake import DeltaTable
import pyarrow.dataset as ds

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

# M4 Max Optimization
BATCH_SIZE = 4096 
EPOCHS = 5
LEARNING_RATE = 1e-5
START_YEAR = 2009
END_YEAR = 2025

class AlphaMLP(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.layers = [
            nn.Linear(input_dim, 512),
            nn.BatchNorm(512),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(512, 256),
            nn.BatchNorm(256),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(256, 128),
            nn.BatchNorm(128),
            nn.ReLU(),
            nn.Dropout(0.1),
            
            nn.Linear(128, 64),
            nn.ReLU(),
            
            nn.Linear(64, 1)
        ]

    def __call__(self, x):
        for layer in self.layers:
            x = layer(x)
        return x

def get_feature_stats(feats):
    print(f"MLX Device: {mx.default_device()}")
    print(f"Computing Global Stats ({START_YEAR}-{END_YEAR})...")
    
    n_total = np.zeros(len(feats), dtype=np.float64)
    sum_x = np.zeros(len(feats), dtype=np.float64)
    sum_sq_x = np.zeros(len(feats), dtype=np.float64)
    
    # Stream via Arrow for efficiency
    dt = DeltaTable(GOLD_PATH)
    dataset = dt.to_pyarrow_dataset()
    
    filter_expr = (ds.field("year") >= START_YEAR) & (ds.field("year") <= END_YEAR)
    scanner = dataset.scanner(columns=feats, batch_size=1_000_000, filter=filter_expr)
    
    for i, batch in enumerate(scanner.to_batches()):
        # Sample 10% for speed
        if i % 10 != 0:
            continue
            
        print(f"  > Processing Batch {i}...", end="\r", flush=True)
        x_chunk = pl.from_arrow(batch).to_numpy().astype(np.float64)
        x_chunk = np.nan_to_num(x_chunk, nan=0.0, posinf=0.0, neginf=0.0)
        
        mask = ~np.isnan(x_chunk)
        n_total += np.sum(mask, axis=0)
        sum_x += np.nansum(x_chunk, axis=0)
        sum_sq_x += np.nansum(x_chunk ** 2, axis=0)
        
        del x_chunk, mask, batch
        
    n_total = np.maximum(n_total, 1.0) 
    means = sum_x / n_total
    variance = (sum_sq_x / n_total) - (means ** 2)
    variance = np.maximum(variance, 0)
    stds = np.sqrt(variance) + 1e-6
    
    means = np.nan_to_num(means, nan=0.0)
    stds = np.nan_to_num(stds, nan=1.0)
    stds = np.maximum(stds, 1e-3)
    means = np.clip(means, -1e9, 1e9)
    stds = np.clip(stds, 1e-3, 1e9)
    
    print(f"\nStats computed. Mean N: {np.mean(n_total):.0f}")
    return means, stds

def load_year_chunk(year, feats, means, stds):
    cols = list(set(feats + ["ticker", "date", "log_return_1d"]))
    
    lf = pl.scan_delta(GOLD_PATH).filter(pl.col("year") == year).select(cols)
    
    lf = lf.sort(["ticker", "date"]).with_columns([
        pl.col("log_return_1d").shift(-1).over("ticker").clip(-5.0, 5.0).alias("target_1d")
    ]).filter(pl.col("target_1d").is_not_null() & pl.col("target_1d").is_finite())
    
    norm_exprs = [
        ((pl.col(f).fill_nan(0.0).fill_null(0.0) - means[i]) / stds[i]).cast(pl.Float32).clip(-5.0, 5.0).alias(f)
        for i, f in enumerate(feats)
    ]
    lf = lf.with_columns(norm_exprs)
    
    df = lf.select(feats + ["target_1d"]).cast({"target_1d": pl.Float32}).collect()
    
    if df.height == 0:
        return None, None
    
    X = df.select(feats).to_numpy()
    y = df.select("target_1d").to_numpy()
    
    return X, y

def loss_fn(model, X, y):
    return mx.mean((model(X) - y) ** 2)

def train_epoch(model, optimizer, feats, means, stds):
    total_loss = 0
    total_steps = 0
    
    for year in range(START_YEAR, END_YEAR + 1):
        X, y = load_year_chunk(year, feats, means, stds)
        if X is None:
            continue
            
        indices = np.arange(len(X))
        np.random.shuffle(indices)
        
        chunk_loss = 0
        chunk_steps = 0
        loss_and_grad_fn = nn.value_and_grad(model, loss_fn)
        
        for start in range(0, len(X), BATCH_SIZE):
            end = start + BATCH_SIZE
            batch_idx = indices[start:end]
            
            X_batch = mx.array(X[batch_idx])
            y_batch = mx.array(y[batch_idx])
            
            loss, grads = loss_and_grad_fn(model, X_batch, y_batch)
            optimizer.update(model, grads)
            mx.eval(model.parameters(), optimizer.state)
            
            chunk_loss += loss.item()
            chunk_steps += 1
        
        if chunk_steps > 0:
            print(f"  > Year {year}: Loss {chunk_loss/chunk_steps:.6f}", end="\r", flush=True)
        
        total_loss += chunk_loss
        total_steps += chunk_steps
        
        del X, y, indices
        gc.collect()
        
    return total_loss / total_steps if total_steps > 0 else 0

def evaluate(model, feats, means, stds):
    years = list(range(2023, 2026))
    all_preds = []
    all_targets = []
    
    for year in years:
        X, y = load_year_chunk(year, feats, means, stds)
        if X is None:
            continue
            
        for start in range(0, len(X), BATCH_SIZE):
            end = start + BATCH_SIZE
            X_batch = mx.array(X[start:end])
            y_pred = model(X_batch)
            all_preds.append(np.array(y_pred))
            
        all_targets.append(y)
        del X, y
        gc.collect()
        
    if not all_preds:
        return 0, 0
        
    preds = np.concatenate(all_preds).flatten()
    y_true = np.concatenate(all_targets).flatten()
    
    mse = np.mean((preds - y_true)**2)
    ic = np.corrcoef(preds, y_true)[0, 1]
    
    return mse, ic

def main():
    start_time = time.time()
    
    print("Detecting features from schema...")
    lf_schema = pl.scan_delta(GOLD_PATH).collect_schema()
    exclude = ["date", "ticker", "target_1d", "cik", "year", "adsh", "cik_right", "name", 
               "period", "filed", "form", "fy", "fp", "currency_symbol", "filing_date", 
               "date_right", "year_right", "filed_date", "id"]
    
    feats = [c for c in lf_schema.names() if c not in exclude and lf_schema[c].is_numeric()]
    print(f"Features: {len(feats)}")
    
    means, stds = get_feature_stats(feats)
    print("Global Stats Computed.")
    
    model = AlphaMLP(len(feats))
    mx.eval(model.parameters())
    optimizer = optim.AdamW(learning_rate=LEARNING_RATE)
    
    print("\nStarting Training...")
    
    for epoch in range(EPOCHS):
        e_start = time.time()
        
        train_loss = train_epoch(model, optimizer, feats, means, stds)
        val_mse, val_ic = evaluate(model, feats, means, stds)
        
        print(f"\nEpoch {epoch+1}/{EPOCHS} | Time: {time.time()-e_start:.1f}s | Train Loss: {train_loss:.6f} | Val IC: {val_ic:.4f}")
    
    print(f"\nTotal Time: {time.time() - start_time:.2f}s")
    model.save_weights(os.path.join(MODELS_DIR, "mlx_model.npz"))

if __name__ == "__main__":
    main()
