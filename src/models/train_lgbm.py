import os
import sys
import gc
import polars as pl
import pandas as pd
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
import numpy as np
from datetime import datetime

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")
MODELS_DIR = os.path.join(BASE_DIR, "models")
PLOTS_DIR = os.path.join(MODELS_DIR, "plots")

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)

def load_and_prep_data():
    print("Loading Data from Gold Layer...")
    
    lf = pl.scan_delta(GOLD_PATH)
    lf = lf.sort(["ticker", "date"])
    
    lf = lf.with_columns([
        pl.col("log_return_1d").shift(-1).over("ticker").alias("target_1d")
    ])
    
    lf = lf.filter(pl.col("target_1d").is_not_null())
    lf = lf.filter(pl.col("target_1d").is_finite())
    
    return lf

def train_model(df_train, df_test, features, target):
    print(f"Training LightGBM on {len(df_train)} rows...")
    print(f"Features: {len(features)}")
    
    train_data = lgb.Dataset(
        df_train[features], 
        label=df_train[target],
        free_raw_data=False
    )
    
    test_data = lgb.Dataset(
        df_test[features], 
        label=df_test[target],
        reference=train_data,
        free_raw_data=False
    )
    
    params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "n_jobs": -1,
        "seed": 42
    }
    
    validation_sets = [train_data, test_data]
    callbacks = [
        lgb.early_stopping(stopping_rounds=50),
        lgb.log_evaluation(period=100)
    ]
    
    bst = lgb.train(
        params,
        train_data,
        num_boost_round=1000,
        valid_sets=validation_sets,
        valid_names=["train", "test"],
        callbacks=callbacks
    )
    
    return bst

def evaluate_model(bst, df_test, features, target):
    print("\nEvaluating Model...")
    
    preds = bst.predict(df_test[features])
    
    df_eval = pd.DataFrame({
        "target": df_test[target],
        "prediction": preds
    })
    
    ic = df_eval.corr().iloc[0, 1]
    r2 = r2_score(df_eval["target"], df_eval["prediction"])
    
    print(f"Information Coefficient (IC): {ic:.4f}")
    print(f"R2 Score: {r2:.4f}")
    
    return ic, r2

def plot_importance(bst):
    print("Plotting Feature Importance...")
    plt.figure(figsize=(10, 8))
    lgb.plot_importance(bst, max_num_features=20, importance_type='gain', figsize=(10, 8))
    plt.title("LightGBM Feature Importance (Gain)")
    plt.tight_layout()
    
    plot_path = os.path.join(PLOTS_DIR, "lgbm_importance.png")
    plt.savefig(plot_path)
    print(f"Saved plot to {plot_path}")

def main():
    start_time = datetime.now()
    
    lf = load_and_prep_data()
    
    exclude_cols = [
        "date", "ticker", "target_1d", "cik", "year", "adsh", "cik_right", "name", 
        "period", "filed", "form", "fy", "fp", "currency_symbol", "filing_date", 
        "date_right", "year_right", "filed_date", "id"
    ]
    
    schema = lf.collect_schema()
    all_cols = schema.names()
    
    feature_cols = [
        c for c in all_cols 
        if c not in exclude_cols 
        and schema[c].is_numeric()
    ]
    
    print(f"Selected {len(feature_cols)} features.")
    
    print("Splitting Data (Train < 2023, Test >= 2023)...")
    
    print("Collecting Train Set...")
    df_train = lf.filter(pl.col("year") < 2023).select(feature_cols + ["target_1d", "year"]).collect().to_pandas()
    
    print("Collecting Test Set...")
    df_test = lf.filter(pl.col("year") >= 2023).select(feature_cols + ["target_1d", "year"]).collect().to_pandas()
    
    print(f"Train Shape: {df_train.shape}")
    print(f"Test Shape: {df_test.shape}")
    
    bst = train_model(df_train, df_test, feature_cols, "target_1d")
    
    evaluate_model(bst, df_test, feature_cols, "target_1d")
    
    plot_importance(bst)
    
    model_path = os.path.join(MODELS_DIR, "lgbm_model.txt")
    bst.save_model(model_path)
    print(f"Model saved to {model_path}")
    
    print(f"Total Time: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
