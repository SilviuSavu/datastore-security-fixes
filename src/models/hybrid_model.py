import mlx.core as mx
import mlx.nn as nn

class FinancialForecaster(nn.Module):
    def __init__(self, price_dim, fund_dim, hidden_dim, num_heads, num_layers, dropout=0.1):
        super().__init__()

        # --- Branch A: Price Encoder (Time Series) ---
        # Projects daily price features (Open, High, Low, Vol) into hidden space
        self.price_embedding = nn.Linear(price_dim, hidden_dim)
        
        # Transformer encoder to capture temporal dependencies
        # MLX TransformerEncoder expects standard args
        self.transformer = nn.TransformerEncoder(
            num_layers=num_layers,
            dims=hidden_dim,
            num_heads=num_heads,
            mlp_dims=hidden_dim * 4,
            dropout=dropout
        )

        # --- Branch B: Fundamental Encoder (Static/Low Freq) ---
        # Projects quarterly data (P/E, EPS, etc.)
        self.fundamental_encoder = nn.Sequential(
            nn.Linear(fund_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, hidden_dim // 4),
            nn.ReLU()
        )

        # --- Fusion Head ---
        # Combines the time-series context with fundamental context
        # Transformer Output (Mean Pooling) -> hidden_dim
        # Fundamental Output -> hidden_dim // 4
        fusion_input_dim = hidden_dim + (hidden_dim // 4)
        
        self.fusion_layer = nn.Sequential(
            nn.Linear(fusion_input_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout)
        )
        
        self.final_head = nn.Linear(hidden_dim, 1) # Predicting 1 value (e.g., Return)

    def __call__(self, price_history, fundamentals):
        # price_history shape: (Batch, Time_Steps, Price_Features)
        # fundamentals shape: (Batch, Fund_Features)

        # 1. Process Time Series
        # Embedding
        x_price = self.price_embedding(price_history) # (Batch, Time, Hidden)
        
        # Transformer
        # We can add positional encodings if needed, but for simple financial windows 
        # usually just the sequence order is implicit.
        # MLX Transformer expects (Batch, Time, Dim)
        x_price = self.transformer(x_price, mask=None) 
        
        # Global Average Pooling to get a single vector for the time series
        # Collapse Time Dimension
        x_price_context = mx.mean(x_price, axis=1) # (Batch, Hidden)

        # 2. Process Fundamentals
        x_fund_context = self.fundamental_encoder(fundamentals) # (Batch, Hidden//4)

        # 3. Fuse and Predict
        combined = mx.concatenate([x_price_context, x_fund_context], axis=-1)
        hidden = self.fusion_layer(combined)
        output = self.final_head(hidden)
        
        return output
