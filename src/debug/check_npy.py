import numpy as np

def check(name, data):
    print(f"\n--- Checking {name} ---")
    print(f"Shape: {data.shape}")
    print(f"Dtype: {data.dtype}")
    
    nans = np.isnan(data).sum()
    infs = np.isinf(data).sum()
    
    print(f"NaNs: {nans}")
    print(f"Infs: {infs}")
    
    if nans == 0 and infs == 0:
        print(f"Min: {data.min()}")
        print(f"Max: {data.max()}")
        print(f"Mean: {data.mean()}")
        print(f"Std: {data.std()}")
    else:
        print("Data is Clean? NO")
        return False
    return True

if __name__ == "__main__":
    try:
        X = np.load("X_2000.npy")
        y = np.load("y_2000.npy")
        
        check("X", X)
        check("y", y)
        
    except Exception as e:
        print(f"Error: {e}")
