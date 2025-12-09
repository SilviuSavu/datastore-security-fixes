import json

def extract_keys(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    financials = data.get('Financials', {})
    statements = ['Balance_Sheet', 'Income_Statement', 'Cash_Flow']
    
    all_keys = set()
    
    print(f"--- Processing {file_path} ---")
    for statement in statements:
        if statement in financials:
            print(f"Found {statement}")
            stmt_data = financials[statement]
            period_data = stmt_data.get('quarterly', {})
            if period_data:
                first_key = next(iter(period_data))
                keys = period_data[first_key].keys()
                print(f"  Keys in {statement} ({len(keys)}): {list(keys)}")
                all_keys.update(keys)
            else:
                print(f"  No quarterly data for {statement}")
                
    print(f"\nTotal unique keys found: {len(all_keys)}")
    sorted_keys = sorted(list(all_keys))
    print(f"All Unique Keys:\n{json.dumps(sorted_keys, indent=2)}")

extract_keys('/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore/bronze/fundamentals/eodhd/json/ALL.US.json')
