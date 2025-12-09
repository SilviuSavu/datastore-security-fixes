import json
import os
import glob
import sys

# Correct path found by 'find' command
search_path = 'bronze/fundamentals/eodhd/json/*.json'
files = glob.glob(search_path)

if not files:
    print(f"No JSON files found in {search_path}")
    sys.exit(1)

print(f"Found {len(files)} JSON files. Checking a sample...")

# Check a few files to be sure
sample_files = files[:5] 

for fpath in sample_files:
    print(f"\n--- Output for {os.path.basename(fpath)} ---")
    try:
        with open(fpath, 'r') as f:
            data = json.load(f)
        
        if 'General' in data:
            gen = data['General']
            print(f"Code: {gen.get('Code')}")
            print(f"Sector: {gen.get('Sector')}")
            print(f"Industry: {gen.get('Industry')}")
            print(f"GicSector: {gen.get('GicSector')}")
            print(f"GicGroup: {gen.get('GicGroup')}")
            print(f"GicIndustry: {gen.get('GicIndustry')}")
            print(f"GicSubIndustry: {gen.get('GicSubIndustry')}")
        else:
            print("No 'General' key found.")
            
    except Exception as e:
        print(f"Error reading {fpath}: {e}")
