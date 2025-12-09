import polars as pl
try:
    schema = pl.scan_delta('/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore/gold/features').collect_schema()
    with open('schema_dump.txt', 'w') as f:
        for name in schema.names():
            f.write(f"{name}\n")
    print("Schema dumped to schema_dump.txt")
except Exception as e:
    print(f"Error: {e}")
