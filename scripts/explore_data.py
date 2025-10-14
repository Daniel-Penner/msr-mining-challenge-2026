import os
import pandas as pd
import duckdb

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")

def list_parquet_files():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    print("Available Parquet files:")
    for f in files:
        print(" -", f)
    return files

def preview_with_pandas(file_name):
    path = os.path.join(DATA_DIR, file_name)
    print(f"\n[Previewing {file_name}]")
    df = pd.read_parquet(path)
    print(df.info())
    print(df.head())

def quick_duckdb_query(file_name, limit=5):
    path = os.path.join(DATA_DIR, file_name)
    print(f"\n[DuckDB Query on {file_name}]")
    con = duckdb.connect()
    query = f"SELECT * FROM '{path}' LIMIT {limit};"
    print(con.execute(query).fetchdf())

if __name__ == "__main__":
    files = list_parquet_files()
    if not files:
        print("No parquet files found. Place them in data/raw/")
    else:
        preview_with_pandas(files[0])
        quick_duckdb_query(files[0])
