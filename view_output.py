# view_output.py

import pandas as pd
import glob
import pyarrow.parquet as pq

def read_parquet_files(folder_path):
    # Find all Parquet files in the folder
    parquet_files = glob.glob(f"{folder_path}/*.parquet")

    if not parquet_files:
        print("No parquet files found.")
        return

    print(f"Found {len(parquet_files)} parquet files.\n")

    for file in parquet_files:
        print(f"📄 Reading file: {file}")
        table = pq.read_table(file)
        df = table.to_pandas()
        print(df.head())  # Show first few rows
        print("-" * 60)

if __name__ == "__main__":
    folder_path = "./output/processed_data"  # Path where your Spark job wrote data
    read_parquet_files(folder_path)