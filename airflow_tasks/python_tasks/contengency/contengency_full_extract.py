import os
import json
import pandas as pd
from datetime import datetime

def main():
    base_dir = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl"
    raw_data_dir = os.path.join(base_dir, "raw_data")
    data_extract_dir = os.path.join(base_dir, "data_extract")

    print("🔄 Starting contingency extraction...")
    print(f"📁 Scanning all folders under: {raw_data_dir}")

    # Create timestamped folder (example: 2025-10-30 19:13:20 UTC)
    timestamp_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    output_dir = os.path.join(data_extract_dir, timestamp_str)
    os.makedirs(output_dir, exist_ok=True)

    # Define expected tables
    tables = ["bookings", "segments", "tickets", "passengers", "offers", "payments"]
    data_collections = {tbl: [] for tbl in tables}

    # Traverse all subfolders in raw_data
    for root, dirs, files in os.walk(raw_data_dir):
        for file in files:
            if file.endswith("sample_raw_export.json"):
                file_path = os.path.join(root, file)
                print(f"📥 Reading file: {file_path}")

                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)

                    # Loop through all expected tables
                    for tbl in tables:
                        if tbl in data and isinstance(data[tbl], list):
                            df = pd.DataFrame(data[tbl])
                            if not df.empty:
                                data_collections[tbl].append(df)
                        else:
                            print(f"⚠️ Table '{tbl}' missing or empty in {file_path}")

                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")

    # Combine and export to timestamped folder
    for tbl, df_list in data_collections.items():
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            output_path = os.path.join(output_dir, f"contengency_{tbl}.csv")
            combined_df.to_csv(output_path, index=False)
            print(f"✅ contengency_{tbl}.csv created with {len(combined_df)} rows")
        else:
            print(f"⚠️ No data found for table: {tbl}")

    print("\n📦 Contingency extraction complete!")
    print(f"📁 Output directory: {output_dir}")

if __name__ == "__main__":
    main()
