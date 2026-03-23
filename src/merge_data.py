import glob
import os
import pandas as pd

# ============================================================
#  MERGE ALL RAW CSV FILES
# ============================================================
files = glob.glob("data/raw/*.csv")

if not files:
    print("No CSV files found in data/raw/")
    exit()

print(f"Found {len(files)} file(s):")
for f in files:
    print(f"  {f}")

dfs = [pd.read_csv(f, encoding="utf-8-sig") for f in files]
merged = pd.concat(dfs, ignore_index=True)

# ============================================================
#  BASIC INSPECTION
# ============================================================
print(f"\nTotal rows   : {len(merged)}")
print(f"Files merged : {len(files)}")
print(f"Columns      : {list(merged.columns)}")
print(f"\nLocations found:\n{merged['location_name'].value_counts()}")
print(f"\nFailed requests:\n{merged[merged['status_code'] != 200][['location_name','collected_at','raw_response']]}")
print(f"\nCongestion levels seen:\n{merged['congestion_status_raw'].value_counts()}")

# ============================================================
#  SAVE MERGED FILE
# ============================================================
os.makedirs("data/processed", exist_ok=True)
out_path = "data/processed/seoul_citydata_merged.csv"
merged.to_csv(out_path, index=False, encoding="utf-8-sig")

print(f"\nMerged file saved: {out_path}")
