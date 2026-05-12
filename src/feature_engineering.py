import pandas as pd
import os

# ============================================================
# WEEK 6 — FEATURE ENGINEERING
# ============================================================

INPUT_FILE = "data/processed/seoul_cleaned_final.csv"
OUTPUT_FILE = "data/processed/modeling_dataset_v1.csv"

# Load final cleaned dataset
df = pd.read_csv(INPUT_FILE)
df["collected_at"] = pd.to_datetime(df["collected_at"])

print("=== BEFORE FEATURE ENGINEERING ===")
print("Rows:", len(df))
print("Columns:", df.columns.tolist())

# ------------------------------------------------------------
# Check required columns
# ------------------------------------------------------------
required_cols = [
    "collected_at",
    "location_name",
    "congestion_status_raw",
    "congestion_level_3class",
    "hour",
    "day_of_week",
    "month",
    "is_weekend",
]

missing_cols = [c for c in required_cols if c not in df.columns]

if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

# ------------------------------------------------------------
# Numeric congestion label
# Low = 0, Medium = 1, High = 2
# ------------------------------------------------------------
label_num_map = {
    "Low": 0,
    "Medium": 1,
    "High": 2,
}

df["label_num"] = df["congestion_level_3class"].map(label_num_map)

# Check if mapping worked
if df["label_num"].isna().any():
    print("Rows with unmapped labels:")
    print(df[df["label_num"].isna()][["congestion_level_3class"]].drop_duplicates())
    raise ValueError("Some congestion labels could not be mapped to numeric values.")

# ------------------------------------------------------------
# Peak hour feature
# Based on Week 5 findings: 8 AM and 14–19
# ------------------------------------------------------------
df["is_peak_hour"] = df["hour"].isin([8, 14, 15, 16, 17, 18, 19]).astype(int)

# ------------------------------------------------------------
# Sort before lag/rolling features
# ------------------------------------------------------------
df = df.sort_values(["location_name", "collected_at"]).copy()

# ------------------------------------------------------------
# Lag features by location
# ------------------------------------------------------------
df["lag_1"] = df.groupby("location_name")["label_num"].shift(1)
df["lag_2"] = df.groupby("location_name")["label_num"].shift(2)
df["lag_3"] = df.groupby("location_name")["label_num"].shift(3)

# ------------------------------------------------------------
# Rolling features by location
# shift(1) prevents data leakage
# ------------------------------------------------------------
df["rolling_mean_3"] = (
    df.groupby("location_name")["label_num"]
      .transform(lambda s: s.shift(1).rolling(3).mean())
)

df["rolling_std_3"] = (
    df.groupby("location_name")["label_num"]
      .transform(lambda s: s.shift(1).rolling(3).std())
)

# ------------------------------------------------------------
# Target: next observation in same location
# This is treated as 1-step-ahead / approximate 1-hour-ahead target
# ------------------------------------------------------------
df["target_1h"] = df.groupby("location_name")["congestion_level_3class"].shift(-1)
df["target_1h_num"] = df.groupby("location_name")["label_num"].shift(-1)

# ------------------------------------------------------------
# Drop rows that cannot be used for modeling
# Beginning rows have missing lags.
# Ending rows have missing target.
# ------------------------------------------------------------
model_df = df.dropna(subset=[
    "lag_1",
    "lag_2",
    "lag_3",
    "rolling_mean_3",
    "rolling_std_3",
    "target_1h",
]).copy()

# Optional: keep only useful columns
keep_cols = [
    "collected_at",
    "location_name",
    "congestion_status_raw",
    "congestion_level_3class",
    "label_num",
    "hour",
    "day_of_week",
    "month",
    "is_weekend",
    "is_peak_hour",
    "lag_1",
    "lag_2",
    "lag_3",
    "rolling_mean_3",
    "rolling_std_3",
    "target_1h",
    "target_1h_num",
]

model_df = model_df[keep_cols]

# Save output
os.makedirs("data/processed", exist_ok=True)
model_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print("\n=== AFTER FEATURE ENGINEERING ===")
print("Original rows:", len(df))
print("Model rows:", len(model_df))
print("Rows removed:", len(df) - len(model_df))

print("\nColumns:")
print(model_df.columns.tolist())

print("\nTarget distribution:")
print(model_df["target_1h"].value_counts())

print("\nMissing values in modeling columns:")
print(model_df.isna().sum())

print(f"\nSaved: {OUTPUT_FILE}")