import pandas as pd
import os

# Load merged data
df = pd.read_csv("data/processed/seoul_citydata_merged.csv")

print("=== BEFORE CLEANING ===")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Standardize column names
df.columns = [c.strip().lower() for c in df.columns]

# Convert timestamps
df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce")

# Standardize location names (Korean → English)
location_map = {
    "광화문·덕수궁": "Gwanghwamun-Deoksugung",
    "사당역": "Sadang Station",
    "서울대입구역": "Seoul Natl Univ Station",
    "노들섬": "Nodeul Island",
    "어린이대공원": "Children's Grand Park"
}
df["location_name"] = df["location_name"].map(location_map).fillna(df["location_name"])

# Map congestion labels to 3 classes
label_map = {
    "여유": "Low",
    "보통": "Medium",
    "약간 붐빔": "High",
    "붐빔": "High"
}
df["congestion_level_3class"] = df["congestion_status_raw"].map(label_map)

# Remove rows where API returned an error in raw_response
df = df[~df["raw_response"].astype(str).str.startswith("ERROR")]

# Remove rows missing critical fields
rows_before = len(df)
df = df.dropna(subset=["collected_at", "location_name", "congestion_status_raw"])
rows_after_null = len(df)

# Remove exact duplicates
df = df.drop_duplicates(subset=["collected_at", "location_name"])
rows_after_dedup = len(df)

# Add time features
df["date"] = df["collected_at"].dt.date
df["hour"] = df["collected_at"].dt.hour
df["day_of_week"] = df["collected_at"].dt.day_name()
df["weekend_flag"] = df["collected_at"].dt.weekday >= 5

# Sort by time
df = df.sort_values("collected_at").reset_index(drop=True)

print("\n=== AFTER CLEANING ===")
print(f"Shape: {df.shape}")
print(f"Null rows removed: {rows_before - rows_after_null}")
print(f"Duplicate rows removed: {rows_after_null - rows_after_dedup}")
print(f"\nClass distribution:")
print(df["congestion_level_3class"].value_counts(dropna=False))
print(f"\nLocation counts:")
print(df["location_name"].value_counts())

# Save cleaned file
os.makedirs("data/processed", exist_ok=True)
df.to_csv("data/processed/seoul_congestion_cleaned_v1.csv", index=False, encoding="utf-8-sig")
print("\nSaved: data/processed/seoul_congestion_cleaned_v1.csv")

# Save cleaning summary
os.makedirs("reports/notes", exist_ok=True)
summary = f"""# Week 5 Cleaning Summary

| Item | Count |
|------|-------|
| Total rows before cleaning | {rows_before} |
| Null rows removed | {rows_before - rows_after_null} |
| Duplicate rows removed | {rows_after_null - rows_after_dedup} |
| Final rows | {rows_after_dedup} |
| Locations | {df['location_name'].nunique()} |
| Date range | {df['collected_at'].min()} → {df['collected_at'].max()} |
"""
with open("reports/notes/week5_cleaning_summary.md", "w", encoding="utf-8") as f:
    f.write(summary)
print("Saved: reports/notes/week5_cleaning_summary.md")