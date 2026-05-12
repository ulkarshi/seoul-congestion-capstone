import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import os

matplotlib.rcParams["figure.figsize"] = (10, 5)

# Because this script runs from project root:
os.makedirs("reports/figures", exist_ok=True)
os.makedirs("reports/notes", exist_ok=True)

# Load final cleaned dataset
df = pd.read_csv("data/processed/seoul_cleaned_final.csv")
df["collected_at"] = pd.to_datetime(df["collected_at"])

print("=== BASIC INFO ===")
print(df.shape)
print(df.columns.tolist())
print(df.dtypes)

print("\n=== DATE / LOCATION / CLASS SUMMARY ===")
print("Date range:", df["collected_at"].min(), "→", df["collected_at"].max())
print("Locations:", df["location_name"].nunique())
print("\nClass counts:")
print(df["congestion_level_3class"].value_counts(dropna=False))

print("\n=== MISSING VALUES ===")
print(df.isna().sum().sort_values(ascending=False))

# ------------------------------------------------------------
# Figure 1: Class distribution
# ------------------------------------------------------------
fig, ax = plt.subplots()
df["congestion_level_3class"].value_counts().plot(
    kind="bar",
    ax=ax,
    color=["green", "orange", "red"]
)
ax.set_title("Overall Congestion Class Distribution")
ax.set_xlabel("Class")
ax.set_ylabel("Count")
plt.tight_layout()
plt.savefig("reports/figures/fig1_class_distribution.png", dpi=150)
plt.close()

# ------------------------------------------------------------
# Figure 2: Class by location
# ------------------------------------------------------------
fig, ax = plt.subplots(figsize=(12, 5))
pd.crosstab(df["location_name"], df["congestion_level_3class"]).plot(
    kind="bar",
    ax=ax,
    color=["green", "red", "orange"]
)
ax.set_title("Congestion Class by Location")
ax.set_xlabel("Location")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("reports/figures/fig2_by_location.png", dpi=150)
plt.close()

# ------------------------------------------------------------
# Figure 3: Congestion by hour
# ------------------------------------------------------------
fig, ax = plt.subplots(figsize=(12, 5))
hourly = pd.crosstab(df["hour"], df["congestion_level_3class"])
hourly.plot(
    kind="bar",
    stacked=True,
    ax=ax,
    color=["green", "orange", "red"]
)
ax.set_title("Congestion by Hour of Day")
ax.set_xlabel("Hour")
ax.set_ylabel("Count")
plt.tight_layout()
plt.savefig("reports/figures/fig3_by_hour.png", dpi=150)
plt.close()

# ------------------------------------------------------------
# Figure 4: Weekday vs Weekend
# Your old notebook used weekend_flag.
# Your new preprocess file uses is_weekend.
# ------------------------------------------------------------
fig, ax = plt.subplots()

pd.crosstab(df["is_weekend"], df["congestion_level_3class"]).plot(
    kind="bar",
    ax=ax,
    color=["green", "orange", "red"]
)

ax.set_xticklabels(["Weekday", "Weekend"], rotation=0)
ax.set_title("Congestion: Weekday vs Weekend")
plt.tight_layout()
plt.savefig("reports/figures/fig4_weekend_vs_weekday.png", dpi=150)
plt.close()

# ------------------------------------------------------------
# Figure 5: Timeline for Gwanghwamun
# ------------------------------------------------------------
one = df[df["location_name"] == "Gwanghwamun-Deoksugung"].copy()
one = one.sort_values("collected_at")

level_map = {"Low": 0, "Medium": 1, "High": 2}
one["level_num"] = one["congestion_level_3class"].map(level_map)

fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(one["collected_at"], one["level_num"], marker="o", linewidth=1)
ax.set_yticks([0, 1, 2])
ax.set_yticklabels(["Low", "Medium", "High"])
ax.set_title("Congestion Over Time — Gwanghwamun")
ax.set_xlabel("Time")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("reports/figures/fig5_timeline_gwanghwamun.png", dpi=150)
plt.close()

# ------------------------------------------------------------
# Findings text
# ------------------------------------------------------------
findings = """
## Week 5 EDA Findings

1. Low congestion is the dominant class in the dataset.
2. Gwanghwamun-Deoksugung shows slightly fewer records because some failed API rows were removed.
3. Hourly congestion patterns should be interpreted using the updated final cleaned dataset.
4. Weekday vs weekend patterns were regenerated using the final frozen dataset.
5. Data coverage is sufficient for baseline feature engineering and modeling.
6. The dataset has clear class imbalance, with Low much more common than Medium and High.
7. The timeline chart helps show changes in congestion over time for one location.
"""

print("\n=== FINDINGS ===")
print(findings)

with open("reports/notes/week5_eda_findings_final.md", "w", encoding="utf-8") as f:
    f.write(findings)

# ------------------------------------------------------------
# Final checks
# ------------------------------------------------------------
checks = {
    "Has valid timestamp": df["collected_at"].notna().all(),
    "Has location": df["location_name"].notna().all(),
    "Has congestion class": df["congestion_level_3class"].notna().sum() > len(df) * 0.8,
    "Has hour feature": "hour" in df.columns,
    "Has day_of_week": "day_of_week" in df.columns,
    "Enough rows (>100)": len(df) > 100,
    "All 5 locations present": df["location_name"].nunique() == 5,
}

print("\n=== FINAL CHECKS ===")
for check, result in checks.items():
    status = "✅" if result else "❌"
    print(f"{status} {check}")

print("\nWeek 5 EDA rerun complete.")
print("Figures saved to: reports/figures/")
print("Findings saved to: reports/notes/week5_eda_findings_final.md")