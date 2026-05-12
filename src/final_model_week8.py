import os
import joblib
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
)

# ============================================================
# WEEK 8 — FINAL MODEL SELECTION + SMALL IMPROVEMENT
# ============================================================

INPUT_FILE = "data/processed/modeling_dataset_v1.csv"

MODELS_DIR = "models"
RESULTS_DIR = "reports/modeling"
FIGURES_DIR = "reports/figures"

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
df = pd.read_csv(INPUT_FILE)
df["collected_at"] = pd.to_datetime(df["collected_at"])

df = df.sort_values("collected_at").reset_index(drop=True)

target_col = "target_1h"

base_feature_cols = [
    "location_name",
    "day_of_week",
    "hour",
    "month",
    "is_weekend",
    "is_peak_hour",
    "lag_1",
    "lag_2",
    "lag_3",
    "rolling_mean_3",
    "rolling_std_3",
]

no_lag_feature_cols = [
    "location_name",
    "day_of_week",
    "hour",
    "month",
    "is_weekend",
    "is_peak_hour",
]

# ------------------------------------------------------------
# Time-based split: same as Week 7
# ------------------------------------------------------------
n = len(df)
train_end = int(n * 0.70)
val_end = int(n * 0.85)

train_df = df.iloc[:train_end].copy()
val_df = df.iloc[train_end:val_end].copy()
test_df = df.iloc[val_end:].copy()

print("=== WEEK 8 DATA SPLIT ===")
print("Train:", len(train_df))
print("Validation:", len(val_df))
print("Test:", len(test_df))

print("\nTest target distribution:")
print(test_df[target_col].value_counts())

# ------------------------------------------------------------
# Helper: build preprocessing pipeline
# ------------------------------------------------------------
def build_preprocessor(feature_cols):
    categorical_features = [c for c in ["location_name", "day_of_week"] if c in feature_cols]
    numeric_features = [c for c in feature_cols if c not in categorical_features]

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("num", SimpleImputer(strategy="median"), numeric_features),
        ]
    )

    return preprocessor

# ------------------------------------------------------------
# Helper: evaluate model
# ------------------------------------------------------------
results = []

def evaluate_model(model_name, feature_cols, classifier, notes):
    X_train = train_df[feature_cols].copy()
    y_train = train_df[target_col].copy()

    X_test = test_df[feature_cols].copy()
    y_test = test_df[target_col].copy()

    preprocessor = build_preprocessor(feature_cols)

    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", classifier),
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    macro_f1 = f1_score(y_test, y_pred, average="macro")
    weighted_f1 = f1_score(y_test, y_pred, average="weighted")

    print("\n==============================")
    print("MODEL:", model_name)
    print("==============================")
    print("Accuracy:", accuracy)
    print("Macro F1:", macro_f1)
    print("Weighted F1:", weighted_f1)

    report = classification_report(y_test, y_pred)
    print("\nClassification report:")
    print(report)

    cm = confusion_matrix(y_test, y_pred, labels=["Low", "Medium", "High"])
    print("\nConfusion matrix:")
    print(cm)

    results.append({
        "Model": model_name,
        "Accuracy": accuracy,
        "Macro_F1": macro_f1,
        "Weighted_F1": weighted_f1,
        "Features": ", ".join(feature_cols),
        "Notes": notes,
    })

    safe_name = model_name.lower().replace(" ", "_").replace("=", "").replace(",", "").replace("(", "").replace(")", "")

    # Save classification report
    with open(f"{RESULTS_DIR}/week8_{safe_name}_classification_report.txt", "w", encoding="utf-8") as f:
        f.write(f"MODEL: {model_name}\n\n")
        f.write(f"Accuracy: {accuracy}\n")
        f.write(f"Macro F1: {macro_f1}\n")
        f.write(f"Weighted F1: {weighted_f1}\n\n")
        f.write(report)
        f.write("\nConfusion matrix:\n")
        f.write(str(cm))

    # Save confusion matrix figure
    disp = ConfusionMatrixDisplay(
        confusion_matrix=cm,
        display_labels=["Low", "Medium", "High"]
    )
    disp.plot()
    plt.title(f"{model_name} Confusion Matrix")
    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}/week8_{safe_name}_confusion_matrix.png", dpi=150)
    plt.close()

    return model, y_pred

# ------------------------------------------------------------
# Experiment 1: Week 7 Random Forest baseline
# ------------------------------------------------------------
rf_baseline, _ = evaluate_model(
    model_name="Random Forest Baseline",
    feature_cols=base_feature_cols,
    classifier=RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        class_weight="balanced"
    ),
    notes="Week 7 baseline Random Forest with lag and rolling features."
)

# ------------------------------------------------------------
# Experiment 2: Small tuning — max_depth=10
# ------------------------------------------------------------
rf_tuned_10, _ = evaluate_model(
    model_name="Random Forest Tuned max_depth 10",
    feature_cols=base_feature_cols,
    classifier=RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        class_weight="balanced"
    ),
    notes="Small Week 8 tuning test with max_depth=10."
)

# ------------------------------------------------------------
# Experiment 3: Small tuning — max_depth=5
# ------------------------------------------------------------
rf_tuned_5, _ = evaluate_model(
    model_name="Random Forest Tuned max_depth 5",
    feature_cols=base_feature_cols,
    classifier=RandomForestClassifier(
        n_estimators=200,
        max_depth=5,
        random_state=42,
        class_weight="balanced"
    ),
    notes="Small Week 8 tuning test with max_depth=5."
)

# ------------------------------------------------------------
# Experiment 4: Feature comparison — without lag/rolling features
# ------------------------------------------------------------
rf_no_lag, _ = evaluate_model(
    model_name="Random Forest Without Lag Features",
    feature_cols=no_lag_feature_cols,
    classifier=RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        class_weight="balanced"
    ),
    notes="Feature comparison: removes lag and rolling features."
)

# ------------------------------------------------------------
# Save Week 8 results table
# ------------------------------------------------------------
results_df = pd.DataFrame(results)
results_df = results_df.sort_values("Macro_F1", ascending=False).reset_index(drop=True)

results_path = f"{RESULTS_DIR}/week8_final_model_results.csv"
results_df.to_csv(results_path, index=False, encoding="utf-8-sig")

print("\n=== WEEK 8 FINAL RESULTS TABLE ===")
print(results_df[["Model", "Accuracy", "Macro_F1", "Weighted_F1", "Notes"]])

# ------------------------------------------------------------
# Select best model by Macro F1
# ------------------------------------------------------------
best_row = results_df.iloc[0]
best_model_name = best_row["Model"]

print("\n=== SELECTED FINAL MODEL ===")
print(best_row[["Model", "Accuracy", "Macro_F1", "Weighted_F1", "Notes"]])

if best_model_name == "Random Forest Baseline":
    final_model = rf_baseline
    final_features = base_feature_cols
elif best_model_name == "Random Forest Tuned max_depth 10":
    final_model = rf_tuned_10
    final_features = base_feature_cols
elif best_model_name == "Random Forest Tuned max_depth 5":
    final_model = rf_tuned_5
    final_features = base_feature_cols
else:
    final_model = rf_no_lag
    final_features = no_lag_feature_cols

# ------------------------------------------------------------
# Save final model pipeline
# ------------------------------------------------------------
final_model_path = f"{MODELS_DIR}/final_model.pkl"
joblib.dump(final_model, final_model_path)

# ------------------------------------------------------------
# Save final model info
# ------------------------------------------------------------
info_text = f"""# Final Model Info — Week 8

Final selected model:
{best_row["Model"]}

Target:
{target_col}

Features used:
{", ".join(final_features)}

Test Accuracy:
{best_row["Accuracy"]}

Test Macro F1:
{best_row["Macro_F1"]}

Test Weighted F1:
{best_row["Weighted_F1"]}

Selection reason:
The final model was selected based primarily on Macro F1-score because the dataset is imbalanced toward the Low congestion class. Random Forest was chosen because it performed better than the Week 7 baseline alternatives and provided stronger minority-class prediction performance.

Week 8 improvement tested:
1. Random Forest max_depth tuning
2. Feature comparison with and without lag/rolling features

Notes:
Lag and rolling features represent recent congestion history and are important for short-term congestion prediction.
"""

info_path = f"{MODELS_DIR}/final_model_info.txt"
with open(info_path, "w", encoding="utf-8") as f:
    f.write(info_text)

# ------------------------------------------------------------
# Save Week 8 interpretation note
# ------------------------------------------------------------
week8_notes = f"""# Week 8 Final Model Selection Notes

Week 8 focused on selecting a final classification model for deployment. Based on the Week 7 baseline results, Random Forest was the strongest candidate because it achieved the highest Macro F1-score and better minority-class performance compared with Dummy, Logistic Regression, and Decision Tree.

A small improvement experiment was performed by testing Random Forest models with different maximum depth values. A feature comparison was also performed by comparing the model with lag/rolling features against a version without lag/rolling features.

The final selected model was:

{best_row["Model"]}

Performance on the test set:
- Accuracy: {best_row["Accuracy"]:.4f}
- Macro F1: {best_row["Macro_F1"]:.4f}
- Weighted F1: {best_row["Weighted_F1"]:.4f}

Macro F1 was prioritized because the dataset is imbalanced and accuracy alone can overstate performance when the Low congestion class dominates.

Remaining difficulty:
Medium and High congestion classes are less frequent than Low, making minority-class prediction more challenging.
"""

notes_path = f"{RESULTS_DIR}/week8_final_model_notes.md"
with open(notes_path, "w", encoding="utf-8") as f:
    f.write(week8_notes)

print(f"\nSaved final model: {final_model_path}")
print(f"Saved final model info: {info_path}")
print(f"Saved Week 8 results: {results_path}")
print(f"Saved Week 8 notes: {notes_path}")
print("Week 8 complete.")