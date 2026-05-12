import os
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier

from sklearn.metrics import (
    accuracy_score,
    f1_score,
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay
)

# ============================================================
# WEEK 7 — BASELINE MODELING
# ============================================================

INPUT_FILE = "data/processed/modeling_dataset_v1.csv"
RESULTS_DIR = "reports/modeling"
FIGURES_DIR = "reports/figures"

os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

# ------------------------------------------------------------
# Load modeling dataset
# ------------------------------------------------------------
df = pd.read_csv(INPUT_FILE)
df["collected_at"] = pd.to_datetime(df["collected_at"])

print("=== DATASET INFO ===")
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print("\nTarget distribution:")
print(df["target_1h"].value_counts())

# ------------------------------------------------------------
# Sort by time for time-based split
# ------------------------------------------------------------
df = df.sort_values("collected_at").reset_index(drop=True)

# ------------------------------------------------------------
# Features and target
# ------------------------------------------------------------
target_col = "target_1h"

feature_cols = [
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

X = df[feature_cols].copy()
y = df[target_col].copy()

# ------------------------------------------------------------
# Time-based split: 70% train, 15% validation, 15% test
# ------------------------------------------------------------
n = len(df)
train_end = int(n * 0.70)
val_end = int(n * 0.85)

X_train = X.iloc[:train_end]
y_train = y.iloc[:train_end]

X_val = X.iloc[train_end:val_end]
y_val = y.iloc[train_end:val_end]

X_test = X.iloc[val_end:]
y_test = y.iloc[val_end:]

print("\n=== SPLIT SIZES ===")
print("Train:", len(X_train))
print("Validation:", len(X_val))
print("Test:", len(X_test))

print("\nTest target distribution:")
print(y_test.value_counts())

# ------------------------------------------------------------
# Preprocessing pipeline
# ------------------------------------------------------------
categorical_features = ["location_name", "day_of_week"]

numeric_features = [
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

preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ("num", SimpleImputer(strategy="median"), numeric_features),
    ]
)

# ------------------------------------------------------------
# Helper function for model evaluation
# ------------------------------------------------------------
results = []

def evaluate_model(model_name, model, X_train, y_train, X_test, y_test):
    print(f"\n==============================")
    print(f"MODEL: {model_name}")
    print(f"==============================")

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    macro_f1 = f1_score(y_test, y_pred, average="macro")
    weighted_f1 = f1_score(y_test, y_pred, average="weighted")

    print("Accuracy:", accuracy)
    print("Macro F1:", macro_f1)
    print("Weighted F1:", weighted_f1)

    print("\nClassification report:")
    report = classification_report(y_test, y_pred)
    print(report)

    print("\nConfusion matrix:")
    cm = confusion_matrix(y_test, y_pred, labels=["Low", "Medium", "High"])
    print(cm)

    results.append({
        "Model": model_name,
        "Accuracy": accuracy,
        "Macro_F1": macro_f1,
        "Weighted_F1": weighted_f1,
    })

    # Save classification report
    safe_name = model_name.lower().replace(" ", "_")
    report_path = f"{RESULTS_DIR}/{safe_name}_classification_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
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
    plt.savefig(f"{FIGURES_DIR}/week7_{safe_name}_confusion_matrix.png", dpi=150)
    plt.close()

    return model, y_pred

# ------------------------------------------------------------
# Model 1: Dummy baseline
# ------------------------------------------------------------
dummy_model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("classifier", DummyClassifier(strategy="most_frequent")),
])

dummy_model, dummy_pred = evaluate_model(
    "Dummy",
    dummy_model,
    X_train,
    y_train,
    X_test,
    y_test
)

# ------------------------------------------------------------
# Model 2: Logistic Regression
# ------------------------------------------------------------
log_model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("classifier", LogisticRegression(
        max_iter=1000,
        class_weight="balanced"
    )),
])

log_model, log_pred = evaluate_model(
    "Logistic Regression",
    log_model,
    X_train,
    y_train,
    X_test,
    y_test
)

# ------------------------------------------------------------
# Model 3: Random Forest
# ------------------------------------------------------------
rf_model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("classifier", RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        class_weight="balanced"
    )),
])

rf_model, rf_pred = evaluate_model(
    "Random Forest",
    rf_model,
    X_train,
    y_train,
    X_test,
    y_test
)

# ------------------------------------------------------------
# Optional Model 4: Decision Tree
# ------------------------------------------------------------
tree_model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("classifier", DecisionTreeClassifier(
        random_state=42,
        class_weight="balanced",
        max_depth=5
    )),
])

tree_model, tree_pred = evaluate_model(
    "Decision Tree",
    tree_model,
    X_train,
    y_train,
    X_test,
    y_test
)

# ------------------------------------------------------------
# Save results table
# ------------------------------------------------------------
results_df = pd.DataFrame(results)
results_path = f"{RESULTS_DIR}/week7_baseline_results.csv"
results_df.to_csv(results_path, index=False, encoding="utf-8-sig")

print("\n=== FINAL RESULTS TABLE ===")
print(results_df)

best_model = results_df.sort_values("Macro_F1", ascending=False).iloc[0]

print("\n=== BEST MODEL BY MACRO F1 ===")
print(best_model)

# Save short modeling notes
notes = f"""# Week 7 Baseline Modeling Notes

Baseline classification models were trained using temporal and lag-based features.

The target variable was `target_1h`, representing the next observed congestion class within the same location.

Because the dataset is imbalanced toward Low congestion, class-weighted models were used for Logistic Regression, Random Forest, and Decision Tree.

Models were evaluated using:
- Accuracy
- Macro F1
- Weighted F1
- Confusion matrices

The best model based on Macro F1 was:

{best_model.to_string()}

Macro F1 was prioritized because accuracy can be misleading when the Low class dominates the dataset.
"""

notes_path = f"{RESULTS_DIR}/week7_modeling_notes.md"
with open(notes_path, "w", encoding="utf-8") as f:
    f.write(notes)

print(f"\nSaved results table: {results_path}")
print(f"Saved modeling notes: {notes_path}")
print(f"Saved confusion matrices to: {FIGURES_DIR}")