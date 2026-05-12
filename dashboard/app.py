import os
import joblib
import pandas as pd
import streamlit as st

# ============================================================
# STREAMLIT DASHBOARD — WEEK 9
# ============================================================

st.set_page_config(
    page_title="Seoul Congestion Dashboard",
    layout="wide"
)

DATA_PATH = "data/processed/modeling_dataset_v1.csv"
MODEL_PATH = "models/final_model.pkl"
RESULTS_PATH = "reports/modeling/week8_final_model_results.csv"
CONFUSION_MATRIX_PATH = "reports/figures/week8_random_forest_tuned_max_depth_10_confusion_matrix.png"

FEATURE_COLS = [
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

LABEL_NUM_MAP = {
    "Low": 0,
    "Medium": 1,
    "High": 2,
}

LABEL_DISPLAY_MAP = {
    0: "Low",
    1: "Medium",
    2: "High",
}


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_PATH)
    df["collected_at"] = pd.to_datetime(df["collected_at"])
    return df


@st.cache_resource
def load_model():
    return joblib.load(MODEL_PATH)


df = load_data()
model = load_model()

# ============================================================
# HEADER
# ============================================================

st.title("Seoul Urban Congestion Prediction Dashboard")

st.write(
    "This dashboard presents a machine-learning demo for predicting "
    "next-observation / approximate next-hour congestion levels for selected Seoul locations."
)

st.info(
    "Final model: Random Forest tuned with max_depth=10. "
    "Target: Low / Medium / High congestion."
)

# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Dashboard Controls")

locations = sorted(df["location_name"].dropna().unique())
selected_location = st.sidebar.selectbox("Select location", locations)

location_df = df[df["location_name"] == selected_location].copy()
location_df = location_df.sort_values("collected_at")

recent_n = st.sidebar.slider(
    "Number of recent records to show",
    min_value=5,
    max_value=50,
    value=15,
    step=5
)

# ============================================================
# KEY METRICS
# ============================================================

st.subheader("Selected Location Overview")

latest_row = location_df.tail(1)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Selected Location", selected_location)

with col2:
    st.metric("Records for Location", len(location_df))

with col3:
    if not latest_row.empty:
        st.metric("Latest Observed Class", latest_row["congestion_level_3class"].iloc[0])
    else:
        st.metric("Latest Observed Class", "No data")

# ============================================================
# PREDICTION
# ============================================================

st.subheader("Predicted Congestion")

if latest_row.empty:
    st.warning("No data available for this location.")
else:
    X_latest = latest_row[FEATURE_COLS]
    prediction = model.predict(X_latest)[0]

    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X_latest)[0]
        class_names = model.classes_
        proba_df = pd.DataFrame({
            "Class": class_names,
            "Probability": proba
        }).sort_values("Probability", ascending=False)
    else:
        proba_df = None

    if prediction == "Low":
        st.success(f"Predicted next congestion level: {prediction}")
    elif prediction == "Medium":
        st.warning(f"Predicted next congestion level: {prediction}")
    else:
        st.error(f"Predicted next congestion level: {prediction}")

    if proba_df is not None:
        st.write("Prediction probabilities:")
        st.dataframe(proba_df, use_container_width=True)

# ============================================================
# RECENT DATA TABLE
# ============================================================

st.subheader("Recent Historical Data")

display_cols = [
    "collected_at",
    "location_name",
    "congestion_level_3class",
    "target_1h",
    "hour",
    "day_of_week",
    "is_peak_hour",
    "lag_1",
    "lag_2",
    "lag_3",
    "rolling_mean_3",
]

st.dataframe(
    location_df[display_cols].tail(recent_n),
    use_container_width=True
)

# ============================================================
# RECENT TREND CHART
# ============================================================

st.subheader("Recent Congestion Trend")

trend_df = location_df.tail(recent_n).copy()
trend_df["congestion_num"] = trend_df["congestion_level_3class"].map(LABEL_NUM_MAP)
trend_df = trend_df.set_index("collected_at")

st.line_chart(trend_df["congestion_num"])

st.caption("Chart scale: Low = 0, Medium = 1, High = 2")

# ============================================================
# CLASS DISTRIBUTION
# ============================================================

st.subheader("Overall Target Class Distribution")

class_counts = df["target_1h"].value_counts().reset_index()
class_counts.columns = ["Congestion Class", "Count"]

st.bar_chart(class_counts.set_index("Congestion Class"))

# ============================================================
# MODEL SUMMARY
# ============================================================

st.subheader("Model Performance Summary")

if os.path.exists(RESULTS_PATH):
    results_df = pd.read_csv(RESULTS_PATH)
    st.dataframe(results_df[["Model", "Accuracy", "Macro_F1", "Weighted_F1", "Notes"]], use_container_width=True)
else:
    st.warning("Week 8 results table not found.")

st.write(
    "The final model was selected using Macro F1-score because the dataset is imbalanced "
    "toward the Low congestion class. Macro F1 gives more balanced attention to Low, Medium, and High."
)

if os.path.exists(CONFUSION_MATRIX_PATH):
    st.image(CONFUSION_MATRIX_PATH, caption="Final Model Confusion Matrix")
else:
    st.warning("Final confusion matrix image not found.")

# ============================================================
# FOOTER
# ============================================================

st.caption(
    "Capstone demo dashboard. Prediction uses the latest available row for the selected location."
)