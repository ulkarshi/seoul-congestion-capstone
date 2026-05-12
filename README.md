# Seoul Urban Congestion Prediction

A machine learning capstone project for predicting short-term urban congestion levels in selected Seoul locations using Seoul city congestion data.

The project collects, cleans, analyzes, and models congestion data, then presents the final model through a simple Streamlit dashboard.

---

## Project Overview

This project predicts the next observed congestion level for selected Seoul locations.

The congestion classes are:

- Low
- Medium
- High

The final model is designed as a simple decision-support demo for understanding short-term congestion patterns by location.

---

## Selected Locations

The project uses data from five Seoul locations:

- Gwanghwamun-Deoksugung
- Sadang Station
- Seoul Natl Univ Station
- Nodeul Island
- Children's Grand Park

---

## Main Pipeline

1. **Data Collection**  
   Seoul city congestion data was collected from the Seoul Open API and saved in `data/raw/`.

2. **Dataset Freeze**  
   Data collection was stopped before modeling so the project could use one fixed dataset snapshot.

3. **Data Merging**  
   Raw CSV files were merged into one final dataset.

4. **Preprocessing**  
   Timestamps were converted, location names were standardized, congestion labels were mapped into three classes, failed API rows were removed, and time features were added.

5. **Feature Engineering**  
   Lag features, rolling features, peak-hour indicators, and the next-observation target were created.

6. **Baseline Modeling**  
   Dummy Classifier, Logistic Regression, Random Forest, and Decision Tree models were compared.

7. **Final Model Selection**  
   A tuned Random Forest model was selected as the final model.

8. **Dashboard**  
   A Streamlit dashboard was built to show recent data, predictions, historical trends, and model performance.

---

## Final Datasets

Important processed files:

```text
data/processed/seoul_merged_final.csv
data/processed/seoul_cleaned_final.csv
data/processed/modeling_dataset_v1.csv
```

A final snapshot is saved in:

```text
data/final_snapshot/
```

---

## Feature Engineering

The final modeling dataset includes:

- `location_name`
- `day_of_week`
- `hour`
- `month`
- `is_weekend`
- `is_peak_hour`
- `lag_1`
- `lag_2`
- `lag_3`
- `rolling_mean_3`
- `rolling_std_3`

Target column:

```text
target_1h
```

Note: `target_1h` represents the next available observation within the same location. It is used as an approximate short-term / next-hour congestion prediction target.

---

## Final Model

The selected final model is:

```text
Random Forest Classifier tuned with max_depth=10
```

The saved model file is:

```text
models/final_model.pkl
```

The final model was selected because it achieved the best Macro F1-score and showed stronger performance on minority congestion classes compared with the baseline models.

---

## Key Results

### Week 7 Baseline Results

| Model | Accuracy | Macro F1 | Weighted F1 |
|---|---:|---:|---:|
| Dummy | 0.8434 | 0.3050 | 0.7718 |
| Logistic Regression | 0.7740 | 0.5321 | 0.7969 |
| Random Forest | 0.8702 | 0.6645 | 0.8758 |
| Decision Tree | 0.6890 | 0.5204 | 0.7440 |

### Week 8 Final Model Results

| Model | Accuracy | Macro F1 | Weighted F1 |
|---|---:|---:|---:|
| Random Forest Baseline | 0.8702 | 0.6645 | 0.8758 |
| Random Forest Tuned max_depth=10 | 0.8725 | 0.6923 | 0.8840 |
| Random Forest Tuned max_depth=5 | 0.8009 | 0.6073 | 0.8302 |
| Random Forest Without Lag Features | 0.8725 | 0.6815 | 0.8787 |

Final selected model:

```text
Random Forest Tuned max_depth=10
```

Main evaluation metric:

```text
Macro F1-score
```

Macro F1 was prioritized because the dataset is imbalanced toward the Low congestion class.

---

## Dashboard

The dashboard was built with Streamlit.

Dashboard file:

```text
dashboard/app.py
```

The dashboard includes:

- Project overview
- Location selector
- Recent historical congestion data
- Next congestion prediction
- Prediction probabilities
- Recent congestion trend chart
- Overall class distribution
- Final model performance summary
- Confusion matrix

---

## How to Run the Dashboard

Install required packages:

```bash
pip install streamlit joblib scikit-learn pandas matplotlib
```

Run the dashboard:

```bash
streamlit run dashboard/app.py
```

If that does not work, run:

```bash
python -m streamlit run dashboard/app.py
```

The dashboard should open at:

```text
http://localhost:8501
```

---

## Folder Structure

```text
seoul-congestion-capstone/
├── .github/
│   └── workflows/
├── dashboard/
│   └── app.py
├── data/
│   ├── raw/
│   ├── processed/
│   │   ├── seoul_merged_final.csv
│   │   ├── seoul_cleaned_final.csv
│   │   └── modeling_dataset_v1.csv
│   ├── external/
│   └── final_snapshot/
├── models/
│   ├── final_model.pkl
│   └── final_model_info.txt
├── notebooks/
├── reports/
│   ├── figures/
│   ├── modeling/
│   ├── notes/
│   └── weekly_logs/
├── src/
│   ├── collect_data.py
│   ├── merge_data.py
│   ├── preprocess.py
│   ├── feature_engineering.py
│   ├── train_baselines.py
│   ├── final_model_week8.py
│   └── rerun_week5_eda.py
├── README.md
└── requirements.txt
```

---

## Main Scripts

| Script | Purpose |
|---|---|
| `src/collect_data.py` | Collects Seoul city congestion data |
| `src/merge_data.py` | Merges raw CSV files |
| `src/preprocess.py` | Cleans and prepares final dataset |
| `src/rerun_week5_eda.py` | Regenerates EDA figures |
| `src/feature_engineering.py` | Creates modeling features and target |
| `src/train_baselines.py` | Trains baseline models |
| `src/final_model_week8.py` | Selects and saves final model |
| `dashboard/app.py` | Runs Streamlit dashboard |

---

## Limitations

- The target variable is based on the next available observation, so it is an approximate short-term prediction rather than a perfectly fixed 60-minute forecast.
- The dataset is imbalanced toward Low congestion.
- Medium and High congestion classes have fewer samples.
- The dashboard is a demo using historical/latest dataset rows, not live API inference.
- External factors such as weather, holidays, events, and transit disruptions were not included.
- The model currently covers five selected Seoul locations.

---

## Future Work

Possible improvements include:

- Adding weather data
- Adding holiday and event indicators
- Expanding to more Seoul locations
- Improving real-time prediction support
- Testing additional models
- Adding live API integration to the dashboard
- Improving dashboard design and interactivity

---

## Project Status

Completed:

- Final dataset snapshot
- Data preprocessing
- Feature engineering
- Baseline modeling
- Final model selection
- Saved final model
- Streamlit dashboard demo
