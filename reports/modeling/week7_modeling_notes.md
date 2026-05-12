# Week 7 Baseline Modeling Notes

Baseline classification models were trained using temporal and lag-based features.

The target variable was `target_1h`, representing the next observed congestion class within the same location.

Because the dataset is imbalanced toward Low congestion, class-weighted models were used for Logistic Regression, Random Forest, and Decision Tree.

Models were evaluated using:
- Accuracy
- Macro F1
- Weighted F1
- Confusion matrices

The best model based on Macro F1 was:

Model          Random Forest
Accuracy            0.870246
Macro_F1             0.66446
Weighted_F1         0.875832

Macro F1 was prioritized because accuracy can be misleading when the Low class dominates the dataset.
