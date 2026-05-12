# Week 8 Final Model Selection Notes

Week 8 focused on selecting a final classification model for deployment. Based on the Week 7 baseline results, Random Forest was the strongest candidate because it achieved the highest Macro F1-score and better minority-class performance compared with Dummy, Logistic Regression, and Decision Tree.

A small improvement experiment was performed by testing Random Forest models with different maximum depth values. A feature comparison was also performed by comparing the model with lag/rolling features against a version without lag/rolling features.

The final selected model was:

Random Forest Tuned max_depth 10

Performance on the test set:
- Accuracy: 0.8725
- Macro F1: 0.6923
- Weighted F1: 0.8840

Macro F1 was prioritized because the dataset is imbalanced and accuracy alone can overstate performance when the Low congestion class dominates.

Remaining difficulty:
Medium and High congestion classes are less frequent than Low, making minority-class prediction more challenging.
