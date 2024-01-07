import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, classification_report
import matplotlib.pyplot as plt
import joblib

pd.set_option('display.max_columns', None)

# Define the rolling_averages function
def rolling_averages(group, cols, new_cols):
    group = group.sort_values("date")
    rolling_stats = group[cols].rolling(3, closed='left').mean()
    group[new_cols] = rolling_stats
    group = group.dropna(subset=new_cols)
    return group

# Load data
matches = pd.read_csv("matches.csv")

# Data preprocessing
#matches = matches[matches['comp'] == 'Premier League']
matches.drop(columns=['notes'], axis=1, inplace=True)
matches["date"] = pd.to_datetime(matches["date"])
matches = matches[~matches['result'].isna()]
matches.drop(['attendance','captain'], axis=1, inplace=True)
matches = matches.dropna()
matches['formation'] = matches['formation'].str.replace('◆', '')
matches['gf'] = matches['gf'].apply(lambda x: int(str(x).split('(')[0].strip()) if '(' in str(x) else x)
matches['ga'] = matches['ga'].apply(lambda x: int(str(x).split('(')[0].strip()) if '(' in str(x) else x)
matches["target"] = (matches["result"] == "W").astype("int")
matches["venue_code"] = matches["venue"].astype("category").cat.codes
matches["opp_code"] = matches["opponent"].astype("category").cat.codes
matches["formation_code"] = matches["formation"].astype("category").cat.codes
matches["comp_code"] = matches["comp"].astype("category").cat.codes
matches["hour"] = matches["time"].str.replace(":.+", "", regex=True).astype("int")
matches["day_code"] = matches["date"].dt.dayofweek

# Feature Engineering using rolling averages
cols = ["gf", "ga", "sh", "sot", "dist", "fk", "pk", "pkatt", "poss","g/sot","g/sh"]
new_cols = [f"{c}_rolling" for c in cols]
matches_rolling = matches.groupby("team").apply(lambda x: rolling_averages(x, cols, new_cols))

# Drop NaN values after rolling averages
matches_rolling = matches_rolling.dropna(subset=new_cols)
matches_rolling = matches_rolling.reset_index(drop=True)

# Update predictors
selected_features = ["venue_code", "opp_code", "day_code", "formation_code","comp_code"] + new_cols

train_data = matches_rolling[matches_rolling['date'] < '2022-01-01']
test_data = matches_rolling[matches_rolling['date'] >= '2022-01-01']

# Split the dataset into training and test sets
X_train, y_train = train_data[selected_features], train_data["target"]
X_test, y_test = test_data[selected_features], test_data["target"]

# Apply the best hyperparameters
best_rf_params = {'max_depth': 5, 'min_samples_leaf': 1, 'min_samples_split': 2, 'n_estimators': 50}
best_rf = RandomForestClassifier(random_state=1, **best_rf_params)

# Feature Importance Plot for the best RandomForest model
best_rf.fit(train_data[selected_features], train_data["target"])

# Feature Importance Plot for the best RandomForest model
feature_importances_rf = pd.Series(best_rf.feature_importances_, index=selected_features)
feature_importances_rf.sort_values().plot(kind='barh', title='Feature Importance - RandomForest')
plt.show()

# Model Evaluation on the test set for all models
preds = best_rf.predict(X_test)
print(f"Model: Random Forest")
print("Accuracy:", accuracy_score(y_test, preds))
print("Precision Score:", precision_score(y_test, preds))
print("Probability of victory:", best_rf.predict_proba(X_test)[:, 1])
print("Recall Score:", recall_score(y_test, preds))
print("F1 Score:", f1_score(y_test, preds))
print("ROC AUC Score:", roc_auc_score(y_test, preds))
print("Classification Report:\n", classification_report(y_test, preds))

#joblib.dump(best_rf, 'random_forest_model.joblib')