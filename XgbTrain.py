import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor

sns.set_theme(style="whitegrid")

df = pd.read_csv("train_weather_merged.csv")

df = df[df.is_mid_route == 0].drop(columns="is_mid_route")

df = df.sort_values(["train_no", "station_index"])
df["prev_delay"] = df.groupby("train_no")["delay_minutes"].shift(1)

df["train_no_enc"] = LabelEncoder().fit_transform(df["train_no"])

feature_map = {
    "station_index": "f1",
    "humidity": "f2",
    "temp_c": "f3",
    "dew_point": "f4",
    "temp_spread": "f5",
    "day_of_week": "f6",
    "prev_delay": "f7",
    "train_no_enc": "f8",
}
FEATURES = list(feature_map.values())
TARGET = "delay_minutes"

df["day_of_week"] = LabelEncoder().fit_transform(df["day_of_week"])
df = df.rename(columns=feature_map)
df = df.dropna(subset=["f1", "f2", "f3", "f4", "f5", "f6", TARGET])

X, y = df[FEATURES], df[TARGET]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

xgb = XGBRegressor(
    n_estimators=300, max_depth=5, learning_rate=0.05, random_state=42
)
xgb.fit(X_train, y_train)
xgb_pred = xgb.predict(X_test)

lr = LinearRegression()
lr.fit(X_train.fillna(0), y_train)
lr_pred = lr.predict(X_test.fillna(0))

def report(name, y_true, y_pred):
    rmse = root_mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    print(f"{name:>10}:  RMSE={rmse:7.2f}  MAE={mae:7.2f}  R2={r2:.3f}")
    return rmse, mae, r2

print("\n--- Model comparison ---")
xgb_rmse, xgb_mae, xgb_r2 = report("XGBoost", y_test, xgb_pred)
lr_rmse, lr_mae, lr_r2 = report("Linear", y_test, lr_pred)

fig, axes = plt.subplots(2, 2, figsize=(13, 10))

metrics_df = pd.DataFrame(
    {"XGBoost": [xgb_rmse, xgb_mae], "Linear": [lr_rmse, lr_mae]},
    index=["RMSE", "MAE"],
)
metrics_df.plot(kind="bar", ax=axes[0, 0], color=["#2ca02c", "#d62728"])
axes[0, 0].set_title("XGBoost vs Linear Regressor — Error")
axes[0, 0].set_ylabel("Minutes")
axes[0, 0].tick_params(axis="x", rotation=0)

axes[0, 1].scatter(y_test, xgb_pred, alpha=0.4, s=15, label="XGBoost", color="#2ca02c")
axes[0, 1].scatter(y_test, lr_pred, alpha=0.4, s=15, label="Linear", color="#d62728")
lims = [y_test.min(), y_test.max()]
axes[0, 1].plot(lims, lims, "k--", linewidth=1)
axes[0, 1].set_xlabel("Actual delay (min)")
axes[0, 1].set_ylabel("Predicted delay (min)")
axes[0, 1].set_title("Actual vs Predicted")
axes[0, 1].legend()

importances = pd.Series(xgb.feature_importances_, index=FEATURES).sort_values()
importances.plot(kind="barh", ax=axes[1, 0], color="#1f77b4")
axes[1, 0].set_title("XGBoost Feature Importance")
axes[1, 0].set_xlabel("Importance")

sns.histplot(y_test - xgb_pred, bins=40, kde=True, ax=axes[1, 1], color="#2ca02c")
axes[1, 1].axvline(0, color="k", linestyle="--", linewidth=1)
axes[1, 1].set_title("XGBoost Residuals (Actual - Predicted)")
axes[1, 1].set_xlabel("Residual (min)")

plt.tight_layout()
plt.savefig("xgboost_vs_linear.png", dpi=150)
print("\nSaved plot -> xgboost_vs_linear.png")