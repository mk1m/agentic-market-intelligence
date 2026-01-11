import polars as pl
import xgboost as xgb
from sklearn.model_selection import train_test_split
import joblib
import matplotlib.pyplot as plt

def train_forecaster():
    # load processed data
    df = pl.read_csv("data/processed_btc_data.csv")
    
    # define features and target
    # use lags and volatility to predict the next return
    features = ["Lag_1", "Lag_2", "Lag_3", "Volatility"]
    X = df.select(features).to_pandas()
    y = df.select("Target_Returns").to_pandas()
    
    # split data; no random shuffle for time-series data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    eval_set = [(X_train, y_train), (X_test, y_test)]
    
    # initialize xgboost regressor with regularization
    model = xgb.XGBRegressor(
        n_estimators=100,           # number of trees
        learning_rate=0.01,         # learning rate
        max_depth=3,                # maximum depth of trees
        reg_lambda=10,              # l2 regularization to prevent overfitting
        gamma=0.1,                  # minimum loss reduction required to make a split
        early_stopping_rounds=10,   # stop if test error doesn't improve for 10 iterations
        eval_metric="rmse",         # root mean squared error
    )
    
    print("--- Training XGBoost Model ---")
    model.fit(
        X_train, y_train,
        eval_set=eval_set,
        verbose=True                # prints error to the console at each step
    )

    # get final RMSE
    results = model.evals_result()
    train_rmse = results['validation_0']['rmse']
    test_rmse = results['validation_1']['rmse']
    print(f"Final Train RMSE: {train_rmse[-1]:.5f}")
    print(f"Final Test RMSE: {test_rmse[-1]:.5f}")

    # save the model for the agent to use
    joblib.dump(model, "models/xgboost_forecaster.pkl")
    print("Model saved to models/xgboost_forecaster.pkl")

    return results

def plot_learning_curve(results):
    plt.figure(figsize=(10, 5))
    plt.plot(results['validation_0']['rmse'], label='Train Error')
    plt.plot(results['validation_1']['rmse'], label='Test Error')
    plt.xlabel('Number of Trees (Iterations)')
    plt.ylabel('RMSE')
    plt.title('XGBoost Learning Curve')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    results = train_forecaster()
    plot_learning_curve(results)