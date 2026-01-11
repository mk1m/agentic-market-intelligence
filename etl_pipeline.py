from src.extract import extract_financial_data
from src.transform import transform_data
from src.load import load_to_sqlite
from src.train_model import train_forecaster
from src.agent import get_latest_context, run_agentic_analysis 
import os
print(f"Current working directory: {os.getcwd()}")
os.makedirs("models", exist_ok=True)
os.makedirs("data", exist_ok=True)

def run_pipeline(ticker):
    """
    Runs the full ETL pipeline for a given ticker.
    """
    # E
    raw_df = extract_financial_data(ticker)
    raw_df.write_csv(f"data/raw_{ticker}.csv")
    
    # T
    processed_df = transform_data(f"data/raw_{ticker}.csv", ticker)
    
    # L
    # save processed data for XGboost
    processed_df.write_csv(f"data/processed_{ticker}_data.csv")
    # load processed data into SQLite for agent
    load_to_sqlite(processed_df)

    # train model if it doesn't exist
    model_path = f"models/{ticker}_xgboost_forecaster.pkl"
    if not os.path.exists(model_path):
        print(f"No model found for {ticker}. Training new model...")
        train_forecaster(ticker) # This will create the .pkl file

    print("Pipeline Complete.")

if __name__ == "__main__":
    user_ticker = input("Enter a stock ticker (e.g., AAPL, NVDA, BTC-USD): ").upper()
    
    # run ETL for ticker
    run_pipeline(user_ticker)
    
    # run agent for ticker
    context = get_latest_context(user_ticker)
    if context:
        print(run_agentic_analysis(context))