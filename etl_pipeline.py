from src.extract import extract_financial_data
from src.transform import transform_data
from src.load import load_to_sqlite

def run_pipeline(ticker="BTC-USD"):
    """
    Runs the full ETL pipeline for a given ticker.
    """
    # E
    raw_df = extract_financial_data(ticker)
    raw_df.write_csv(f"data/raw_{ticker}.csv")
    
    # T
    processed_df = transform_data(f"data/raw_{ticker}.csv")
    
    # L
    load_to_sqlite(processed_df)
    print("Pipeline Complete!")

if __name__ == "__main__":
    run_pipeline()