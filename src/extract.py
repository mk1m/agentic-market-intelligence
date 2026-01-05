import yfinance as yf
import polars as pl
from datetime import datetime, timedelta
import os

def extract_financial_data(ticker: str, days: int = 30):
    """
    Extracts historical price data using yfinance.
    """
    print('='*50)
    print(f"Extracting data for {ticker}")
    print('='*50)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Fetch data
    data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
    
    # Flatten MultiIndex columns if present
    if hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1:
        data.columns = data.columns.get_level_values(0)

    # Convert to Polars DataFrame
    df = pl.from_pandas(data.reset_index())
    
    return df

if __name__ == "__main__":
    # Test with Bitcoin
    btc_data = extract_financial_data("BTC-USD")
    print(btc_data.head())
    
    # Save a raw sample to data
    os.makedirs("data", exist_ok=True)  # Create data directory if it doesn't exist
    btc_data.write_csv("data/raw_btc_data.csv")  # Save raw data to CSV
    print("Successfully saved raw data to data/raw_btc_data.csv")