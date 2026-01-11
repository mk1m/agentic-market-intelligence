import yfinance as yf
import polars as pl
from datetime import datetime, timedelta
import pandas as pd
import time

def extract_financial_data(ticker: str, days: int = 30, retries: int = 3):
    '''
    Extracts data with retry logic and returns a Polars DataFrame.
    '''
    for attempt in range(retries):
        try:
            print(f"--- Attempt {attempt + 1}: Extracting {ticker} ---")
            data = yf.download(ticker, period=f"{days}d", interval="1d")
            
            if data.empty:
                raise ValueError("No data returned")
            
            # flatten the MultiIndex columns if they exist
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # reset the index so 'Date' becomes a normal column
            data = data.reset_index()
            
            # rename columns to be simple strings
            data.columns = [str(col) for col in data.columns]

            df = pl.from_pandas(data)
            return df
            
        except Exception as e:
            print(f"Error: {e}. Waiting to retry...")
            time.sleep(2 ** attempt) 
            
    raise Exception(f"Failed to extract {ticker} after {retries} attempts.")