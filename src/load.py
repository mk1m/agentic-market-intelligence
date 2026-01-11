import sqlite3
import polars as pl
import os

def load_to_sqlite(df: pl.DataFrame, db_name: str = "data/market_data.db", table_name: str = "stock_metrics"):
    '''
    Load Polars DataFrame into a SQLite database, appending new tickers 
    without deleting old ones.
    '''
    # ensure data directory exists
    os.makedirs(os.path.dirname(db_name), exist_ok=True)

    print('='*50)
    print(f"Loading data into SQL table: {table_name}")
    print('='*50)
    
    # connect to SQLite
    conn = sqlite3.connect(db_name)

    # first drop existing rows for this specific ticker to avoid duplicates
    ticker = df["Ticker"][0] if "Ticker" in df.columns else "UNKNOWN"
    
    try:
        conn.execute(f"DELETE FROM {table_name} WHERE Ticker = '{ticker}'")
    except sqlite3.OperationalError:
        # table doesn't exist yet
        pass

    # convert and load
    df.to_pandas().to_sql(table_name, conn, if_exists="append", index=False)
    
    conn.close()
    print(f"Successfully loaded {ticker} data into {db_name}")

if __name__ == "__main__":
    # if running this file directly for testing
    if os.path.exists("data/processed_btc_data.csv"):
        processed_df = pl.read_csv("data/processed_btc_data.csv")
        load_to_sqlite(processed_df)
    else:
        print("No processed data found to load.")