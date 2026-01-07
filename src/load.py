import sqlite3
import polars as pl

def load_to_sqlite(df: pl.DataFrame, db_name: str = "data/market_data.db", table_name: str = "btc_metrics"):
    """
    Loads a Polars DataFrame into a SQLite database.
    """
    print('='*50)
    print(f"Loading data into SQL table: {table_name}")
    print('='*50)
    
    # SQLite requires a connection
    conn = sqlite3.connect(db_name)
    
    # Convert Polars to Pandas just for the 'to_sql' helper
    df.to_pandas().to_sql(table_name, conn, if_exists="replace", index=False)
    
    conn.close()
    print(f"Successfully loaded data into {db_name}")

if __name__ == "__main__":
    # Grab the processed data from transform step
    processed_df = pl.read_csv("data/processed_btc_data.csv")
    
    # Load it into the SQL database
    load_to_sqlite(processed_df)