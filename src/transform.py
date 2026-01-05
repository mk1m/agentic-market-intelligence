import polars as pl

def transform_data(file_path: str):
    """
    Cleans raw financial data and adds technical indicators using Polars.
    """
    print('='*50)
    print(f'Transforming data from {file_path}')
    print('='*50)

    # Load data
    df = pl.read_csv(file_path)
    
    # Handle nulls and ensure Date is a datetime object
    df = df.with_columns(pl.col("Date").str.to_datetime())
    
    # Add 7 day and 21 day moving averages
    df = df.with_columns([
        pl.col("Close").rolling_mean(window_size=7).alias("MA7"),
        pl.col("Close").rolling_mean(window_size=21).alias("MA21")
    ])
    
    # Add volatility - std of the last 7 days
    df = df.with_columns(
        pl.col("Close").rolling_std(window_size=7).alias("Volatility")
    )
    
    # Remove rows where MA/volatility couldn't be calculated
    df = df.drop_nulls()
    
    return df

if __name__ == "__main__":
    # Test transformation
    processed_df = transform_data("data/raw_btc_data.csv")
    print(processed_df.head())
    
    # Save processed data for the model
    processed_df.write_csv("data/processed_btc_data.csv")
    print("Successfully saved processed data to data/processed_btc_data.csv")