import polars as pl

def transform_data(file_path: str):
    # try_parse_dates handles the 'Date' column format automatically
    df = pl.read_csv(file_path, try_parse_dates=True)
    
    # for stationarity: calculate daily returns
    # predict % change because raw price is non-stationary
    df = df.with_columns(
        (pl.col("Close").pct_change()).alias("Target_Returns")
    )
    
    # lagging features
    # create lags for the last 3 days to capture recent momentum
    for i in range(1, 4):
        df = df.with_columns(
            pl.col("Target_Returns").shift(i).alias(f"Lag_{i}")
        )
    
    # technical indicators
    df = df.with_columns([
        pl.col("Close").rolling_mean(window_size=7).alias("MA7"),
        pl.col("Close").rolling_mean(window_size=21).alias("MA21"), 
        pl.col("Close").rolling_std(window_size=7).alias("Volatility")
    ])
    
    # drop rows with nulls created by shifting/rolling
    return df.drop_nulls()

if __name__ == "__main__":
    df = transform_data("data/raw_BTC-USD.csv")
    df.write_csv("data/processed_btc_data.csv")
    print(df.head())