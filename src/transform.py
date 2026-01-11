import polars as pl

def transform_data(file_path: str, ticker: str):
    # try_parse_dates handles the 'Date' column format automatically
    df = pl.read_csv(file_path, try_parse_dates=True)
    
    # for stationarity: calculate daily returns
    # predict % change because raw price is non-stationary
    df = df.with_columns(
        pl.lit(ticker).alias("Ticker")
    )
    
    # lagging features
    # create lags for the last 3 days to capture recent momentum
    df = df.with_columns(
        (pl.col("Close").pct_change()).alias("Target_Returns")
    )
    
    # technical indicators
    df = df.with_columns([
        pl.col("Target_Returns").shift(1).alias("Lag_1"),
        pl.col("Target_Returns").shift(2).alias("Lag_2"),
        pl.col("Target_Returns").shift(3).alias("Lag_3"),
        pl.col("Close").rolling_mean(window_size=7).alias("MA7"),
        pl.col("Close").rolling_mean(window_size=21).alias("MA21"),
        pl.col("Close").rolling_std(window_size=7).alias("Volatility")
    ])
    
    # drop rows with nulls created by shifting/rolling
    return df.drop_nulls()

if __name__ == "__main__":
    # test with a custom ticker
    test_ticker = "AAPL"
    df = transform_data(f"data/raw_{test_ticker}.csv", test_ticker)
    print(df.select(["Date", "Ticker", "Close"]).head())