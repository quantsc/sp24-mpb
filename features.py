import polars as pl

def bid_ask_spread(df: pl.DataFrame) -> pl.Series:
    """A positive value indicating the current difference between the bid and ask prices in the current order books.
    See Kearns HFT page 7.
    """
    return df['ask_px_00'] - df['bid_px_00']

def mid_price(df: pl.DataFrame) -> pl.Series:
    """The average of the best bid and ask prices in the current order books."""
    return (df['ask_px_00'] + df['bid_px_00']) / 2

def weighted_mid_price(df: pl.DataFrame) -> pl.Series:
    """A variation on mid-price where the average of the bid and ask prices is weighted according to their inverse volume."""
    return (df['ask_px_00'] * df['bid_ct_00'] + df['bid_px_00'] * df['ask_ct_00']) / (df['bid_ct_00'] + df['ask_ct_00'])

def volume_imbalance(df: pl.DataFrame) -> pl.Series:
    """A signed quantity indicating the number of shares at the bid minus the number of shares at the ask in the current order books."""
    return df['bid_ct_00'] - df['ask_ct_00']

def log_return(df: pl.DataFrame) -> pl.Series:
    """The natural logarithm of the ratio of the current mid-price to the previous mid-price."""
    return (df['mid_price'] / df['mid_price'].shift(1)).ln()

# def rolling_signed_transaction_volume(df: pl.DataFrame, lookback: int=15) -> pl.Series:
#     """A signed quantity indicating the number of shares bought in the last lookback seconds minus the number of shares sold in the last lookback seconds."""
#     bought = df['purchase_ct'].rolling(lookback).sum()
#     sold = df['sale_ct'].rolling(lookback).sum()
#     return bought - sold

def rolling_weighted_mid_price(df: pl.DataFrame, lookback: int=15) -> pl.Series:
    """A variation on weighted mid-price where the average of the bid and ask prices is weighted according to their inverse volume in the last lookback seconds."""
    return (df['ask_px_00'] * df['bid_ct_00'] + df['bid_px_00'] * df['ask_ct_00']).rolling(lookback).sum() / (df['bid_ct_00'] + df['ask_ct_00']).rolling(lookback).sum()

def rolling_volatility(df: pl.DataFrame, lookback: int=15) -> pl.Series:
    """The standard deviation of log returns in the last lookback seconds."""
    return df['log_return'].rolling(lookback).std()

def rolling_volume_imbalance(df: pl.DataFrame, lookback: int=15) -> pl.Series:
    """A signed quantity indicating the number of shares at the bid minus the number of shares at the ask in the last lookback seconds."""
    return df['bid_ct_00'].rolling(lookback).sum() - df['ask_ct_00'].rolling(lookback).sum()

def target(df: pl.DataFrame, offset: int) -> pl.Series:
    """The natural logarithm of the ratio of the future mid-price to the current mid-price, offset by the given number of seconds."""
    return (df['mid_price'].shift(-offset) / df['mid_price']).ln()

def base_features(df: pl.DataFrame, aggregation: str = "1s") -> pl.DataFrame:
    """Build a DataFrame of base features from the given DataFrame.
    
    # Assumptions
    - The DataFrame has columns for the following:
    - ['ts_recv', 'action', 'side', 'price', 'size', 'symbol']

    # Returns
    - Number of each action [T = Trade, F = Fill, C = Cancel, M = Modify, A = Add]
    - Number of shares bought and sold
    - best bid and ask price
    - bid and ask size


    Notes: 
    - If many people are cancelling, or placing orders, that is information that can be used to predict future price movements.
    
    """
    return df.with_columns([
        bid_ask_spread(df),
        mid_price(df),
        weighted_mid_price(df),
        volume_imbalance(df),
    ])

def build_features(df: pl.DataFrame, lookback: int = 60, offset: int=5) -> pl.DataFrame:
    """Build a DataFrame of features from the given DataFrame.
    
    # Assumptions
    - The DataFrame has columns for the following:
        - `ask_px_00`: The best ask price in the current order books.
        - `bid_px_00`: The best bid price in the current order books.
        - `ask_ct_00`: The number of shares at the best ask price in the current order books.
        - `bid_ct_00`: The number of shares at the best bid price in the current order books.
        - `purchase_ct`: The number of shares bought in the last second.
        - `sale_ct`: The number of shares sold in the last second.
    # TODO: 
    - Support multiple depth levels 
    - Support multiple order types
    """
    feature_cols = [
        'log_return',
        'rolling_volume_imbalance',
        'rolling_weighted_mid_price',
        'rolling_volatility',
    ]
    df = df.with_columns(
        log_return=log_return(df),
        rolling_weighted_mid_price=rolling_weighted_mid_price(df, lookback),
        rolling_volatility=rolling_volatility(df, lookback),
        rolling_volume_imbalance=rolling_volume_imbalance(df, lookback),
    )
    for col in feature_cols:
        df = df.with_column(col + f"_mean_{lookback}", df[col].rolling(lookback).mean())
        df = df.with_column(col + f"_std_{lookback}", df[col].rolling(lookback).std())
        df = df.with_column(col + f"_sum_{lookback}", df[col].rolling(lookback).sum())
        df = df.with_column(col + f"_max_{lookback}", df[col].rolling(lookback).max())
        df = df.with_column(col + f"_min_{lookback}", df[col].rolling(lookback).min())
        df = df.with_column(col + f"_quantile_25_{lookback}", df[col].rolling(lookback).quantile(0.25))
        df = df.with_column(col + f"_quantile_75_{lookback}", df[col].rolling(lookback).quantile(0.75))
        df = df.with_column(col + f"_skew_{lookback}", df[col].rolling(lookback).skew())
        df = df.with_column(col + f"_kurtosis_{lookback}", df[col].rolling(lookback).kurtosis())
        df = df.with_column(col + f"_corr_{lookback}", df[col].rolling(lookback).corr())
    return df
