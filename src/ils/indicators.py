import pandas as pd
import numpy as np

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).
    """
    if len(df) < period:
        return pd.Series(index=df.index, dtype=float)

    high = df['High']
    low = df['Low']
    close = df['Close']
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean() 
    return atr

def calculate_chop_index(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Choppiness Index.
    """
    if len(df) < period:
        return pd.Series(index=df.index, dtype=float)

    high = df['High']
    low = df['Low']
    close = df['Close']
    
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    sum_tr = tr.rolling(window=period).sum()
    max_high = high.rolling(window=period).max()
    min_low = low.rolling(window=period).min()
    
    range_diff = max_high - min_low
    range_diff = range_diff.replace(0, np.nan) 
    
    chop = 100 * np.log10(sum_tr / range_diff) / np.log10(period)
    
    return chop

def find_swings_fractal(df: pd.DataFrame, lookback: int = 2) -> pd.DataFrame:
    """
    Identify fractal swing highs and lows.
    """
    high = df['High']
    low = df['Low']
    
    is_swing_high = pd.Series(True, index=df.index)
    is_swing_low = pd.Series(True, index=df.index)
    
    for i in range(1, lookback + 1):
        is_swing_high &= (high > high.shift(i)) & (high > high.shift(-i))
        is_swing_low &= (low < low.shift(i)) & (low < low.shift(-i))
        
    df_out = pd.DataFrame(index=df.index)
    df_out['SwingHigh'] = np.nan
    df_out['SwingLow'] = np.nan
    
    df_out.loc[is_swing_high, 'SwingHigh'] = high[is_swing_high]
    df_out.loc[is_swing_low, 'SwingLow'] = low[is_swing_low]
    
    return df_out
