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

def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average Directional Index (ADX).
    """
    if len(df) < period:
        return pd.Series(index=df.index, dtype=float)

    high = df['High']
    low = df['Low']
    close = df['Close']
    
    # Calculate TR (True Range)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # directional movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
    # smooth
    # alpha = 1/period
    tr_smooth = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (pd.Series(plus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean() / tr_smooth)
    minus_di = 100 * (pd.Series(minus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean() / tr_smooth)
    
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    
    return adx

def calculate_adaptive_lookback(df: pd.DataFrame, l_base: int = 5, alpha: float = 0.5) -> pd.Series:
    """
    Calculate Adaptive Fractal Lookback based on ATR ratio (Market Structure Engine).
    L_adaptive = round(L_base * (1 + alpha * (ATR_long / ATR_short - 1)))
    """
    atr_short = calculate_atr(df, period=14)
    atr_long = calculate_atr(df, period=100)
    
    # Avoid division by zero
    ratio = atr_long / atr_short.replace(0, np.nan)
    ratio = ratio.fillna(1.0)
    
    l_adaptive = l_base * (1 + alpha * (ratio - 1))
    l_adaptive = l_adaptive.round().astype(int)
    
    # Clip logic to sensible bounds? Spec doesn't say, but let's keep it >= 2
    l_adaptive = l_adaptive.clip(lower=2)
    
    return l_adaptive
