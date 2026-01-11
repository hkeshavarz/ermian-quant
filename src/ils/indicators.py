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

def find_swings_adaptive(df: pd.DataFrame, lookback_series: pd.Series) -> pd.DataFrame:
    """
    Identify fractal swing highs and lows using a dynamic lookback.
    Returns DataFrame with 'SwingHigh' and 'SwingLow' columns representing
    the most recent CONFIRMED swing points known at index `i`.
    """
    high = df['High'].values
    low = df['Low'].values
    lb_vals = lookback_series.fillna(5).astype(int).values
    
    n = len(df)
    
    last_confirmed_high = np.nan
    last_confirmed_low = np.nan
    
    out_highs = np.full(n, np.nan)
    out_lows = np.full(n, np.nan)
    
    for i in range(n):
        # 1. Check if we confirm a new swing at this bar
        L = lb_vals[i]
        if L < 2: L = 2
        
        center_idx = i - L
        if center_idx >= 0:
            start = center_idx - L
            end = i + 1
            if start < 0: start = 0
            
            if high[center_idx] == np.max(high[start:end]):
                last_confirmed_high = high[center_idx]
                
            if low[center_idx] == np.min(low[start:end]):
                last_confirmed_low = low[center_idx]
        
        # 2. Store current knowledge
        out_highs[i] = last_confirmed_high
        out_lows[i] = last_confirmed_low
            
    res = pd.DataFrame(index=df.index)
    res['SwingHigh'] = out_highs
    res['SwingLow'] = out_lows
    return res

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
