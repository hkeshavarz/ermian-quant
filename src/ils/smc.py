import pandas as pd
import numpy as np
from .indicators import calculate_atr, find_swings_fractal

def validate_displacement(df: pd.DataFrame, atr_col: str = 'ATR') -> pd.DataFrame:
    """
    Validate Displacement Candles based on 3.0 spec:
    - Body / Range >= 0.6
    - Range >= 1.5 * ATR
    - Bearish: Close in bottom 30%
    - Bullish: Close in top 30%
    """
    high = df['High']
    low = df['Low']
    open_ = df['Open']
    close = df['Close']
    atr = df[atr_col] if atr_col in df.columns else pd.Series(0, index=df.index)
    
    candle_range = high - low
    body_size = (close - open_).abs()
    
    # Avoid zero division
    body_ratio = body_size / candle_range.replace(0, np.inf)
    
    range_condition = candle_range >= (1.5 * atr)
    body_condition = body_ratio >= 0.6
    
    # Directional close
    # High - Low is range. 
    # Bearish: Close <= Low + 0.3 * Range
    bearish_close = close <= (low + 0.3 * candle_range)
    # Bullish: Close >= High - 0.3 * Range = Low + 0.7 * Range (approx, strictly High - 0.3*R)
    bullish_close = close >= (high - 0.3 * candle_range)
    
    is_displacement_bull = range_condition & body_condition & bullish_close
    is_displacement_bear = range_condition & body_condition & bearish_close
    
    res = pd.DataFrame(index=df.index)
    res['Displacement_Bullish'] = is_displacement_bull
    res['Displacement_Bearish'] = is_displacement_bear
    return res

def detect_fvg(df: pd.DataFrame, atr_threshold_multiplier: float = 0.5, atr_col: str = 'ATR') -> pd.DataFrame:
    """
    Detect Fair Value Gaps (FVG) based on 3-candle logic.
    Strict size filter: Gap >= 0.5 * ATR14
    """
    high = df['High']
    low = df['Low']
    open_ = df['Open']
    close = df['Close']
    atr = df[atr_col] if atr_col in df.columns else pd.Series(0, index=df.index)

    # Bullish
    gap_bull = low - high.shift(2)
    green_candle_prev = close.shift(1) > open_.shift(1)
    
    # Bearish
    gap_bear = low.shift(2) - high
    red_candle_prev = close.shift(1) < open_.shift(1)
    
    # Strict size filter
    min_size = atr * atr_threshold_multiplier
    
    is_bull_fvg = (gap_bull > 0) & green_candle_prev & (gap_bull >= min_size)
    is_bear_fvg = (gap_bear > 0) & red_candle_prev & (gap_bear >= min_size)
    
    result = pd.DataFrame(index=df.index)
    result['FVG_Bullish'] = is_bull_fvg
    result['FVG_Bearish'] = is_bear_fvg
    result['FVG_Top'] = np.where(is_bull_fvg, low, np.where(is_bear_fvg, low.shift(2), np.nan))
    result['FVG_Bottom'] = np.where(is_bull_fvg, high.shift(2), np.where(is_bear_fvg, high, np.nan))
    
    return result

def detect_liquidity_sweeps(df: pd.DataFrame, swing_lookback: int = 5, atr_col: str = 'ATR') -> pd.DataFrame:
    """
    Detect Liquidity Sweeps (Turtle Soup).
    Bearish Sweep:
      - High > SwingHigh
      - Close <= SwingHigh + 0.2 * ATR
    Bullish Sweep:
      - Low < SwingLow
      - Close >= SwingLow - 0.2 * ATR
    """
    swings = find_swings_fractal(df, lookback=swing_lookback)
    atr = df[atr_col] if atr_col in df.columns else pd.Series(0, index=df.index)
    
    sweeps = pd.DataFrame(index=df.index)
    sweeps['Sweep_Bullish'] = False
    sweeps['Sweep_Bearish'] = False
    
    last_swing_high = -np.inf
    last_swing_low = np.inf
    
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values
    atr_vals = atr.values
    
    swing_highs = swings['SwingHigh'].values
    swing_lows = swings['SwingLow'].values
    
    res_bull = np.zeros(len(df), dtype=bool)
    res_bear = np.zeros(len(df), dtype=bool)

    for i in range(len(df)):
        confirmed_idx = i - swing_lookback
        if confirmed_idx >= 0:
            if not np.isnan(swing_highs[confirmed_idx]):
                last_swing_high = swing_highs[confirmed_idx]
            if not np.isnan(swing_lows[confirmed_idx]):
                last_swing_low = swing_lows[confirmed_idx]
        
        # Bearish Sweep Logic
        if last_swing_high > -np.inf:
            threshold = last_swing_high + (0.2 * atr_vals[i])
            if high[i] > last_swing_high and close[i] <= threshold:
                res_bear[i] = True
        
        # Bullish Sweep Logic
        if last_swing_low < np.inf:
            threshold = last_swing_low - (0.2 * atr_vals[i])
            if low[i] < last_swing_low and close[i] >= threshold:
                res_bull[i] = True
                
    sweeps['Sweep_Bullish'] = res_bull
    sweeps['Sweep_Bearish'] = res_bear
    
    return sweeps

def detect_order_blocks(df: pd.DataFrame, fvg_df: pd.DataFrame, swings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect Order Blocks (OB).
    Bullish OB: Last down-close candle before a move that:
       1. Created a Bullish FVG.
       2. Broke a Swing High (MSS).
    Bearish OB: Last up-close candle before a move that:
       1. Created a Bearish FVG.
       2. Broke a Swing Low (MSS).
       
    Returns DataFrame with 'OB_Bullish', 'OB_Bearish' booleans.
    """
    ob_bull = pd.Series(False, index=df.index)
    ob_bear = pd.Series(False, index=df.index)
    
    # We need access to values for speed
    close = df['Close'].values
    open_ = df['Open'].values
    high = df['High'].values
    low = df['Low'].values
    
    # FVG flags
    has_fvg_bull = fvg_df['FVG_Bullish'].values
    has_fvg_bear = fvg_df['FVG_Bearish'].values
    
    # Swings
    swing_highs = swings_df['SwingHigh'].values 
    swing_lows = swings_df['SwingLow'].values 
    
    for i in range(3, len(df)):
        # Check Bullish OB Condition
        if has_fvg_bull[i]:
            # This FVG implies a move from i-2 to i.
            # Did this move break structure?
            valid_break = False
            prev_swing_idx = -1
            
            # Look back for a swing high
            for k in range(i-3, max(0, i-50), -1):
                if not np.isnan(swing_highs[k]):
                    prev_swing_idx = k
                    break
            
            if prev_swing_idx != -1:
                # If Close of break candle (i) > Swing High
                if close[i] > high[prev_swing_idx]:
                    valid_break = True
            
            if valid_break:
                # Find the Origin Candle (last Down candle before i-2)
                # Scan back from i-2
                origin_idx = -1
                for k in range(i-2, max(0, i-10), -1):
                     # Down candle
                     if close[k] < open_[k]:
                         origin_idx = k
                         break
                
                if origin_idx != -1:
                    ob_bull.iloc[origin_idx] = True

        # Check Bearish OB Condition
        if has_fvg_bear[i]:
            valid_break = False
            prev_swing_idx = -1
            
            for k in range(i-3, max(0, i-50), -1):
                if not np.isnan(swing_lows[k]):
                    prev_swing_idx = k
                    break
            
            if prev_swing_idx != -1:
                # Break Low
                if close[i] < low[prev_swing_idx]:
                    valid_break = True
            
            if valid_break:
                # Find Origin (Last Up candle)
                origin_idx = -1
                for k in range(i-2, max(0, i-10), -1):
                    if close[k] > open_[k]:
                        origin_idx = k
                        break
                
                if origin_idx != -1:
                    ob_bear.iloc[origin_idx] = True
                    
    result = pd.DataFrame(index=df.index)
    result['OB_Bullish'] = ob_bull
    result['OB_Bearish'] = ob_bear
    return result
