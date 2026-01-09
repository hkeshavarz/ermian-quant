import pandas as pd
import numpy as np
from .indicators import calculate_atr, find_swings_fractal

def detect_fvg(df: pd.DataFrame, atr_threshold_multiplier: float = 0.5, atr_col: str = 'ATR') -> pd.DataFrame:
    """
    Detect Fair Value Gaps (FVG) based on 3-candle logic.
    """
    if atr_col not in df.columns:
        pass

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
    
    min_size = atr * atr_threshold_multiplier
    
    is_bull_fvg = (gap_bull > 0) & green_candle_prev & (gap_bull > min_size)
    is_bear_fvg = (gap_bear > 0) & red_candle_prev & (gap_bear > min_size)
    
    result = pd.DataFrame(index=df.index)
    result['FVG_Bullish'] = is_bull_fvg
    result['FVG_Bearish'] = is_bear_fvg
    result['FVG_Top'] = np.where(is_bull_fvg, low, np.where(is_bear_fvg, low.shift(2), np.nan))
    result['FVG_Bottom'] = np.where(is_bull_fvg, high.shift(2), np.where(is_bear_fvg, high, np.nan))
    
    return result

def detect_liquidity_sweeps(df: pd.DataFrame, swing_lookback: int = 5) -> pd.DataFrame:
    """
    Detect Liquidity Sweeps (Turtle Soup).
    """
    swings = find_swings_fractal(df, lookback=swing_lookback)
    
    sweeps = pd.DataFrame(index=df.index)
    sweeps['Sweep_Bullish'] = False
    sweeps['Sweep_Bearish'] = False
    
    last_swing_high = -np.inf
    last_swing_low = np.inf
    
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values
    
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
        
        if last_swing_high > -np.inf:
            if high[i] > last_swing_high and close[i] < last_swing_high:
                res_bear[i] = True
        
        if last_swing_low < np.inf:
            if low[i] < last_swing_low and close[i] > last_swing_low:
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
            # Find closest valid prior swing high (before i-2)
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
