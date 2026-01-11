import pandas as pd
import numpy as np
from .indicators import calculate_atr, find_swings_fractal

def validate_displacement(df: pd.DataFrame, atr_col: str = 'ATR', body_ratio_min: float = 0.6, range_atr_min: float = 1.5) -> pd.DataFrame:
    """
    Validate Displacement Candles based on Configurable spec:
    - Body / Range >= body_ratio_min
    - Range >= range_atr_min * ATR
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
    
    range_condition = candle_range >= (range_atr_min * atr)
    body_condition = body_ratio >= body_ratio_min
    
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
    Strict size filter: Gap >= atr_threshold_multiplier * ATR14
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

def detect_liquidity_sweeps(df: pd.DataFrame, swings_df: pd.DataFrame, atr_col: str = 'ATR', sweep_atr_tolerance: float = 0.2) -> pd.DataFrame:
    """
    Detect Liquidity Sweeps (Turtle Soup).
    Uses pre-calculated `swings_df` which contains the 'Active Swing Level' for each bar.
    Tolerance: +/- sweep_atr_tolerance * ATR.
    """
    atr = df[atr_col] if atr_col in df.columns else pd.Series(0, index=df.index)
    
    sweeps = pd.DataFrame(index=df.index)
    sweeps['Sweep_Bullish'] = False
    sweeps['Sweep_Bearish'] = False
    
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values
    atr_vals = atr.values
    
    swing_highs = swings_df['SwingHigh'].values
    swing_lows = swings_df['SwingLow'].values
    
    res_bull = np.zeros(len(df), dtype=bool)
    res_bear = np.zeros(len(df), dtype=bool)

    for i in range(1, len(df)):
        # Active Swing Levels
        last_swing_high = swing_highs[i]
        last_swing_low = swing_lows[i]
        
        # Bearish Logic
        if not np.isnan(last_swing_high):
            threshold = last_swing_high + (sweep_atr_tolerance * atr_vals[i])
            
            # 1. Wick Sweep
            if high[i] > last_swing_high and close[i] <= threshold: 
                 if close[i] < last_swing_high:
                     res_bear[i] = True
            
            # 2. Delayed Sweep (Fakeout)
            # Prev candle closed ABOVE, Current candle closes BELOW
            if close[i-1] > last_swing_high and close[i] < last_swing_high:
                res_bear[i] = True
        
        # Bullish Logic
        if not np.isnan(last_swing_low):
            threshold = last_swing_low - (sweep_atr_tolerance * atr_vals[i])
            
            # 1. Wick Sweep
            if low[i] < last_swing_low and close[i] > last_swing_low:
                res_bull[i] = True
                
            # 2. Delayed Sweep
            # Prev candle closed BELOW, Current candle closes ABOVE
            if close[i-1] < last_swing_low and close[i] > last_swing_low:
                res_bull[i] = True
                
    sweeps['Sweep_Bullish'] = res_bull
    sweeps['Sweep_Bearish'] = res_bear
    
    return sweeps

def validate_mss(df: pd.DataFrame, swings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate Market Structure Shift (MSS).
    Simplified Vectorized Check against Active Swing Levels.
    """
    res = pd.DataFrame(index=df.index)
    # Check if Close breaks the Swing Level
    # Note: swings_df is already aligned so that row i contains the swing active for row i.
    res['MSS_Bullish'] = (df['Close'] > swings_df['SwingHigh'])
    res['MSS_Bearish'] = (df['Close'] < swings_df['SwingLow'])
    return res

def detect_order_blocks(df: pd.DataFrame, fvg_df: pd.DataFrame, swings_df: pd.DataFrame, volume_factor: float = 1.0) -> pd.DataFrame:
    """
    Detect Order Blocks (OB) with Volume Confirmation.
    """
    ob_bull = pd.Series(False, index=df.index)
    ob_bear = pd.Series(False, index=df.index)
    
    # We need access to values for speed
    close = df['Close'].values
    open_ = df['Open'].values
    
    # Volume Check
    check_vol = False
    vol_vals = None
    vol_sma = None
    if 'Volume' in df.columns:
        check_vol = True
        vol_vals = df['Volume'].values
        # Simple fillna for SMA
        vol_sma = df['Volume'].rolling(20).mean().fillna(0).values
    
    # FVG flags
    has_fvg_bull = fvg_df['FVG_Bullish'].values
    has_fvg_bear = fvg_df['FVG_Bearish'].values
    
    # Swings (Active Levels)
    swing_highs = swings_df['SwingHigh'].values 
    swing_lows = swings_df['SwingLow'].values 
    
    for i in range(3, len(df)):
        # Check Bullish OB Condition
        if has_fvg_bull[i]:
            # This FVG implies a move from i-2 to i caused FVG.
            # Did this move break structure?
            # We check if Close[i] broke the active Swing High.
            
            valid_break = False
            last_idx = i # or i-1? 
            # The break usually happens on the displacement candle (i-1) or the FVG candle (i).
            # The FVG logic says FVG is at i (gap between i-2 and i).
            # The move is the Green candle at i-1.
            # So usually we check if Close[i-1] broke structure? 
            # OR if Close[i] is holding above?
            # Standard: The displacement candle (i-1) should break structure.
            # But sometimes the follow through (i) confirms it.
            # Let's check Close[i] (current) and Close[i-1] (displacement).
            # If either broke the structure that was active THEN.
            
            active_high = swing_highs[i] 
            
            if not np.isnan(active_high):
                 if close[i] > active_high: 
                     valid_break = True
                     
            if valid_break:
                # Find the Origin Candle (last Down candle before i-2)
                origin_idx = -1
                for k in range(i-2, max(0, i-10), -1):
                     if close[k] < open_[k]:
                         origin_idx = k
                         break
                
                if origin_idx != -1:
                    is_valid_ob = True
                    if check_vol:
                         if vol_vals[origin_idx] <= (vol_sma[origin_idx] * volume_factor):
                             is_valid_ob = False
                             
                    if is_valid_ob:
                        ob_bull.iloc[origin_idx] = True

        # Check Bearish OB Condition
        if has_fvg_bear[i]:
            valid_break = False
            active_low = swing_lows[i]
            
            if not np.isnan(active_low):
                if close[i] < active_low:
                    valid_break = True
            
            if valid_break:
                origin_idx = -1
                for k in range(i-2, max(0, i-10), -1):
                    if close[k] > open_[k]:
                        origin_idx = k
                        break
                
                if origin_idx != -1:
                    is_valid_ob = True
                    if check_vol:
                         if vol_vals[origin_idx] <= (vol_sma[origin_idx] * volume_factor):
                             is_valid_ob = False
                             
                    if is_valid_ob:
                        ob_bear.iloc[origin_idx] = True
                    
    result = pd.DataFrame(index=df.index)
    result['OB_Bullish'] = ob_bull
    result['OB_Bearish'] = ob_bear
    return result
