import pandas as pd
import numpy as np
from datetime import time
from .indicators import calculate_atr, calculate_chop_index, find_swings_fractal, calculate_adx
from .smc import detect_fvg, detect_liquidity_sweeps, detect_order_blocks, validate_displacement
from .risk import get_risk_percentage, calculate_position_size

def check_killzone(timestamp) -> bool:
    """
    Check if time is within London or NY Killzones (UTC).
    London Killzone: 07:00–10:00
    NY Killzone: 12:00–15:00
    London Close: 15:00–17:00
    """
    if not isinstance(timestamp, pd.Timestamp):
        return False
    t = timestamp.time()
    
    # London Killzone
    if time(7, 0) <= t <= time(10, 0):
        return True
    # NY Killzone
    if time(12, 0) <= t <= time(15, 0):
        return True
    # London Close
    if time(15, 0) <= t <= time(17, 0):
        return True
        
    return False

def calculate_confluence_score(row, htf_bias: str = 'Neutral'):
    """
    Calculate Tiered Score (0-100) based on ILS 3.0 factors.
    Returns (score, breakdown_dict).
    """
    score = 0
    breakdown = {'Score_HTF': 0, 'Score_Disp': 0, 'Score_Liq': 0, 'Score_Context': 0}
    
    # 1. HTF Alignment (Max 40)
    # Bias aligned: +25
    if htf_bias == 'Bullish' and row.get('Signal') == 'Long':
        score += 25
        breakdown['Score_HTF'] += 25
    elif htf_bias == 'Bearish' and row.get('Signal') == 'Short':
        score += 25
        breakdown['Score_HTF'] += 25
        
    # HTF POI: +15
    if row.get('Near_POI', False):
        score += 15
        breakdown['Score_HTF'] += 15
    
    # 2. Displacement (Max 20)
    # Strong Displacement: +10
    score += 10
    breakdown['Score_Disp'] += 10
    
    # Clean FVG: +10
    if row.get('Signal') == 'Long' and row.get('FVG_Bullish'):
        score += 10
        breakdown['Score_Disp'] += 10
    elif row.get('Signal') == 'Short' and row.get('FVG_Bearish'):
        score += 10
        breakdown['Score_Disp'] += 10
    
    # 3. Liquidity (Max 25)
    # HTF Sweep/Inducement
    if row.get('Sweep_Bullish') or row.get('Sweep_Bearish'):
        score += 15
        breakdown['Score_Liq'] += 15

    # 4. Context (Max 15)
    # Killzone timing: +10
    if row.get('In_Killzone', False):
        score += 10
        breakdown['Score_Context'] += 10
    
    # CHOP < 50: +5 (Trendiness)
    if 'Chop' in row and row['Chop'] < 50:
        score += 5
        breakdown['Score_Context'] += 5
        
    return score, breakdown

def run_strategy(df: pd.DataFrame, account_equity: float = 10000.0, htf_bias = 'Neutral') -> pd.DataFrame:
    """
    Run the ILS 3.0 Strategy on a dataframe.
    htf_bias: str or pd.Series/list aligned with df index.
    """
    # 1. Indicators
    df['ATR'] = calculate_atr(df, period=14)
    df['Chop'] = calculate_chop_index(df)
    df['ADX'] = calculate_adx(df)
    
    # 2. SMC Detection
    fvg_df = detect_fvg(df, atr_col='ATR')
    df = df.join(fvg_df)
    
    # Displacement
    disp_df = validate_displacement(df, atr_col='ATR')
    df = df.join(disp_df)
    
    # Swings needed for OB and Sweeps
    swings_df = find_swings_fractal(df) # Should ideally be adaptive, using default for now
    
    sweeps_df = detect_liquidity_sweeps(df, swing_lookback=5, atr_col='ATR')
    df = df.join(sweeps_df)
    
    ob_df = detect_order_blocks(df, fvg_df, swings_df)
    df = df.join(ob_df)
    
    # 3. Signals & Scoring
    results = df.copy()
    results['Signal'] = None
    results['Tier_Score'] = 0
    results['Score_HTF'] = 0
    results['Score_Disp'] = 0
    results['Score_Liq'] = 0
    results['Score_Context'] = 0
    results['Risk_Units'] = 0.0
    results['Entry_Price'] = np.nan
    results['Stop_Loss'] = np.nan
    results['Take_Profit'] = np.nan
    results['HTF_Bias'] = htf_bias
    
    # Killzones
    # Ensure index is datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except:
            pass 
            
    is_killzone = pd.Series([check_killzone(t) for t in df.index], index=df.index)
    results['In_Killzone'] = is_killzone
    results['Near_POI'] = False
    
    active_bull_obs = [] 
    active_bear_obs = []
    
    for i in range(len(df)):
        if i < 100: continue # Warmup
        
        row = df.iloc[i]
        
        # 1. Update Active OBs
        if row['OB_Bullish']:
            active_bull_obs.append({'top': row['High'], 'bottom': row['Low']})
        if row['OB_Bearish']:
            active_bear_obs.append({'top': row['High'], 'bottom': row['Low']})
            
        # 2. Check Near POI & Invalidation
        near_poi = False
        
        # Bullish OBs
        # Keep valid: Close >= bottom
        # Near: Low <= top
        next_active_bull = []
        for ob in active_bull_obs:
            if row['Close'] >= ob['bottom']:
                next_active_bull.append(ob)
                if row['Low'] <= ob['top']:
                    near_poi = True
        active_bull_obs = next_active_bull
        
        # Bearish OBs
        # Keep valid: Close <= top
        # Near: High >= bottom
        next_active_bear = []
        for ob in active_bear_obs:
            if row['Close'] <= ob['top']:
                next_active_bear.append(ob)
                if row['High'] >= ob['bottom']:
                    near_poi = True
        active_bear_obs = next_active_bear
        
        results.at[df.index[i], 'Near_POI'] = near_poi
        
        # Regime Filter: If CHOP > 61.8 AND ADX < 20 -> NO TRADING
        if row['Chop'] > 61.8 and row['ADX'] < 20:
            continue
            
        signal_detected = False
        direction = None
        
        # Valid MSS requires: Prior Liquidity Sweep + Valid Displacement
        # Bearish Setup:
        if row['Sweep_Bearish'] and row['Displacement_Bearish']:
            signal_detected = True
            direction = 'Short'
            
        # Bullish Setup
        elif row['Sweep_Bullish'] and row['Displacement_Bullish']:
            signal_detected = True
            direction = 'Long'
            
        if signal_detected:
            # Prepare row for scoring
            row_dict = row.to_dict()
            row_dict['Signal'] = direction
            row_dict['Near_POI'] = near_poi
            
            curr_bias = results.at[df.index[i], 'HTF_Bias']
            score, breakdown = calculate_confluence_score(row_dict, curr_bias)
            
            risk_pct = get_risk_percentage(score)
            
            if risk_pct > 0:
                results.at[df.index[i], 'Signal'] = direction
                results.at[df.index[i], 'Tier_Score'] = score
                results.at[df.index[i], 'Score_HTF'] = breakdown['Score_HTF']
                results.at[df.index[i], 'Score_Disp'] = breakdown['Score_Disp']
                results.at[df.index[i], 'Score_Liq'] = breakdown['Score_Liq']
                results.at[df.index[i], 'Score_Context'] = breakdown['Score_Context']
                
                # Execution details
                entry_price = row['Close']
                atr_val = row['ATR']
                
                if direction == 'Long':
                    stop_loss = row['Low'] - (1.5 * atr_val)
                    risk = entry_price - stop_loss
                    take_profit = entry_price + (risk * 3.0) 
                else:
                    stop_loss = row['High'] + (1.5 * atr_val)
                    risk = stop_loss - entry_price
                    take_profit = entry_price - (risk * 3.0) 
                
                dist = abs(entry_price - stop_loss)
                units = calculate_position_size(account_equity, risk_pct, dist)
                
                results.at[df.index[i], 'Entry_Price'] = entry_price
                results.at[df.index[i], 'Stop_Loss'] = stop_loss
                results.at[df.index[i], 'Take_Profit'] = take_profit
                results.at[df.index[i], 'Risk_Units'] = units

    return results
