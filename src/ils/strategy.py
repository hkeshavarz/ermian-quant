import pandas as pd
import numpy as np
from .indicators import calculate_atr, calculate_chop_index, find_swings_fractal
from .smc import detect_fvg, detect_liquidity_sweeps, detect_order_blocks
from .risk import get_risk_percentage, calculate_position_size

def calculate_confluence_score(row, htf_bias: str = 'Neutral') -> int:
    """
    Calculate Tiered Score (0-100) based on factors.
    """
    score = 0
    
    # Base score for Setup (Sweep + Displacement) assumption
    score += 50 
    
    # HTF Bias
    # If Signal matches Bias -> +30
    if htf_bias == 'Bullish' and row.get('Signal') == 'Long':
        score += 30
    elif htf_bias == 'Bearish' and row.get('Signal') == 'Short':
        score += 30
    
    # Bonus for not being choppy
    if 'Chop' in row and row['Chop'] < 61.8: 
        score += 15
        
    # Bonus for FVG presence (Urgency)
    if row.get('FVG_Bullish') or row.get('FVG_Bearish'):
        score += 10
    
    # Bonus for Order Block (Institutional Sponsorship)
    # If we are reacting off an OB, that's high confluence.
    if row.get('OB_Bullish') or row.get('OB_Bearish'):
        score += 15
        
    return score

def run_strategy(df: pd.DataFrame, account_equity: float = 10000.0, htf_bias = 'Neutral') -> pd.DataFrame:
    """
    Run the ILS 3.0 Strategy on a dataframe.
    htf_bias: str or pd.Series/list aligned with df index.
    """
    # 1. Indicators
    df['ATR'] = calculate_atr(df)
    df['Chop'] = calculate_chop_index(df)
    
    # 2. SMC Detection
    fvg_df = detect_fvg(df, atr_col='ATR')
    df = df.join(fvg_df)
    
    # Swings needed for OB
    swings_df = find_swings_fractal(df)
    
    ob_df = detect_order_blocks(df, fvg_df, swings_df)
    df = df.join(ob_df)
    
    sweeps_df = detect_liquidity_sweeps(df)
    df = df.join(sweeps_df)
    
    # 3. Signals & Scoring
    results = df.copy()
    results['Signal'] = None
    results['Tier_Score'] = 0
    results['Risk_Units'] = 0.0
    results['Entry_Price'] = np.nan
    results['Stop_Loss'] = np.nan
    results['Take_Profit'] = np.nan
    
    # Assign HTF Bias Column
    results['HTF_Bias'] = htf_bias
    
    for i in range(len(df)):
        if i < 50: continue # Warmup
        
        row = df.iloc[i]
        
        # Check for Sweep Setup
        body_size = abs(row['Close'] - row['Open'])
        candle_range = row['High'] - row['Low']
        is_displacement = (body_size > 0.6 * candle_range) if candle_range > 0 else False
        
        signal_detected = False
        direction = None
        
        if row['Sweep_Bearish'] and is_displacement and row['Close'] < row['Open']:
            signal_detected = True
            direction = 'Short'
            
        elif row['Sweep_Bullish'] and is_displacement and row['Close'] > row['Open']:
            signal_detected = True
            direction = 'Long'
            
        if signal_detected:
            # Score
            # Monkey patch row for scoring
            row_dict = row.to_dict()
            row_dict['Signal'] = direction
            
            # Extract specific bias for this row
            current_bias = results.at[df.index[i], 'HTF_Bias']
            
            score = calculate_confluence_score(row_dict, current_bias)
            risk_pct = get_risk_percentage(score)
            
            if risk_pct > 0:
                results.at[df.index[i], 'Signal'] = direction
                results.at[df.index[i], 'Tier_Score'] = score
                
                # Execution details
                entry_price = row['Close']
                if direction == 'Long':
                    stop_loss = row['Low'] - row['ATR'] * 0.5
                    risk = entry_price - stop_loss
                    take_profit = entry_price + (risk * 2.0) # 2R Target
                else:
                    stop_loss = row['High'] + row['ATR'] * 0.5
                    risk = stop_loss - entry_price
                    take_profit = entry_price - (risk * 2.0) # 2R Target
                
                dist = abs(entry_price - stop_loss)
                units = calculate_position_size(account_equity, risk_pct, dist)
                
                results.at[df.index[i], 'Entry_Price'] = entry_price
                results.at[df.index[i], 'Stop_Loss'] = stop_loss
                results.at[df.index[i], 'Take_Profit'] = take_profit
                results.at[df.index[i], 'Risk_Units'] = units

    return results
