import pandas as pd
import numpy as np
from datetime import time
from .indicators import calculate_atr, calculate_chop_index, find_swings_fractal, calculate_adx, find_swings_adaptive
from .smc import detect_fvg, detect_liquidity_sweeps, detect_order_blocks, validate_displacement, validate_mss
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

def calculate_confluence_score(row, htf_bias: str = 'Neutral', scoring_config: dict = None):
    """
    Calculate Tiered Score (0-100) based on Configurable weights.
    Returns (score, breakdown_dict).
    """
    if scoring_config is None:
        scoring_config = {
            'htf_alignment': 25, 'htf_poi': 15, 'displacement': 10,
            'fvg_clean': 10, 'liquidity_sweep': 15, 'killzone': 10, 'chop_filter': 5
        }
        
    score = 0
    breakdown = {'Score_HTF': 0, 'Score_Disp': 0, 'Score_Liq': 0, 'Score_Context': 0}
    
    # 1. HTF Alignment
    w_htf = scoring_config.get('htf_alignment', 25)
    if htf_bias == 'Bullish' and row.get('Signal') == 'Long':
        score += w_htf
        breakdown['Score_HTF'] += w_htf
    elif htf_bias == 'Bearish' and row.get('Signal') == 'Short':
        score += w_htf
        breakdown['Score_HTF'] += w_htf
        
    # HTF POI
    w_poi = scoring_config.get('htf_poi', 15)
    if row.get('Near_POI', False):
        score += w_poi
        breakdown['Score_HTF'] += w_poi
    
    # 2. Displacement
    w_disp = scoring_config.get('displacement', 10)
    score += w_disp
    breakdown['Score_Disp'] += w_disp
    
    # Clean FVG
    w_fvg = scoring_config.get('fvg_clean', 10)
    if row.get('Signal') == 'Long' and row.get('FVG_Bullish'):
        score += w_fvg
        breakdown['Score_Disp'] += w_fvg
    elif row.get('Signal') == 'Short' and row.get('FVG_Bearish'):
        score += w_fvg
        breakdown['Score_Disp'] += w_fvg
    
    # 3. Liquidity
    w_sweep = scoring_config.get('liquidity_sweep', 15)
    if row.get('Sweep_Bullish') or row.get('Sweep_Bearish'):
        score += w_sweep
        breakdown['Score_Liq'] += w_sweep

    # 4. Context
    w_kz = scoring_config.get('killzone', 10)
    if row.get('In_Killzone', False):
        score += w_kz
        breakdown['Score_Context'] += w_kz
    
    # CHOP Filter
    w_chop = scoring_config.get('chop_filter', 5)
    if 'Chop' in row and row['Chop'] < 50:
        score += w_chop
        breakdown['Score_Context'] += w_chop
        
    return score, breakdown

def run_strategy(df: pd.DataFrame, account_equity: float = 10000.0, htf_bias = 'Neutral', config: dict = None) -> pd.DataFrame:
    """
    Run the ILS 3.0 Strategy on a dataframe using Configurable Parameters.
    """
    if config is None:
        config = {}
        
    # Extract configs
    ind_cfg = config.get('indicators', {})
    smc_cfg = config.get('smc', {})
    adapt_cfg = config.get('adaptive', {})
    score_cfg = config.get('scoring', {})
    risk_cfg = config.get('risk', {})
    
    # 1. Indicators
    atr_fast_p = ind_cfg.get('atr_fast', 14)
    atr_slow_p = ind_cfg.get('atr_slow', 100)
    chop_p = ind_cfg.get('chop_period', 14)
    adx_p = ind_cfg.get('adx_period', 14)
    
    df['ATR'] = calculate_atr(df, period=atr_fast_p)
    df['ATR_100'] = calculate_atr(df, period=atr_slow_p)
    df['Chop'] = calculate_chop_index(df, period=chop_p)
    df['ADX'] = calculate_adx(df, period=adx_p)
    
    # 2. SMC Detection
    fvg_df = detect_fvg(df, atr_threshold_multiplier=smc_cfg.get('fvg_threshold', 0.5), atr_col='ATR')
    df = df.join(fvg_df)
    
    # Displacement
    disp_df = validate_displacement(df, atr_col='ATR', 
                                    body_ratio_min=smc_cfg.get('disp_body_ratio', 0.6),
                                    range_atr_min=smc_cfg.get('disp_range_atr', 1.5))
    df = df.join(disp_df)
    
    # Adaptive Swings (Section 8)
    base_lb = adapt_cfg.get('base_lookback', 5)
    min_lb = adapt_cfg.get('min_lookback', 3)
    max_lb = adapt_cfg.get('max_lookback', 10)
    
    # User override via swing_lookback
    sl_cfg = adapt_cfg.get('swing_lookback', {})
    if sl_cfg:
        base_lb = sl_cfg.get('htf', base_lb)
        min_lb = sl_cfg.get('ltf', min_lb)
    
    atr_ratio = df['ATR'] / df['ATR_100'].replace(0, np.nan)
    lookback_series = (base_lb * atr_ratio).fillna(base_lb).round()
    lookback_series = lookback_series.clip(lower=min_lb, upper=max_lb)
    df['L_Adaptive'] = lookback_series
    
    swings_df = find_swings_adaptive(df, lookback_series) 
    
    # Sweeps
    sweeps_df = detect_liquidity_sweeps(df, swings_df, atr_col='ATR', 
                                        sweep_atr_tolerance=smc_cfg.get('sweep_atr', 0.2))
    df = df.join(sweeps_df)
    
    # MSS Validation
    mss_df = validate_mss(df, swings_df)
    df = df.join(mss_df)
    
    ob_df = detect_order_blocks(df, fvg_df, swings_df, volume_factor=smc_cfg.get('ob_volume_factor', 1.0))
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
    
    active_bull_fvgs = [] 
    active_bear_fvgs = []
    
    chop_regime_thresh = score_cfg.get('chop_regime_threshold', 61.8)
    adx_regime_thresh = score_cfg.get('adx_regime_threshold', 20)
    
    for i in range(len(df)):
        if i < 100: continue
        
        row = df.iloc[i]
        
        # 1. Update Active OBs
        if row['OB_Bullish']:
            active_bull_obs.append({'top': row['High'], 'bottom': row['Low']})
        if row['OB_Bearish']:
            active_bear_obs.append({'top': row['High'], 'bottom': row['Low']})
            
        # 2. Update Active FVGs
        if row['FVG_Bullish']:
            active_bull_fvgs.append({'top': row['FVG_Top'], 'bottom': row['FVG_Bottom']})
        if row['FVG_Bearish']:
            active_bear_fvgs.append({'top': row['FVG_Top'], 'bottom': row['FVG_Bottom']})
            
        # Filter invalidated FVGs
        active_bull_fvgs = [f for f in active_bull_fvgs if row['Close'] >= f['bottom']]
        active_bear_fvgs = [f for f in active_bear_fvgs if row['Close'] <= f['top']]

        # 3. Check Near POI
        near_poi = False
        
        # Bullish OBs
        next_active_bull = []
        for ob in active_bull_obs:
            if row['Close'] >= ob['bottom']: 
                next_active_bull.append(ob)
                if row['Low'] <= ob['top']:
                    near_poi = True
        active_bull_obs = next_active_bull
        
        # Bearish OBs
        next_active_bear = []
        for ob in active_bear_obs:
            if row['Close'] <= ob['top']:
                next_active_bear.append(ob)
                if row['High'] >= ob['bottom']:
                    near_poi = True
        active_bear_obs = next_active_bear
        
        results.at[df.index[i], 'Near_POI'] = near_poi
        
        # Regime Filter
        # If High Chop AND Low ADX -> Skip
        if row['Chop'] > chop_regime_thresh and row['ADX'] < adx_regime_thresh:
            continue
            
        signal_detected = False
        direction = None
        
        # New Logic: Sweep -> Displacement -> MSS
        
        # SMC Toggles
        choch_req = smc_cfg.get('choch_required', True)
        
        # Check MSS (CHoCH)
        has_mss_bear = row['MSS_Bearish'] if choch_req else True
        has_mss_bull = row['MSS_Bullish'] if choch_req else True
        
        if row['Displacement_Bearish'] and has_mss_bear:
            lookback = 10
            if df['Sweep_Bearish'].iloc[max(0, i-lookback):i+1].any():
                signal_detected = True
                direction = 'Short'
                
        elif row['Displacement_Bullish'] and has_mss_bull:
            lookback = 10
            if df['Sweep_Bullish'].iloc[max(0, i-lookback):i+1].any():
                 signal_detected = True
                 direction = 'Long'
            
        if signal_detected:
            row_dict = row.to_dict()
            row_dict['Signal'] = direction
            row_dict['Near_POI'] = near_poi
            
            curr_bias = results.at[df.index[i], 'HTF_Bias']
            score, breakdown = calculate_confluence_score(row_dict, curr_bias, scoring_config=score_cfg)
            
            risk_pct = get_risk_percentage(score, risk_config=risk_cfg)
            
            if risk_pct > 0:
                results.at[df.index[i], 'Signal'] = direction
                results.at[df.index[i], 'Tier_Score'] = score
                results.at[df.index[i], 'Score_HTF'] = breakdown['Score_HTF']
                results.at[df.index[i], 'Score_Disp'] = breakdown['Score_Disp']
                results.at[df.index[i], 'Score_Liq'] = breakdown['Score_Liq']
                results.at[df.index[i], 'Score_Context'] = breakdown['Score_Context']
                
                # Execution
                entry_price = row['Close']
                atr_val = row['ATR']
                
                if direction == 'Long':
                    stop_loss = row['Low'] - (1.5 * atr_val)
                else:
                    stop_loss = row['High'] + (1.5 * atr_val)
                
                dist = abs(entry_price - stop_loss)
                # Risk Units
                units = calculate_position_size(account_equity, risk_pct, dist)
                risk_amt = dist * units # Approx risk amount
                
                # TP
                if direction == 'Long':
                     take_profit = entry_price + (3.0 * dist)
                else:
                     take_profit = entry_price - (3.0 * dist)
                     
                results.at[df.index[i], 'Entry_Price'] = entry_price
                results.at[df.index[i], 'Stop_Loss'] = stop_loss
                results.at[df.index[i], 'Take_Profit'] = take_profit
                results.at[df.index[i], 'Risk_Units'] = units

    return results
