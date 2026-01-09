import pandas as pd
import numpy as np
import os
import sys
import argparse

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
from ils.indicators import calculate_atr

def categorize_session(row):
    h = row.hour
    sessions = []
    if 0 <= h < 9:
        sessions.append('Asia')
    if 8 <= h < 17:
        sessions.append('London')
    if 13 <= h < 22:
        sessions.append('NY')
    return ",".join(sessions)

def process_data(input_file, output_dir, instrument, timeframe):
    daily_dir = os.path.join(output_dir, 'daily')
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(daily_dir, exist_ok=True)
    
    print(f"Reading {input_file}...")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    print("Processing timestamps...")
    # Assumes timestamp in ms as per previous files
    # TODO: Make timestamp unit configurable if needed? 
    # For now, standardize on the known tick format
    if 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    elif 'Date' in df.columns:
         df['date'] = pd.to_datetime(df['Date'], utc=True)
    else:
        print("Error: No 'timestamp' or 'Date' column found.")
        return

    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)
    
    print("Calculating Mid Price...")
    # Handle Bid/Ask if present, else assume Close/Mid is provided
    if 'askPrice' in df.columns and 'bidPrice' in df.columns:
        df['Mid'] = (df['askPrice'] + df['bidPrice']) / 2.0
        has_bidask = True
    elif 'Close' in df.columns:
        df['Mid'] = df['Close']
        has_bidask = False
    elif 'close' in df.columns:
        df['Mid'] = df['close']
        has_bidask = False
    else:
        print("Error: Could not determine price column (Mid/Close).")
        return

    # Group by Day for processing
    grouped = df.groupby(pd.Grouper(freq='D'))
    
    total_days = len(grouped)
    print(f"Processing {total_days} days into {timeframe} bars...")
    
    for date, group in grouped:
        if group.empty:
            continue
            
        date_str = date.strftime('%Y%m%d')
        
        # 1. Daily Bar (for HTF Bias) -> Always generated
        daily_open = group['Mid'].iloc[0]
        daily_close = group['Mid'].iloc[-1]
        daily_high = group['Mid'].max()
        daily_low = group['Mid'].min()
        
        df_daily = pd.DataFrame([{
            'date': date,
            'open': daily_open,
            'high': daily_high,
            'low': daily_low,
            'close': daily_close
        }])
        
        daily_filename = os.path.join(daily_dir, f"{instrument}_daily_{date_str}.csv")
        df_daily.to_csv(daily_filename, index=False)
        
        # 2. Execution Timeframe Bars
        # Resample
        m1 = group['Mid'].resample(timeframe).agg(['first', 'max', 'min', 'last'])
        m1.columns = ['Open', 'High', 'Low', 'Close']
        
        # Volume
        vol = group['Mid'].resample(timeframe).count()
        m1['Volume'] = vol
        
        # Bid/Ask Snapshots if available
        if has_bidask:
            bid_snap = group['bidPrice'].resample(timeframe).last()
            ask_snap = group['askPrice'].resample(timeframe).last()
            m1['Bid_Snapshot'] = bid_snap
            m1['Ask_Snapshot'] = ask_snap
        
        # Drop NaNs
        m1.ffill(inplace=True)
        m1.dropna(inplace=True) 
        
        if m1.empty:
            continue
            
        # ATR
        # Ensure enough data for ATR? usually needs previous close. 
        # For daily chunks, ATR on first few bars might be NaN or inaccurate if not carrying over state.
        # Implication: Backtesting with Daily files resets ATR each day. 
        # Better approach for generic processor: Process WHOLE dataset then chunk?
        # OR accept that first 14 bars of each day are warming up? 
        # The Current runner concatenates files, so ATR is calc'd on the full series in the runner (runner calls run_strategy calls indicators).
        # Wait, run_strategy(df) calls calculate_atr(df).
        # IF we concat files in runner, then ATR is correct.
        # IF we calculate ATR here in processor, it breaks at day boundaries.
        # BUT `process_backtest_data.py` CALCULATED ATR here!
        # "m1['ATR'] = calculate_atr(m1)" inside the loop.
        # This means ATR was resetting every day. That's suboptimal but consistent with previous.
        # I will keep it for now but note that Runner recalculates it properly on full DF if it calls `run_strategy`.
        # Actually `run_strategy` DOES call `calculate_atr`. So the ATR here is redundant/diagnostic.
        
        m1['ATR'] = calculate_atr(m1)
        
        # Session
        m1['Session'] = m1.index.map(categorize_session)
        
        # Filename: INSTRUMENT_TIMEFRAME_DATE.csv
        # Normalize timeframe for filename (e.g. 1h -> 1hour)
        tf_label = timeframe.replace('h', 'hour').replace('min', 'min')
        out_filename = os.path.join(output_dir, f"{instrument}_{tf_label}_{date_str}.csv")
        m1.to_csv(out_filename)

    print("Processing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Tick Data into OHLCV Bars")
    parser.add_argument("--input", required=True, help="Path to input CSV (ticks)")
    parser.add_argument("--output", required=True, help="Output directory path")
    parser.add_argument("--instrument", required=True, help="Instrument Name (e.g. XAUUSD)")
    parser.add_argument("--timeframe", default="1h", help="Timeframe (e.g. 1min, 1h, 4h)")
    
    args = parser.parse_args()
    
    process_data(args.input, args.output, args.instrument, args.timeframe)
