import pandas as pd
import numpy as np
import os
import sys
import argparse

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

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

def process_data(input_file, output_dir, instrument, timeframes):
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Reading {input_file}...")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    print("Processing timestamps...")
    if 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    elif 'Date' in df.columns:
         df['date'] = pd.to_datetime(df['Date'], utc=True)
    else:
        print("Error: No 'timestamp' or 'Date' column found.")
        return

    # De-duplication (Keep last) - Rule 16.3
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)

    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)
    
    print("Validating and cleaning ticks...")
    # Rule 16.3: Drop Non-positive quotes and Inverted Spreads
    # Required: askPrice, bidPrice
    
    if 'askPrice' in df.columns and 'bidPrice' in df.columns:
        # 1. Drop <= 0
        df = df[(df['bidPrice'] > 0) & (df['askPrice'] > 0)]
        
        # 2. Drop Inverted
        df = df[df['askPrice'] >= df['bidPrice']]
        
        # 3. Calculate Mid
        df['Mid'] = (df['askPrice'] + df['bidPrice']) / 2.0
        
        # 4. Calculate Spread (for aggregation)
        df['Spread'] = df['askPrice'] - df['bidPrice']
        
    elif 'Close' in df.columns:
        # Fallback (Simulated)
        df['Mid'] = df['Close']
        df['askPrice'] = df['Close']
        df['bidPrice'] = df['Close']
        df['Spread'] = 0.0 # Synthetic? Runner handles synthetic.
    else:
        print("Error: Could not determine price columns.")
        return
        
    # Volume calculation
    if 'askVolume' in df.columns and 'bidVolume' in df.columns:
        df['Volume'] = df['askVolume'] + df['bidVolume']
    elif 'Volume' in df.columns:
        pass
    else:
        df['Volume'] = 1.0 # Proxy for sum

    # Group by Day
    grouped = df.groupby(pd.Grouper(freq='D'))
    
    total_days = len(grouped)
    print(f"Processing {total_days} days into {timeframes}...")
    
    for date, group in grouped:
        if group.empty:
            continue
            
        date_str = date.strftime('%Y%m%d')
        
        for tf in timeframes:
            # Aggregation - Rule 16.4 & 16.8
            agg_dict = {
                'Mid': ['first', 'max', 'min', 'last', 'count'], # count = Tick_count
                'bidPrice': ['first', 'max', 'min', 'last'],
                'askPrice': ['first', 'max', 'min', 'last'],
                'Volume': 'sum',
                'Spread': 'mean' # Spread_avg
            }
            
            resampled = group.resample(tf).agg(agg_dict)
            
            # Extract Columns
            m_open = resampled[('Mid', 'first')]
            m_high = resampled[('Mid', 'max')]
            m_low = resampled[('Mid', 'min')]
            m_close = resampled[('Mid', 'last')]
            tick_count = resampled[('Mid', 'count')]
            
            b_open = resampled[('bidPrice', 'first')]
            b_high = resampled[('bidPrice', 'max')]
            b_low = resampled[('bidPrice', 'min')]
            b_close = resampled[('bidPrice', 'last')]
            
            a_open = resampled[('askPrice', 'first')]
            a_high = resampled[('askPrice', 'max')]
            a_low = resampled[('askPrice', 'min')]
            a_close = resampled[('askPrice', 'last')]
            
            vol = resampled[('Volume', 'sum')]
            spread_avg = resampled[('Spread', 'mean')]
            
            out_df = pd.DataFrame({
                'Open': m_open,
                'High': m_high,
                'Low': m_low,
                'Close': m_close,
                'Volume': vol,
                'Tick_Count': tick_count,
                'Spread_Avg': spread_avg,
                'Bid_Open': b_open,
                'Bid_High': b_high,
                'Bid_Low': b_low,
                'Bid_Close': b_close,
                'Ask_Open': a_open,
                'Ask_High': a_high,
                'Ask_Low': a_low,
                'Ask_Close': a_close
            })
            
            # NaNs (Empty Bars)
            out_df.dropna(subset=['Open'], inplace=True) # Drop if no trades
            
            if out_df.empty:
                continue
                
            if tf not in ['1D', '1W']:
                out_df['Session'] = out_df.index.map(categorize_session)
            else:
                out_df['Session'] = 'Daily'
            
            # Filename
            tf_label = tf.replace('h', 'hour').replace('min', 'min')
            if tf == '1D': tf_label = 'daily'
            
            out_filename = os.path.join(output_dir, f"{instrument}_{tf_label}_{date_str}.csv")
            out_df.to_csv(out_filename)

    print("Processing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Tick Data into OHLCV Bars")
    parser.add_argument("--input", required=True, help="Path to input CSV (ticks)")
    parser.add_argument("--output", required=True, help="Output directory path")
    parser.add_argument("--instrument", required=True, help="Instrument Name (e.g. XAUUSD)")
    parser.add_argument("--timeframes", default="5min,15min,1h,4h,1D", help="Comma-separated timeframes (e.g. 15min,1h)")
    
    args = parser.parse_args()
    
    tf_list = [t.strip() for t in args.timeframes.split(',')]
    
    process_data(args.input, args.output, args.instrument, tf_list)
