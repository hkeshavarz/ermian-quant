import pandas as pd
import glob
import os
import sys
import datetime
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
from ils.strategy import run_strategy
from ils.backtest import TradeManager
from ils.metrics import calculate_metrics, generate_monthly_returns

def load_data(data_dir, instrument, start_date, end_date, timeframe="1h"):
    """
    Load and filter data files for specific timeframe.
    """
    # Pattern: INSTRUMENT_{tf_label}_YYYYMMDD.csv
    tf_label = timeframe.replace('h', 'hour').replace('min', 'min')
    pattern = os.path.join(data_dir, f"{instrument}_{tf_label}_*.csv")
    files = glob.glob(pattern)
    files.sort()
    
    loaded_dfs = []
    
    start_dt = pd.to_datetime(start_date).date()
    end_dt = pd.to_datetime(end_date).date()
    
    print(f"Scanning {len(files)} files ({timeframe}) for range {start_date} to {end_date}...")
    
    for f in files:
        basename = os.path.basename(f)
        try:
            date_part = basename.split('_')[-1].replace('.csv', '')
            file_date = datetime.datetime.strptime(date_part, '%Y%m%d').date()
            
            if start_dt <= file_date <= end_dt:
                df = pd.read_csv(f)
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    loaded_dfs.append(df)
        except Exception as e:
            continue
            
    if not loaded_dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(loaded_dfs)
    full_df = full_df[~full_df.index.duplicated(keep='first')]
    full_df.sort_index(inplace=True)
    return full_df

def load_daily_bias(data_dir, instrument):
    """
    Load daily bars and create a date->bias map.
    """
    daily_dir = os.path.join(data_dir, 'daily')
    pattern = os.path.join(daily_dir, f"{instrument}_daily_*.csv")
    files = glob.glob(pattern)
    
    bias_map = {}
    
    for f in files:
        try:
            basename = os.path.basename(f)
            date_part = basename.split('_')[-1].replace('.csv', '')
            d_date = datetime.datetime.strptime(date_part, '%Y%m%d').date()
            
            df = pd.read_csv(f)
            if not df.empty:
                # Simple Logic: Close > Open = Bullish
                d_open = df.iloc[0]['Open']
                d_close = df.iloc[0]['Close']
                bias = 'Bullish' if d_close > d_open else 'Bearish'
                bias_map[d_date] = bias
        except:
            continue
            
    return bias_map

def run_backtest_engine(instrument, start_date, end_date, data_dir, initial_balance=25000.0, timeframe="1h"):
    print(f"=== Backtest Runner: {instrument} ===")
    print(f"Range: {start_date} -> {end_date} [{timeframe}]")
    
    # 1. Load Data
    df = load_data(data_dir, instrument, start_date, end_date, timeframe)
    if df.empty:
        print("No data found for specified parameters.")
        return None
        
    print(f"Loaded {len(df)} bars.")
    
    # 2. Daily Bias
    bias_map = load_daily_bias(data_dir, instrument)
    
    # Map Bias to Bar
    # Bias for Day T is determined by Day T-1 (Previous Day)
    bias_series = []
    for idx in df.index:
        prev_day = (idx.date() - datetime.timedelta(days=1))
        b = bias_map.get(prev_day, 'Neutral')
        bias_series.append(b)
        
    # 3. Strategy Execution
    print("Running Strategy Engine...")
    results = run_strategy(df, account_equity=initial_balance, htf_bias=bias_series)
    
    # 4. Trade Simulation
    print("Simulating Trades...")
    manager = TradeManager()
    
    for timestamp, row in results.iterrows():
        manager.update(row)
        if pd.notnull(row['Signal']):
            manager.add_trade(row)
            
    trades_df = manager.get_results_df()
    
    # 5. Metrics & Output
    if not trades_df.empty:
        print("\n--- Performance Metrics ---")
        metrics = calculate_metrics(trades_df, initial_balance)
        for k, v in metrics.items():
            print(f"{k}: {v}")
            
        # Monthly Returns
        print("\n--- Monthly Returns ---")
        monthly = generate_monthly_returns(trades_df)
        print(monthly)
        
        # Return results for master script usage
        return trades_df, metrics
        
    else:
        print("No trades executed.")
        return pd.DataFrame(), {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", help="Instrument Symbol (e.g. XAUUSD)")
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--data-dir", help="Data Directory")
    parser.add_argument("--initial-balance", type=float, help="Starting Equity")
    parser.add_argument("--config", default="config.yml", help="Path to config file")
    
    args = parser.parse_args()
    
    # Load defaults from config
    import yaml
    config = {}
    if os.path.exists(args.config):
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)
            
    # Resolve Parameters
    # Priority: CLI > Config > Hardcoded Default
    
    # helper
    def get_arg(cli_val, config_section, config_key, default_val):
        if cli_val: return cli_val
        return config.get(config_section, {}).get(config_key, default_val)
        
    backtest_cfg = config.get('backtest', {})
    
    # Instrument: CLI > first instrument in config > None
    instrument = args.instrument
    data_dir = args.data_dir
    
    if not instrument and config.get('instruments'):
        # Just pick first one for demo if not specified
        first_inst = config['instruments'][0]
        instrument = first_inst['symbol']
        if not data_dir:
            data_dir = first_inst['processed_dir']
            
    # If explicit instrument but no data_dir, try to find it in config
    if instrument and not data_dir:
        for inst in config.get('instruments', []):
            if inst['symbol'] == instrument:
                data_dir = inst['processed_dir']
                break
    
    start_date = get_arg(args.start_date, 'backtest', 'start_date', '2025-01-01')
    end_date = get_arg(args.end_date, 'backtest', 'end_date', '2025-12-31')
    initial_balance = args.initial_balance if args.initial_balance else backtest_cfg.get('initial_balance', 25000.0)
    
    if not instrument or not data_dir:
        print("Error: Instrument or Data Directory not specified and could not be inferred from config.")
        sys.exit(1)
        
    # Save CSV locally if run directly
    trades_df, metrics = run_backtest_engine(
        instrument, 
        start_date, 
        end_date, 
        data_dir, 
        initial_balance
    )
    
    if not trades_df.empty:
        out_file = f"backtest_results_{instrument}.csv"
        trades_df.to_csv(out_file, index=False)
        print(f"\nSaved trade log to {out_file}")
