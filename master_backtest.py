import yaml
import os
import sys
import pandas as pd
import glob
import shutil
from datetime import datetime
import datetime as dt

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from process_data import process_data
from backtest_runner import load_data, load_daily_bias
from visualize_stats import generate_dashboard
from src.ils.portfolio import PortfolioManager
from src.ils.strategy import run_strategy
from src.ils.metrics import calculate_metrics

def load_config(config_path="config.yml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def check_data_exists(processed_dir, start_date, end_date, timeframe="1h"):
    """
    Check if processed files exist covering the requested date range with valid schema.
    """
    tf_label = timeframe.replace('h', 'hour').replace('min', 'min')
    pattern = os.path.join(processed_dir, f"*_{tf_label}_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        return False
        
    # Check Coverage
    dates = []
    for f in files:
        try:
            bn = os.path.basename(f)
            # Expect INSTRUMENT_TF_YYYYMMDD.csv
            d_str = bn.split('_')[-1].replace('.csv', '')
            d = pd.to_datetime(d_str).date()
            dates.append(d)
        except:
            continue
            
    if not dates:
        return False
        
    min_found = min(dates)
    max_found = max(dates)
    
    req_start = pd.to_datetime(start_date).date()
    req_end = pd.to_datetime(end_date).date()
    
    if req_start < min_found or req_end > max_found:
        print(f"Coverage Gap: Found {min_found} to {max_found}. Need {req_start} to {req_end}.")
        return False
        
    # Validation: Check newest file for schema
    files.sort()
    try:
        df = pd.read_csv(files[-1], nrows=1)
        if 'Bid_Open' not in df.columns:
            print(f"Data in {processed_dir} is outdated (missing Bid/Ask). Reprocessing...")
            return False
            if 'Volume' not in df.columns:
                return False
    except:
        return False
        
    return True

def archive_previous_results(output_dir):
    """
    Move existing results to a timestamped archive folder.
    """
    if not os.path.exists(output_dir):
        return
        
    # Check if there's anything to archive
    items = os.listdir(output_dir)
    if not items:
        return
        
    # We only want to archive actual result files, not the 'archive' folder itself if it exists
    items_to_move = [
        i for i in items 
        if i != "archive" and (i.endswith(".csv") or i == "charts")
    ]
    
    if not items_to_move:
        return
        
    # Create timestamped directory inside 'archive'
    ts = datetime.now().strftime("%Y%m%d%H%M")
    archive_root = os.path.join(output_dir, "archive")
    target_dir = os.path.join(archive_root, ts)
    
    print(f"Archiving previous results to {target_dir}...")
    os.makedirs(target_dir, exist_ok=True)
    
    for item in items_to_move:
        src = os.path.join(output_dir, item)
        dst = os.path.join(target_dir, item)
        try:
            shutil.move(src, dst)
        except Exception as e:
            print(f"Failed to move {item}: {e}")

def main(force_process=False):
    print("=== Master Backtest Orchestrator (Portfolio Mode) ===")
    config = load_config()
    
    # Phase 8: Environment & Timeframe
    env = config.get('environment', 'backtest')
    print(f"Environment: {env.upper()}")
    
    global_settings = config.get('backtest', {})
    data_settings = config.get('data', {})
    instruments = config.get('instruments', [])
    
    start_date = global_settings.get('start_date')
    end_date = global_settings.get('end_date')
    initial_balance = global_settings.get('initial_balance', 25000.0)
    base_output_dir = global_settings.get('output_base_dir', 'data/backtest_results')
    
    # Archive previous runs before starting new one
    archive_previous_results(base_output_dir)
    
    charts_dir = os.path.join(base_output_dir, "charts")
    
    # Phase 8: Execution Timeframe
    tf_list = config.get('timeframes', {}).get('execution', [])
    if tf_list:
        timeframe = tf_list[0]
        print(f"Using Primary Execution Timeframe: {timeframe}")
    else:
        timeframe = data_settings.get('timeframe', '1h')
    
    os.makedirs(base_output_dir, exist_ok=True)
    os.makedirs(charts_dir, exist_ok=True)
    
    # --- PHASE 1: Data Loading & Signal Generation ---
    print("\n--- Phase 1: Signal Gathering ---")
    market_data = {} # {symbol: df}
    daily_biases = {} # {symbol: map}
    all_signals = [] 
    
    for inst in instruments:
        if not inst.get('enabled', True):
            continue

        symbol = inst['symbol']
        input_file = inst['input_file']
        processed_dir = inst['processed_dir']
        
        print(f"Processing {symbol}...")
        
        # 1. Check Data
        if force_process or not check_data_exists(processed_dir, start_date, end_date, timeframe):
             print(f"Data gap for {symbol}, processing source...")
             has_input = False
             if os.path.exists(input_file) or glob.glob(input_file):
                  has_input = True
             if not has_input:
                print(f"CRITICAL: Input source {input_file} not found. Skipping {symbol}.")
                continue
             process_data(input_file, processed_dir, symbol, [timeframe, '1D'])

        # 2. Load Data
        df = load_data(processed_dir, symbol, start_date, end_date, timeframe)
        if df.empty:
            print(f"No data loaded for {symbol}")
            continue
            
        # 3. Load Bias
        bias_map = load_daily_bias(processed_dir, symbol)
        bias_series = []
        for idx in df.index:
            prev_day = (idx.date() - dt.timedelta(days=1))
            b = bias_map.get(prev_day, 'Neutral')
            bias_series.append(b)

        # 4. Generate Raw Signals
        # Note: We pass default equity because sizing is ignored here
        print(f"Generating signals for {symbol}...")
        results_df = run_strategy(df, account_equity=initial_balance, htf_bias=bias_series, config=config)
        
        # Store Market Data (needed for portfolio simulation)
        market_data[symbol] = results_df # Contains OHLC + Indicators
        
        # Collect Candidates
        signals = results_df[results_df['Signal'].notnull()]
        for ts, row in signals.iterrows():
            sig = row.to_dict()
            sig['Symbol'] = symbol
            sig['Timestamp'] = ts
            all_signals.append(sig)
            
    # Sort signals by timestamp
    all_signals.sort(key=lambda x: x['Timestamp'])
    print(f"Total Candidates Found: {len(all_signals)}")
    
    # --- PHASE 2: Portfolio Simulation ---
    print("\n--- Phase 2: Portfolio Simulation ---")
    
    if not market_data:
        print("No market data available using enabled instruments.")
        return

    # Master Timeline: Union of all indices
    print("Building Master Timeline...")
    all_indices = pd.DatetimeIndex([])
    for df in market_data.values():
        all_indices = all_indices.union(df.index)
    all_indices = all_indices.sort_values()
    
    pm = PortfolioManager(initial_balance, risk_config=config.get('risk', {}), limits_config=config.get('trading_limits', {}))
    
    sig_idx = 0
    num_signals = len(all_signals)
    count_processed = 0
    
    # Optimization: Iterate signals or Iterate Time?
    # Must iterate Time to capture Exits accurately.
    # 300k steps is fine.
    
    print(f"Simulating {len(all_indices)} time steps...")
    
    for current_time in all_indices:
        # A. Get Price Snapshot
        current_prices = {}
        for sym, df in market_data.items():
            if current_time in df.index:
                # Need Bid/Ask High/Low for strict checking
                # Assuming df has 'Bid_Low' etc if processed, or falling back to OHLC
                row = df.loc[current_time]
                # Construct price dict
                current_prices[sym] = row
        
        if not current_prices: continue
            
        # B. Mark to Market & Exits
        pm.update_mark_to_market(current_prices)
        pm.process_market_update(current_time, current_prices)
        
        # C. Process New Signals
        while sig_idx < num_signals and all_signals[sig_idx]['Timestamp'] == current_time:
            candidate = all_signals[sig_idx]
            sig_idx += 1
            
            # Use Portfolio Manager to Size & Check Correlation
            units = pm.size_candidate(candidate)
            if units > 0:
                pm.execute_trade(candidate, units, current_time)
                
        if count_processed % 10000 == 0:
            print(f"\rProgress: {current_time} | Equity: ${pm.current_equity:.2f}", end="")
        count_processed += 1
        
    print(f"\nSimulation Complete. Final Equity: ${pm.current_equity:.2f}")
    
    # --- PHASE 3: Reporting ---
    print("\n--- Phase 3: Reporting ---")
    
    trades = pm.closed_trades
    all_trades_df = pd.DataFrame(trades)
    
    summary_results = []
    
    if not all_trades_df.empty:
        # Split by Instrument for Individual Reporting
        instruments_traded = all_trades_df['symbol'].unique()
        
        for symbol in instruments_traded:
            inst_trades = all_trades_df[all_trades_df['symbol'] == symbol]
            
            # Save CSV
            out_csv = os.path.join(base_output_dir, f"{symbol}_trades.csv")
            inst_trades.to_csv(out_csv, index=False)
            
            # Metrics
            # Note: Initial Balance for individual metrics is tricky in portfolio.
            # We use prorated or just reuse global? Reusing global distorts %.
            # We'll use 0 for "Allocated" or just display PnL.
            metrics = calculate_metrics(inst_trades, initial_balance) # Rel to global equity
            metrics['Instrument'] = symbol
            summary_results.append(metrics)
            
            # Charts
            inst_chart_dir = os.path.join(charts_dir, symbol)
            # generate_dashboard requires dataframe with 'exit_time', 'pnl'
            # our trade dicts match
            print(f"Generating charts for {symbol}...")
            # We pass initial_balance=None so it doesn't show huge curve flatline?
            # Or we pass a nominal amount.
            generate_dashboard(inst_trades, inst_chart_dir, initial_balance, instrument=symbol)

        # Portfolio Summary
        print("\n=== FINAL SUMMARY REPORT ===")
        summary_df = pd.DataFrame(summary_results)
        
        perf_cols = ['Instrument', 'Total Trades', 'Win Rate (%)', 'Total PnL ($)', 'Return (%)', 'Max Drawdown (%)', 'Sharpe Ratio']
        perf_cols = [c for c in perf_cols if c in summary_df.columns]
        print(summary_df[perf_cols].to_string(index=False))
        
        summary_csv = os.path.join(base_output_dir, "summary_report.csv")
        summary_df[perf_cols].to_csv(summary_csv, index=False)
        
    else:
        print("No trades executed in portfolio simulation.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-process", action="store_true", help="Force reprocessing of data")
    args = parser.parse_args()
    
    main(force_process=args.force_process)
