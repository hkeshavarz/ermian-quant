import yaml
import os
import sys
import pandas as pd
import glob
import shutil
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from process_data import process_data
from backtest_runner import run_backtest_engine
from visualize_stats import generate_dashboard

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
    print("=== Master Backtest Orchestrator ===")
    config = load_config()
    
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
    
    timeframe = data_settings.get('timeframe', '1h')
    
    os.makedirs(base_output_dir, exist_ok=True)
    os.makedirs(charts_dir, exist_ok=True)
    
    summary_results = []
    
    for inst in instruments:
        if not inst.get('enabled', True):
            continue

        symbol = inst['symbol']
        input_file = inst['input_file']
        processed_dir = inst['processed_dir']
        
        print(f"\n--- Instrument: {symbol} ---")
        
        # 1. Data Processing Check
        if force_process or not check_data_exists(processed_dir, start_date, end_date, timeframe):
            print(f"Data missing or incomplete in {processed_dir}. Processing source...")
            
            # Check if input_file exists (file or dir) or pattern
            has_input = False
            if os.path.exists(input_file):
                 has_input = True
            elif glob.glob(input_file):
                 has_input = True
                 
            if not has_input:
                print(f"CRITICAL: Input source {input_file} not found. Skipping {symbol}.")
                continue
                
            process_data(input_file, processed_dir, symbol, [timeframe, '1D'])
        else:
            print(f"Data found in {processed_dir}. Skipping processing.")
            
        # 2. Run Backtest
        print(f"Running Backtest ({start_date} to {end_date})...")
        trades_df, metrics = run_backtest_engine(
            instrument=symbol,
            start_date=start_date,
            end_date=end_date,
            data_dir=processed_dir,
            initial_balance=initial_balance,
            timeframe=timeframe
        )

        if not trades_df.empty:
            # Save results to centralized folder
            out_csv = os.path.join(base_output_dir, f"{symbol}_trades.csv")
            trades_df.to_csv(out_csv, index=False)
            print(f"Saved trades to {out_csv}")
            
            # Generate Visualization
            print(f"Generating charts for {symbol}...")
            inst_chart_dir = os.path.join(charts_dir, symbol)
            generate_dashboard(trades_df, inst_chart_dir, initial_balance, instrument=symbol)
        else:
            print(f"No trades generated for {symbol}.")
            # Initialize empty metrics for report
            metrics = {
                'Total Trades': 0, 
                'Win Rate (%)': 0, 'Total PnL ($)': 0, 'Return (%)': 0, 
                'Max Drawdown (%)': 0, 'Sharpe Ratio': 0,
                'Avg Score': 0, 'Tier 1 Trades': 0, 'Tier 2 Trades': 0,
                'Avg HTF': 0, 'Avg Disp': 0, 'Avg Liq': 0, 'Avg Ctxt': 0
            }
            
        # Add to Summary
        metrics['Instrument'] = symbol
        summary_results.append(metrics)
            
    # 3. Final Report
    if summary_results:
        print("\n\n=== FINAL SUMMARY REPORT ===")
        summary_df = pd.DataFrame(summary_results)
        
        # Performance Report
        perf_cols = ['Instrument', 'Total Trades', 'Win Rate (%)', 'Total PnL ($)', 'Return (%)', 'Max Drawdown (%)', 'Sharpe Ratio', 'Tier 1 Trades', 'Tier 2 Trades']
        perf_cols = [c for c in perf_cols if c in summary_df.columns]
        print(summary_df[perf_cols].to_string(index=False))
        
        summary_csv = os.path.join(base_output_dir, "summary_report.csv")
        summary_df[perf_cols].to_csv(summary_csv, index=False)
        print(f"\nSaved summary to {summary_csv}")
        
        # Detailed Analysis Report
        print("\n=== DETAILED ANALYSIS REPORT ===")
        analysis_cols = ['Instrument', 'Total Trades', 'Avg Score', 'Avg HTF', 'Avg Disp', 'Avg Liq', 'Avg Ctxt']
        # Fill NaNs with 0 just in case
        analysis_df = summary_df.fillna(0)
        analysis_cols = [c for c in analysis_cols if c in analysis_df.columns]
        
        print(analysis_df[analysis_cols].to_string(index=False))
        
        analysis_csv = os.path.join(base_output_dir, "analysis_report.csv")
        analysis_df[analysis_cols].to_csv(analysis_csv, index=False)
        print(f"\nSaved analysis to {analysis_csv}")
    else:
        print("\nNo results to summarize.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-process", action="store_true", help="Force reprocessing of data")
    args = parser.parse_args()
    
    main(force_process=args.force_process)
