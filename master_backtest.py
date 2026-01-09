import yaml
import os
import sys
import pandas as pd
import glob

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from process_data import process_data
from backtest_runner import run_backtest_engine
from visualize_stats import generate_dashboard

def load_config(config_path="config.yml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def check_data_exists(processed_dir, timeframe="1h"):
    """
    Check if we have processed files in the directory.
    """
    # Filename convention: *_1hour_*.csv or *_1min_*.csv
    # normalize timeframe: 1h -> 1hour
    tf_label = timeframe.replace('h', 'hour').replace('min', 'min')
    pattern = os.path.join(processed_dir, f"*_{tf_label}_*.csv")
    files = glob.glob(pattern)
    return len(files) > 0

def main():
    print("=== Master Backtest Orchestrator ===")
    config = load_config()
    
    global_settings = config.get('backtest', {})
    data_settings = config.get('data', {})
    instruments = config.get('instruments', [])
    
    start_date = global_settings.get('start_date')
    end_date = global_settings.get('end_date')
    initial_balance = global_settings.get('initial_balance', 25000.0)
    base_output_dir = global_settings.get('output_base_dir', 'data/backtest_results')
    
    charts_dir = os.path.join(base_output_dir, "charts")
    
    timeframe = data_settings.get('timeframe', '1h')
    
    os.makedirs(base_output_dir, exist_ok=True)
    os.makedirs(charts_dir, exist_ok=True)
    
    summary_results = []
    
    for inst in instruments:
        symbol = inst['symbol']
        input_file = inst['input_file']
        processed_dir = inst['processed_dir']
        
        print(f"\n--- Instrument: {symbol} ---")
        
        # 1. Data Processing Check
        if not check_data_exists(processed_dir, timeframe):
            print(f"Data missing in {processed_dir}. Processing raw ticks...")
            if not os.path.exists(input_file):
                print(f"CRITICAL: Input file {input_file} not found. Skipping {symbol}.")
                continue
                
            process_data(input_file, processed_dir, symbol, timeframe)
        else:
            print(f"Data found in {processed_dir}. Skipping processing.")
            
        # 2. Run Backtest
        print(f"Running Backtest ({start_date} to {end_date})...")
        trades_df, metrics = run_backtest_engine(
            instrument=symbol,
            start_date=start_date,
            end_date=end_date,
            data_dir=processed_dir,
            initial_balance=initial_balance
        )
        
        if not trades_df.empty:
            # Save results to centralized folder
            out_csv = os.path.join(base_output_dir, f"{symbol}_trades.csv")
            trades_df.to_csv(out_csv, index=False)
            print(f"Saved trades to {out_csv}")
            
            # Generate Visualization
            print(f"Generating charts for {symbol}...")
            inst_chart_dir = os.path.join(charts_dir, symbol)
            generate_dashboard(trades_df, inst_chart_dir, initial_balance)
            
            # Add to Summary
            metrics['Instrument'] = symbol
            summary_results.append(metrics)
        else:
            print(f"No trades generated for {symbol}.")
            
    # 3. Final Report
    if summary_results:
        print("\n\n=== FINAL SUMMARY REPORT ===")
        summary_df = pd.DataFrame(summary_results)
        
        # Reorder columns usually
        cols = ['Instrument', 'Total Trades', 'Win Rate (%)', 'Total PnL ($)', 'Return (%)', 'Max Drawdown (%)', 'Sharpe Ratio']
        # Filter existing cols
        cols = [c for c in cols if c in summary_df.columns]
        
        print(summary_df[cols].to_string(index=False))
        
        summary_csv = os.path.join(base_output_dir, "summary_report.csv")
        summary_df.to_csv(summary_csv, index=False)
        print(f"\nSaved summary to {summary_csv}")
    else:
        print("\nNo results to summarize.")

if __name__ == "__main__":
    main()
