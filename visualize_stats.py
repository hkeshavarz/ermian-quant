import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import sys
import os
import yaml

def load_config(config_path="config.yml"):
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    return {}

def plot_equity_curve(trades_df, initial_balance=25000.0, output_file='equity_curve.png', instrument=None):
    trades_df = trades_df.sort_values('exit_time')
    trades_df['equity'] = initial_balance + trades_df['pnl'].cumsum()
    
    # Create a time series with start point
    equity_series = trades_df.set_index('exit_time')['equity']
    
    # Add initial point
    start_date = pd.to_datetime(trades_df['exit_time'].iloc[0]) - pd.Timedelta(days=1)
    equity_series[start_date] = initial_balance
    equity_series = equity_series.sort_index()
    
    plt.figure(figsize=(12, 6))
    plt.plot(equity_series, label='Equity')
    
    title = f'Account Equity Curve - {instrument}' if instrument else 'Account Equity Curve'
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Balance ($)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Saved {output_file}")
    plt.close()

def plot_monthly_heatmap(trades_df, output_file='monthly_heatmap.png', instrument=None):
    trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
    trades_df['Year'] = trades_df['exit_time'].dt.year
    trades_df['Month'] = trades_df['exit_time'].dt.month
    
    monthly = trades_df.groupby(['Year', 'Month'])['pnl'].sum().reset_index()
    pivot = monthly.pivot(index='Year', columns='Month', values='pnl')
    pivot = pivot.fillna(0)
    
    plt.figure(figsize=(10, 6))
    try:
        sns.heatmap(pivot, annot=True, fmt='.0f', cmap='RdYlGn', center=0)
    except Exception as e:
        print(f"Heatmap error: {e}")
        
    title = f'Monthly PnL Heatmap ($) - {instrument}' if instrument else 'Monthly PnL Heatmap ($)'
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Saved {output_file}")
    plt.close()

def plot_drawdown(trades_df, initial_balance=25000.0, output_file='drawdown.png', instrument=None):
    trades_df = trades_df.sort_values('exit_time')
    trades_df['equity'] = initial_balance + trades_df['pnl'].cumsum()
    trades_df['peak'] = trades_df['equity'].cummax()
    trades_df['drawdown_pct'] = ((trades_df['equity'] - trades_df['peak']) / trades_df['peak']) * 100
    
    plt.figure(figsize=(12, 4))
    plt.fill_between(trades_df['exit_time'], trades_df['drawdown_pct'], 0, color='red', alpha=0.3)
    plt.plot(trades_df['exit_time'], trades_df['drawdown_pct'], color='red', linewidth=1)
    
    title = f'Drawdown (%) - {instrument}' if instrument else 'Drawdown (%)'
    plt.title(title)
    plt.ylabel('Drawdown %')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Saved {output_file}")
    plt.close()

def generate_dashboard(trades_df, output_dir, initial_balance=None, instrument=None):
    """
    Generate all charts for a set of trades.
    """
    if trades_df.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    
    # Try to load config if balance not provided
    if initial_balance is None:
        config = load_config()
        initial_balance = config.get('backtest', {}).get('initial_balance', 25000.0)
    
    trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
    
    plot_equity_curve(trades_df, initial_balance, os.path.join(output_dir, 'equity_curve.png'), instrument=instrument)
    plot_drawdown(trades_df, initial_balance, os.path.join(output_dir, 'drawdown.png'), instrument=instrument)
    plot_monthly_heatmap(trades_df, os.path.join(output_dir, 'monthly_heatmap.png'), instrument=instrument)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_file", help="Path to backtest_results.csv")
    parser.add_argument("--config", default="config.yml", help="Path to config file")
    parser.add_argument("--output", help="Output directory for charts")
    args = parser.parse_args()
    
    if not os.path.exists(args.results_file):
        print("File not found.")
        return
        
    df = pd.read_csv(args.results_file)
    if df.empty:
        print("Empty results file.")
        return
        
    # Load config defaults
    config = load_config(args.config)
    initial_balance = config.get('backtest', {}).get('initial_balance', 25000.0)
    
    # Output dir defaults to same as results file if not specified
    output_dir = args.output if args.output else os.path.dirname(args.results_file)
    
    generate_dashboard(df, output_dir, initial_balance)

if __name__ == "__main__":
    main()
