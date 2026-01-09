import pandas as pd
import numpy as np

def calculate_metrics(trades_df: pd.DataFrame, initial_balance: float = 25000.0) -> dict:
    """
    Calculate comprehensive performance metrics from a trades DataFrame.
    """
    if trades_df.empty:
        return {}
        
    # Basic Stats
    total_trades = len(trades_df)
    wins = trades_df[trades_df['result'] == 'Win']
    losses = trades_df[trades_df['result'] == 'Loss']
    
    win_rate = (len(wins) / total_trades) * 100
    
    # PnL
    total_pnl = trades_df['pnl'].sum()
    final_balance = initial_balance + total_pnl
    return_pct = (total_pnl / initial_balance) * 100
    
    # Averages
    avg_win = wins['pnl'].mean() if not wins.empty else 0
    avg_loss = losses['pnl'].mean() if not losses.empty else 0
    
    # Expectancy (Average PnL per trade)
    expectancy = total_pnl / total_trades
    
    # Profit Factor
    gross_profit = wins['pnl'].sum() if not wins.empty else 0
    gross_loss = abs(losses['pnl'].sum()) if not losses.empty else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
    # Drawdown Calculation (requires equity curve)
    # Reconstruct Equity Curve from trade list (approximated at close of each trade)
    trades_df = trades_df.sort_values('exit_time')
    trades_df['equity'] = initial_balance + trades_df['pnl'].cumsum()
    trades_df['peak'] = trades_df['equity'].cummax()
    trades_df['drawdown'] = trades_df['equity'] - trades_df['peak']
    trades_df['drawdown_pct'] = (trades_df['drawdown'] / trades_df['peak']) * 100
    
    max_drawdown = trades_df['drawdown_pct'].min() # Negative value
    
    # Sharpe Ratio (Simplistic: based on trade returns, better to use daily returns)
    # Let's try to infer daily returns if possible, or just trade-based Sharpe
    # Trade-based Sharpe: Mean(Trade Returns) / Std(Trade Returns) * Sqrt(N)
    # Where N is roughly trades per year?
    # Better: Resample equity curve to daily.
    
    # Resampling Equity Curve to Daily
    # Map trade exit times to days
    trades_df['exit_date'] = pd.to_datetime(trades_df['exit_time']).dt.date
    daily_pnl = trades_df.groupby('exit_date')['pnl'].sum()
    
    # We need a continuous daily series for correct Sharpe
    idx = pd.date_range(trades_df['exit_date'].min(), trades_df['exit_date'].max())
    daily_pnl_series = daily_pnl.reindex(idx, fill_value=0)
    
    daily_returns = daily_pnl_series / initial_balance # Simple return on initial capital
    
    mean_daily_ret = daily_returns.mean()
    std_daily_ret = daily_returns.std()
    
    sharpe_ratio = 0
    if std_daily_ret > 0:
        sharpe_ratio = (mean_daily_ret / std_daily_ret) * np.sqrt(252)
        
    # Tier Stats
    avg_score = trades_df['tier_score'].mean() if 'tier_score' in trades_df.columns else 0
    t1_count = len(trades_df[trades_df['tier_type'] == 'Tier 1']) if 'tier_type' in trades_df.columns else 0
    t2_count = len(trades_df[trades_df['tier_type'] == 'Tier 2']) if 'tier_type' in trades_df.columns else 0
    
    avg_htf = trades_df['score_htf'].mean() if 'score_htf' in trades_df.columns else 0
    avg_disp = trades_df['score_disp'].mean() if 'score_disp' in trades_df.columns else 0
    avg_liq = trades_df['score_liq'].mean() if 'score_liq' in trades_df.columns else 0
    avg_ctxt = trades_df['score_ctxt'].mean() if 'score_ctxt' in trades_df.columns else 0
        
    metrics = {
        'Total Trades': total_trades,
        'Win Rate (%)': round(win_rate, 2),
        'Total PnL ($)': round(total_pnl, 2),
        'Return (%)': round(return_pct, 2),
        'Profit Factor': round(profit_factor, 2),
        'Max Drawdown (%)': round(max_drawdown, 2),
        'Sharpe Ratio': round(sharpe_ratio, 2),
        'Expectancy ($)': round(expectancy, 2),
        'Avg Win ($)': round(avg_win, 2),
        'Avg Loss ($)': round(avg_loss, 2),
        'Avg Score': round(avg_score, 1),
        'Tier 1 Trades': t1_count,
        'Tier 2 Trades': t2_count,
        'Avg HTF': round(avg_htf, 1),
        'Avg Disp': round(avg_disp, 1),
        'Avg Liq': round(avg_liq, 1),
        'Avg Ctxt': round(avg_ctxt, 1)
    }
    
    return metrics

def generate_monthly_returns(trades_df):
    """
    Generate a pivot table of monthly returns.
    """
    if trades_df.empty:
        return pd.DataFrame()
        
    trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
    trades_df['Year'] = trades_df['exit_time'].dt.year
    trades_df['Month'] = trades_df['exit_time'].dt.month
    
    monthly = trades_df.groupby(['Year', 'Month'])['pnl'].sum().reset_index()
    
    pivot = monthly.pivot(index='Year', columns='Month', values='pnl')
    pivot = pivot.fillna(0)
    
    # Add Total column
    pivot['Total'] = pivot.sum(axis=1)
    
    return pivot
