import pandas as pd
import numpy as np

def calculate_metrics(trades_df, initial_balance=25000.0):
    """
    Calculate performance metrics from a DataFrame of trades.
    """
    if trades_df.empty:
        return {
            'Total Trades': 0, 'Win Rate (%)': 0.0, 'Total PnL ($)': 0.0,
            'Return (%)': 0.0, 'Max Drawdown (%)': 0.0, 'Sharpe Ratio': 0.0,
            'Profit Factor': 0.0, 'Avg Score': 0.0, 'Avg HTF': 0.0,
            'Avg Disp': 0.0, 'Avg Liq': 0.0, 'Avg Ctxt': 0.0,
            'Tier 1 Trades': 0, 'Tier 2 Trades': 0
        }
        
    # Basic Stats
    total_trades = len(trades_df)
    wins = trades_df[trades_df['result'] == 'Win']
    losses = trades_df[trades_df['result'] == 'Loss']
    
    win_rate = (len(wins) / total_trades) * 100
    total_pnl = trades_df['pnl'].sum()
    ret_pct = (total_pnl / initial_balance) * 100
    
    # Drawdown
    equity_curve = initial_balance + trades_df['pnl'].cumsum()
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak * 100
    max_dd = drawdown.min()
    
    # Profit Factor
    gross_profit = wins['pnl'].sum()
    gross_loss = abs(losses['pnl'].sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.inf
    
    # Sharpe (Simplified)
    returns = trades_df['pnl']
    if returns.std() != 0:
        sharpe = (returns.mean() / returns.std()) * np.sqrt(len(trades_df))
    else:
        sharpe = 0.0
        
    # --- SCORING ANALYSIS FIX ---
    # Helper to find column case-insensitively
    def get_col_mean(df, targets):
        cols_lower = [c.lower() for c in df.columns]
        for t in targets:
            # 1. Exact match
            if t in df.columns:
                return df[t].mean()
            # 2. Case-insensitive match
            if t.lower() in cols_lower:
                idx = cols_lower.index(t.lower())
                actual_col = df.columns[idx]
                return df[actual_col].mean()
        return 0.0
    
    # Note: 'tier_score' is usually lowercase in backtest.py
    avg_score = get_col_mean(trades_df, ['tier_score', 'Tier_Score', 'score'])
    
    avg_htf = get_col_mean(trades_df, ['score_htf', 'Score_HTF'])
    avg_disp = get_col_mean(trades_df, ['score_disp', 'Score_Disp'])
    avg_liq = get_col_mean(trades_df, ['score_liq', 'Score_Liq'])
    
    # FIX: Added 'score_ctxt' to the list (this was the specific bug for Context)
    avg_ctxt = get_col_mean(trades_df, ['score_ctxt', 'score_context', 'Score_Context']) 
    
    # Tier Counts - checking existence of column first
    if 'tier_type' in trades_df.columns:
        t1 = len(trades_df[trades_df['tier_type'] == 'Tier 1'])
        t2 = len(trades_df[trades_df['tier_type'] == 'Tier 2'])
    else:
        t1 = 0
        t2 = 0

    metrics = {
        'Total Trades': total_trades,
        'Win Rate (%)': round(win_rate, 2),
        'Total PnL ($)': round(total_pnl, 2),
        'Return (%)': round(ret_pct, 2),
        'Max Drawdown (%)': round(max_dd, 2),
        'Sharpe Ratio': round(sharpe, 2),
        'Profit Factor': round(profit_factor, 2),
        'Avg Score': round(avg_score, 1),
        'Avg HTF': round(avg_htf, 1),
        'Avg Disp': round(avg_disp, 1),
        'Avg Liq': round(avg_liq, 1),
        'Avg Ctxt': round(avg_ctxt, 1),
        'Tier 1 Trades': t1,
        'Tier 2 Trades': t2
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
