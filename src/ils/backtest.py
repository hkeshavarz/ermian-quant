import pandas as pd
import numpy as np

class TradeManager:
    def __init__(self):
        self.active_trades = []
        self.closed_trades = []
        self.trade_id_counter = 0

    def add_trade(self, signal_row):
        """
        Open a new trade based on signal.
        """
        self.trade_id_counter += 1
        trade = {
            'id': self.trade_id_counter,
            'entry_time': signal_row.name,
            'signal': signal_row['Signal'],
            'entry_price': signal_row['Entry_Price'],
            'stop_loss': signal_row['Stop_Loss'],
            'take_profit': signal_row['Take_Profit'],
            'risk_units': signal_row['Risk_Units'],
            'htf_bias': signal_row.get('HTF_Bias', 'Neutral'),
            'atr': signal_row.get('ATR', 0),
            'status': 'Open'
        }
        self.active_trades.append(trade)

    def update(self, bar_row):
        """
        Update active trades based on current bar (OHLC).
        Check for SL/TP hits.
        """
        # Iterate backwards to remove safely
        for i in range(len(self.active_trades) - 1, -1, -1):
            trade = self.active_trades[i]
            
            # Skip if bar is before entry (shouldn't happen if fed sequentially)
            # Or if bar is same minute as entry? Assuming entry at close, check next bar.
            if bar_row.name <= trade['entry_time']:
                continue
                
            # Check Stop Loss
            sl_hit = False
            tp_hit = False
            
            if trade['signal'] == 'Long':
                # SL: If Low drops below SL
                if bar_row['Low'] <= trade['stop_loss']:
                    sl_hit = True
                    exit_price = trade['stop_loss'] # Slippage? Assume limit/stop
                # TP: If High rises above TP
                elif bar_row['High'] >= trade['take_profit']:
                    tp_hit = True
                    exit_price = trade['take_profit']
            else: # Short
                # SL: If High rises above SL
                if bar_row['High'] >= trade['stop_loss']:
                    sl_hit = True
                    exit_price = trade['stop_loss']
                # TP: If Low drops below TP
                elif bar_row['Low'] <= trade['take_profit']:
                    tp_hit = True
                    exit_price = trade['take_profit']
            
            if sl_hit or tp_hit:
                trade['exit_time'] = bar_row.name
                trade['exit_price'] = exit_price
                trade['result'] = 'Win' if tp_hit else 'Loss'
                trade['status'] = 'Closed'
                
                # Calculate PnL (Raw Money based on Units)
                if trade['signal'] == 'Long':
                    pnl = (exit_price - trade['entry_price']) * trade['risk_units']
                else:
                    pnl = (trade['entry_price'] - exit_price) * trade['risk_units']
                
                trade['pnl'] = pnl
                
                self.closed_trades.append(trade)
                self.active_trades.pop(i)

    def get_results_df(self):
        if not self.closed_trades:
            return pd.DataFrame()
        return pd.DataFrame(self.closed_trades)
