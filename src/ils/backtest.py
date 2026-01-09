import pandas as pd
import numpy as np

class TradeManager:
    def __init__(self):
        self.active_trades = []
        self.closed_trades = []
        self.trade_id_counter = 0

    def calculate_slippage(self, atr, min_tick=0.00001):
        # ILS 16.6: Slippage = max(0.1 * ATR_1m, min_tick)
        # We use current bar ATR as proxy for ATR_1m if not available explicitly
        return max(0.1 * atr, min_tick) if atr > 0 else 0

    def add_trade(self, signal_row):
        """
        Open a new trade based on signal.
        Applies Spread and Slippage to Entry Price.
        """
        self.trade_id_counter += 1
        
        # Determine Execution Price (Bid/Ask)
        direction = signal_row['Signal']
        mid = signal_row['Close']
        atr = signal_row.get('ATR', 0)
        
        # Prices
        ask = signal_row.get('Ask_Close', mid) # Use Mid if Ask missing (Spread=0 assumption or pre-adjusted)
        bid = signal_row.get('Bid_Close', mid)
        
        slippage = self.calculate_slippage(atr)
        
        # Score & Tier
        score = int(signal_row.get('Tier_Score', 0))
        tier_type = 'Tier 1' if score >= 85 else ('Tier 2' if score >= 65 else 'Unknown')
        
        # Breakdown
        s_htf = int(signal_row.get('Score_HTF', 0))
        s_disp = int(signal_row.get('Score_Disp', 0))
        s_liq = int(signal_row.get('Score_Liq', 0))
        s_ctxt = int(signal_row.get('Score_Context', 0))
        
        if direction == 'Long':
            # Buy at Ask + Slippage
            exec_price = ask + slippage
            spread_paid = ask - mid # Approximate half spread? Or (Ask-Bid)
            # Actually cost is usually tracking total spread.
            spread_cost_per_unit = ask - bid
        else:
            # Sell at Bid - Slippage
            exec_price = bid - slippage
            spread_cost_per_unit = ask - bid
            
        trade = {
            'id': self.trade_id_counter,
            'entry_time': signal_row.name,
            'signal': direction,
            'tier_type': tier_type,
            'tier_score': score,
            'score_htf': s_htf,
            'score_disp': s_disp,
            'score_liq': s_liq,
            'score_ctxt': s_ctxt,
            'entry_price': exec_price, # Actual fill
            'planned_entry': mid, # Signal price
            'stop_loss': signal_row['Stop_Loss'],
            'take_profit': signal_row['Take_Profit'],
            'risk_units': signal_row['Risk_Units'],
            'htf_bias': signal_row.get('HTF_Bias', 'Neutral'),
            'atr': atr,
            'status': 'Open',
            'slippage_entry': slippage,
            'spread_cost_unit': spread_cost_per_unit
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
            
            if bar_row.name <= trade['entry_time']:
                continue
                
            # Prices for validation
            # SL triggers on Bid (Long) / Ask (Short)
            # TP triggers on Bid (Long) / Ask (Short) usually
            
            # If Bid/Ask avail
            mid_low = bar_row['Low']
            mid_high = bar_row['High']
            bid_low = bar_row.get('Bid_Low', mid_low)
            bid_high = bar_row.get('Bid_High', mid_high)
            ask_low = bar_row.get('Ask_Low', mid_low)
            ask_high = bar_row.get('Ask_High', mid_high)
            atr = bar_row.get('ATR', trade['atr'])
            
            sl_hit = False
            tp_hit = False
            exit_price = 0.0
            slippage = 0.0
            
            if trade['signal'] == 'Long':
                # SL Trigger: If Bid drops to SL
                # Conservative: check Low of Bid
                if bid_low <= trade['stop_loss']:
                    sl_hit = True
                    # Filled at SL minus Slippage (Market Order)
                    slippage = self.calculate_slippage(atr)
                    exit_price = trade['stop_loss'] - slippage
                    
                # TP Trigger: If Bid rises to TP
                elif bid_high >= trade['take_profit']:
                    tp_hit = True
                    # Filled at TP (Limit Order - No Slippage usually, positive slippage ignored in v1)
                    exit_price = trade['take_profit']
                    
            else: # Short
                # SL Trigger: If Ask rises to SL
                if ask_high >= trade['stop_loss']:
                    sl_hit = True
                    slippage = self.calculate_slippage(atr)
                    exit_price = trade['stop_loss'] + slippage
                    
                # TP Trigger: If Ask drops to TP
                elif ask_low <= trade['take_profit']:
                    tp_hit = True
                    exit_price = trade['take_profit']
            
            if sl_hit or tp_hit:
                trade['exit_time'] = bar_row.name
                trade['exit_price'] = exit_price
                trade['result'] = 'Win' if tp_hit else 'Loss'
                trade['status'] = 'Closed'
                trade['slippage_exit'] = slippage
                
                # Calculate PnL
                units = trade['risk_units']
                if trade['signal'] == 'Long':
                    gross_pnl = (exit_price - trade['entry_price']) * units
                else:
                    gross_pnl = (trade['entry_price'] - exit_price) * units
                
                # Costs
                # Spread Cost: (Ask-Bid) * Units -> approximated by entry spread + exit spread?
                # Actually PnL calc using Exec prices (Ask entry, Bid exit) ALREADY includes spread cost!
                # So we don't deduct it again from PnL, but we can track it.
                # Slippage Cost: Already in exit_price/entry_price.
                # We track the components for reporting.
                
                total_slippage = (trade['slippage_entry'] + slippage) * units
                # Spread captured in price
                
                trade['pnl'] = gross_pnl
                trade['slippage_cost'] = total_slippage
                
                self.closed_trades.append(trade)
                self.active_trades.pop(i)

    def get_results_df(self):
        if not self.closed_trades:
            return pd.DataFrame()
        return pd.DataFrame(self.closed_trades)
