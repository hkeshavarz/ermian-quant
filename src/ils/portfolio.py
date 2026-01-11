import pandas as pd
import numpy as np
from .risk import get_risk_percentage

# Section 6.3 Correlation Matrix (Hardcoded for Major Pairs)
# Values > 0.7 indicate high positive correlation.
# Values < -0.7 indicate high negative correlation. (Inverse)
# This is a simplified static matrix. Ideally dynamic.
CORRELATION_MATRIX = {
    'EURUSD': {'GBPUSD': 0.85, 'USDCHF': -0.90, 'AUDUSD': 0.75, 'XAUUSD': 0.40},
    'GBPUSD': {'EURUSD': 0.85, 'USDCHF': -0.80, 'AUDUSD': 0.70},
    'AUDUSD': {'EURUSD': 0.75, 'GBPUSD': 0.70, 'XAUUSD': 0.60},
    'USDCHF': {'EURUSD': -0.90, 'GBPUSD': -0.80},
    'USDCAD': {'AUDUSD': -0.50, 'XAUUSD': -0.30}, # Oil proxy, often inverse AUD slightly
    'USDJPY': {'EURJPY': 0.60, 'GBPJPY': 0.60},
    'EURJPY': {'USDJPY': 0.60, 'EURUSD': 0.50},
    'GBPJPY': {'USDJPY': 0.60, 'GBPUSD': 0.50},
    'XAUUSD': {'AUDUSD': 0.60, 'XAGUSD': 0.85},
    'XAGUSD': {'XAUUSD': 0.85}
}

class PortfolioManager:
    def __init__(self, initial_equity=25000.0, risk_config: dict = None, limits_config: dict = None):
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.risk_config = risk_config or {}
        self.limits_config = limits_config or {}
        
        self.high_water_mark = initial_equity
        self.open_positions = [] 
        self.closed_trades = []
        self.trade_id_counter = 0
        
        self.correlation_threshold = self.limits_config.get('correlation_threshold', 0.75)
        self.max_correlated_pos = self.limits_config.get('max_correlated_positions', 100)
        self.max_concurrent_pos = self.limits_config.get('max_concurrent_positions', 100)
        
        self.max_correlated_risk = self.risk_config.get('max_correlated_exposure', 0.02)
        
        self.circuit_breaker_dd = self.risk_config.get('circuit_breaker_drawdown', 0.05)
        self.circuit_breaker_active = False # Section 6.2

    def update_mark_to_market(self, current_prices):
        """
        Update current equity based on unrealized PnL of open positions.
        current_prices: dict {symbol: {'Bid': float, 'Ask': float}}
        """
        unrealized_pnl = 0.0
        
        for trade in self.open_positions:
            symbol = trade['symbol']
            if symbol not in current_prices:
                continue
                
            prices = current_prices[symbol]
            # Long exits at Bid, Short exits at Ask
            if trade['signal'] == 'Long':
                current_value = prices.get('Bid_Close', prices.get('Close'))
                pnl = (current_value - trade['entry_price']) * trade['units']
            else:
                current_value = prices.get('Ask_Close', prices.get('Close'))
                pnl = (trade['entry_price'] - current_value) * trade['units']
            
            unrealized_pnl += pnl
            
        realized_pnl = sum(t['pnl'] for t in self.closed_trades)
        
        # Total Equity = Initial + Realized + Unrealized
        self.current_equity = self.initial_equity + realized_pnl + unrealized_pnl
        
        # High Water Mark Logic
        if self.current_equity > self.high_water_mark:
            self.high_water_mark = self.current_equity
            self.circuit_breaker_active = False # Reset if recovered
            
        # Drawdown Check (Section 6.2)
        if self.high_water_mark > 0:
            dd_pct = (self.high_water_mark - self.current_equity) / self.high_water_mark
        else:
            dd_pct = 0
            
        if dd_pct >= self.circuit_breaker_dd:
            self.circuit_breaker_active = True

    def check_correlation_constraint(self, new_symbol, new_direction, new_risk_pct):
        """
        Check correlated limits: count and risk.
        """
        current_correlated_risk = 0.0
        correlated_count = 0
        
        # Check against open positions
        for trade in self.open_positions:
            existing_symbol = trade['symbol']
            
            is_correlated = False
            
            if existing_symbol == new_symbol:
                is_correlated = True
            else:
                # Check Matrix
                corr = CORRELATION_MATRIX.get(new_symbol, {}).get(existing_symbol, 0)
                
                if abs(corr) >= self.correlation_threshold:
                     if new_direction == trade['signal'] and corr > 0:
                         is_correlated = True
                     elif new_direction != trade['signal'] and corr < 0:
                         is_correlated = True

            if is_correlated:
                current_correlated_risk += trade['risk_pct_account']
                correlated_count += 1
                
        # Count Limit
        if correlated_count >= self.max_correlated_pos:
            return False
            
        # Risk Limit
        total_risk = current_correlated_risk + new_risk_pct
        if total_risk > self.max_correlated_risk:
            return False 
            
        return True

    def size_candidate(self, signal):
        """
        Calculate Position Size (Fixed Fractional) using Config.
        """
        # 1. Check Max Concurrent Positions
        if len(self.open_positions) >= self.max_concurrent_pos:
            return 0.0
            
        # Delegate to risk module
        risk_pct = get_risk_percentage(signal['Tier_Score'], self.circuit_breaker_active, self.risk_config)
        
        if risk_pct <= 0:
            return 0.0
            
        # Section 6.3: Correlation Cap Check
        if not self.check_correlation_constraint(signal['Symbol'], signal['Signal'], risk_pct):
            return 0.0
            
        # Calculation
        entry = signal['Entry_Price']
        sl = signal['Stop_Loss']
        dist = abs(entry - sl)
        
        if dist == 0: return 0.0
        
        risk_amt = self.current_equity * risk_pct
        units = risk_amt / dist
        
        signal['Risk_Pct_Account'] = risk_pct
        return units

    def execute_trade(self, signal, units, timestamp):
        self.trade_id_counter += 1
        trade = {
            'id': self.trade_id_counter,
            'symbol': signal['Symbol'],
            'entry_time': timestamp,
            'signal': signal['Signal'],
            'entry_price': signal['Entry_Price'],
            'stop_loss': signal['Stop_Loss'],
            'take_profit': signal['Take_Profit'],
            'tier_score': signal['Tier_Score'],
            'units': units,
            'risk_pct_account': signal.get('Risk_Pct_Account', 0),
            'pnl': 0.0,
            'status': 'Open'
        }
        self.open_positions.append(trade)
        
    def process_market_update(self, timestamp, current_prices):
        """
        Check SL/TP for all open positions.
        current_prices: dict {symbol: {'Bid', 'Ask', 'High', 'Low'}}
        """
        # Iterate backwards
        for i in range(len(self.open_positions) - 1, -1, -1):
            trade = self.open_positions[i]
            symbol = trade['symbol']
            
            if symbol not in current_prices:
                continue
                
            bar = current_prices[symbol]
            # Assuming bar has 'High', 'Low' (Bid/Ask High/Low ideally)
            # Simplification: Use Bar High/Low (Mid) +/- Spread proxy?
            # Or pass actual Bid/Ask bars. 
            # For backtest efficiency, we pass Aggregated Bar.
            # Logic similar to TradeManager.
            
            sl_hit = False
            tp_hit = False
            exit_price = 0.0
            
            bid_low = bar.get('Bid_Low', bar['Low'])
            bid_high = bar.get('Bid_High', bar['High'])
            ask_low = bar.get('Ask_Low', bar['Low'])
            ask_high = bar.get('Ask_High', bar['High'])
            
            if trade['signal'] == 'Long':
                if bid_low <= trade['stop_loss']:
                    sl_hit = True
                    # Slippage logic placeholder (Section 16.6)
                    exit_price = trade['stop_loss'] 
                elif bid_high >= trade['take_profit']:
                    tp_hit = True
                    exit_price = trade['take_profit']
            else:
                if ask_high >= trade['stop_loss']:
                    sl_hit = True
                    exit_price = trade['stop_loss']
                elif ask_low <= trade['take_profit']:
                    tp_hit = True
                    exit_price = trade['take_profit']
            
            if sl_hit or tp_hit:
                trade['exit_time'] = timestamp
                trade['exit_price'] = exit_price
                trade['status'] = 'Closed'
                
                # PnL Calc
                if trade['signal'] == 'Long':
                    pnl = (exit_price - trade['entry_price']) * trade['units']
                else:
                    pnl = (trade['entry_price'] - exit_price) * trade['units']
                    
                trade['pnl'] = pnl
                trade['result'] = 'Win' if pnl > 0 else 'Loss'
                self.closed_trades.append(trade)
                self.open_positions.pop(i)
