def calculate_position_size(equity: float, risk_percentage: float, stop_loss_distance: float) -> float:
    """
    Calculate position size using Fixed Fractional Sizing.
    """
    if stop_loss_distance <= 0:
        return 0.0
    
    risk_amount = equity * risk_percentage
    units = risk_amount / stop_loss_distance
    return units

def get_risk_percentage(tier_score: int, circuit_breaker_active: bool = False, risk_config: dict = None) -> float:
    """
    Determine Risk Percentage using config.
    Default fallback provided for backward compatibility.
    """
    if risk_config is None:
        risk_config = {
            'base_risk_pct': 0.01,
            'tier_1_threshold': 85,
            'tier_2_threshold': 65,
            'tier_2_modifier': 0.5,
            'circuit_breaker_modifier': 0.5
        }
        
    base_unit = risk_config.get('base_risk_pct', 0.01)
    
    if circuit_breaker_active:
        base_unit *= risk_config.get('circuit_breaker_modifier', 0.5)
        
    t1 = risk_config.get('tier_1_threshold', 85)
    t2 = risk_config.get('tier_2_threshold', 65)
    t2_mod = risk_config.get('tier_2_modifier', 0.5)
    
    if tier_score >= t1:
        return base_unit
    elif tier_score >= t2:
        return base_unit * t2_mod
    else:
        return 0.0

def apply_correlation_filter(new_trade_asset: str, open_trades: list) -> bool:
    return True

def apply_news_filter(current_time, news_events: list) -> bool:
    return True
