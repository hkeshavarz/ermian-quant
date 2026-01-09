import pytest
import pandas as pd
import numpy as np
import sys
import os

# Ensure we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from ils.indicators import calculate_atr
from ils.smc import detect_fvg, detect_liquidity_sweeps
from ils.risk import calculate_position_size, get_risk_percentage

@pytest.fixture
def sample_data():
    # create simple OHLC data
    idx = pd.date_range('2023-01-01', periods=10, freq='h')
    data = {
        'Open': [100, 102, 101, 105, 104, 100, 101, 103, 102, 105],
        'High': [102, 104, 103, 108, 105, 102, 104, 104, 103, 107],
        'Low':  [99,  101, 100, 104, 100, 98,  100, 101, 100, 103],
        'Close':[102, 101, 103, 106, 101, 99,  103, 102, 101, 106]
    }
    return pd.DataFrame(data, index=idx)

def test_atr(sample_data):
    atr = calculate_atr(sample_data, period=3)
    assert not atr.isnull().all()
    # First few should be nan
    assert np.isnan(atr.iloc[0])

def test_fvg_detection():
    # Manufactur a Bullish FVG
    # i-2: High=100
    # i-1: Green candle
    # i: Low=102 -> Gap=2.0
    
    data = {
        'Open': [90, 101, 105],
        'High': [100, 105, 110], 
        'Low':  [80, 101, 102], # Low[2] (102) > High[0] (100)
        'Close':[95, 104, 108]  # Candle 1 is Green (104 > 101)
    }
    df = pd.DataFrame(data)
    df['ATR'] = 1.0 # Force small ATR so gap is valid
    
    fvg = detect_fvg(df)
    
    # Index 2 should identify FVG from 0..2
    assert fvg['FVG_Bullish'].iloc[2] == True

def test_risk_calc():
    # Equity=10000, Risk=1% (0.01) -> $100 risk
    # StopDist=2.0
    # Units = 100 / 2 = 50
    units = calculate_position_size(10000, 0.01, 2.0)
    assert units == 50.0

def test_risk_scaling():
    # Tier 1
    assert get_risk_percentage(90) == 0.01
    # Tier 2
    assert get_risk_percentage(70) == 0.005
    # Tier 3
    assert get_risk_percentage(50) == 0.0
