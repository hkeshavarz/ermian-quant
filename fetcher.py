from ib_async import *
import pandas as pd
import numpy as np
import datetime
import os
import asyncio
import sys

# Add src to path to import indicators
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
from ils.indicators import calculate_atr

def categorize_session(row):
    """
    Determine trading session based on UTC hour (datetime index).
    """
    h = row.hour
    sessions = []
    if 0 <= h < 9:
        sessions.append('Asia')
    if 8 <= h < 17:
        sessions.append('London')
    if 13 <= h < 22:
        sessions.append('NY')
    
    return ",".join(sessions)

async def fetch_bars(ib, contract, end_datetime, what_to_show):
    """
    Fetch 1 Day of 1-hour bars.
    """
    try:
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_datetime,
            durationStr='1 D',
            barSizeSetting='1 hour',
            whatToShow=what_to_show,
            useRTH=True,
            formatDate=1
        )
    except Exception as e:
        print(f"Error fetching {what_to_show}: {e}")
        return pd.DataFrame()

    if not bars:
        return pd.DataFrame()
        
    df = util.df(bars)
    # Ensure index is datetime
    if not df.empty and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
    return df

async def main():
    ib = IB()
    try:
        # Port 4002 as seen in user file, ClientID 12 to avoid conflict
        await ib.connectAsync('127.0.0.1', 4002, clientId=12)
    except Exception as e:
        print(f"Could not connect: {e}")
        return

    contract = Forex('EURUSD', exchange='IDEALPRO')
    
    # Using the date range
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31) 

    current_date = start_date
    delta = datetime.timedelta(days=1)
    
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/daily', exist_ok=True) # New folder for HTF

    while current_date <= end_date:
        # End DateTime as End of Day (23:59:59) for the request
        end_dt = datetime.datetime.combine(current_date, datetime.time(23, 59, 59, 999999))
        
        print(f"Fetching data for {current_date} (End: {end_dt})...")
        
        # 1. Fetch Daily Bar for THIS Day
        try:
            daily_bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_dt,
                durationStr='1 D',
                barSizeSetting='1 day',
                whatToShow='MIDPOINT',
                useRTH=True,
                formatDate=1
            )
            
            if daily_bars:
                df_d = util.df(daily_bars)
                # Save daily bar
                filename_d = f"data/daily/EURUSD_daily_{current_date.strftime('%Y%m%d')}.csv"
                df_d.to_csv(filename_d, index=False)
                print(f"Saved Daily bar to {filename_d}")
        except Exception as e:
            print(f"Error fetching daily: {e}")
        
        # 2. Fetch Midpoint
        df_mid = await fetch_bars(ib, contract, end_dt, 'MIDPOINT')
        if df_mid.empty:
            print(f"No Midpoint data for {current_date}")
            current_date += delta
            await asyncio.sleep(1) 
            continue
            
        # 3. Fetch Bid (for Snapshot)
        await asyncio.sleep(0.1)
        df_bid = await fetch_bars(ib, contract, end_dt, 'BID')
        
        # 4. Fetch Ask (for Snapshot)
        await asyncio.sleep(0.1)
        df_ask = await fetch_bars(ib, contract, end_dt, 'ASK')
        
        # Align: df_mid has open, high, low, close.
        # Rename standard columns
        df_mid = df_mid[['open', 'high', 'low', 'close', 'volume']] 
        df_mid.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # Merge Bid/Ask Closes
        # Join by index (time)
        if not df_bid.empty:
            # Rename for join
            df_bid = df_bid[['close']].rename(columns={'close': 'Bid_Snapshot'})
            df_mid = df_mid.join(df_bid, how='left')
        else:
            df_mid['Bid_Snapshot'] = np.nan
            
        if not df_ask.empty:
            df_ask = df_ask[['close']].rename(columns={'close': 'Ask_Snapshot'})
            df_mid = df_mid.join(df_ask, how='left')
        else:
            df_mid['Ask_Snapshot'] = np.nan
            
        # ATR Calculation
        df_mid['ATR'] = calculate_atr(df_mid)
        
        # Session
        df_mid['Session'] = df_mid.index.map(categorize_session)
        
        filename = f"data/EURUSD_1hour_{current_date.strftime('%Y%m%d')}.csv"
        df_mid.to_csv(filename)
        print(f"Saved {len(df_mid)} bars to {filename}")
        
        current_date += delta
        await asyncio.sleep(2) # Pacing

    ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())