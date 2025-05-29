import requests
import pandas as pd
from src.db.postgres import PostgresDB
from src.db.model import StockOHLCV


dates = ['2025-05', '2025-04', '2025-03', '2025-02', '2025-01', '2024-12', '2024-11', '2024-10',
         '2024-09', '2024-08', '2024-07', '2024-06', '2024-05', '2024-04', '2024-03', '2024-02', 
         '2024-01', '2023-12', '2023-11', '2023-10', '2023-09', '2023-08', '2023-07', '2023-06']
api = 'LKCUIB4AZYDSE7HX'
# G0X8Z8B9SKTG6NNL
df = pd.DataFrame()
for date in dates:
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=NVDA&interval=60min&month={date}&outputsize=full&apikey={api}'
    r = requests.get(url)
    data = r.json()
    
    dtemp = pd.DataFrame(data['Time Series (60min)']).T
    dtemp = dtemp.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    })
    dtemp = dtemp.astype(float)
    dtemp.index = pd.to_datetime(dtemp.index)
    dtemp = dtemp.sort_index(ascending=False)
    dtemp = dtemp[['open', 'high', 'low', 'close', 'volume']]
    dtemp = dtemp.rename_axis('datetime').reset_index()
    dtemp['datetime'] = dtemp['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    dtemp['datetime'] = pd.to_datetime(dtemp['datetime'])
    df = pd.concat([df, dtemp], ignore_index=True)

db = PostgresDB()
if db.connect():
    for row in df.iterrows():
        data = {
                "date_time": row[1]["datetime"],
                "symbol": "NVDA",
                "open": row[1]["open"],
                "high": row[1]["high"],
                "low": row[1]["low"],
                "close": row[1]["close"],
                "volume": row[1]["volume"]
            }
        db.create_record(StockOHLCV, data)
    db.disconnect()
    
