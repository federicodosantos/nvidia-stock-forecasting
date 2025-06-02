from src.db.postgres import PostgresDB
from src.db.model import StockOHLCV
from datetime import datetime

def load_to_postgres(df, db):
    if db.connect():
        for row in df.collect():
            data = {
                "date_time": datetime.strptime(row["date_time"], "%Y-%m-%d %H:%M:%S"),
                "symbol": row["s"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            }
            db.create_record(StockOHLCV, data)
        db.disconnect()
