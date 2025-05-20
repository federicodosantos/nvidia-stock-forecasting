from etl.extract import extract_from_redis
from etl.transform import transform_to_ohlcv
from etl.load import load_to_postgres
from db.model import StockOHLCV
from db.postgres import PostgresDB

def run_etl(db):
    data = extract_from_redis()
    if data:
        df = transform_to_ohlcv(data)
        df.show()
        load_to_postgres(df, db) 
    else:
        print("No data to process.")

if __name__ == "__main__":
    db = PostgresDB()
    if db.connect():
        db.create_tables()
        run_etl(db)
        db.disconnect()
