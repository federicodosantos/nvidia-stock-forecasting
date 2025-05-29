from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, date_format, to_date
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
import pandas as pd
import numpy as np
from src.db.postgres import PostgresDB
from src.db.model import StockOHLCV

def preprocess_stock_data(spark, data_source="postgres", target_column="high"):
    """
    Preprocess stock data for time series analysis
    
    Args:
        spark: SparkSession instance
        data_source: Source of data ("postgres" or DataFrame)
        target_column: Target column to predict ("high", "close", "low", "open")
    
    Returns:
        Preprocessed DataFrame ready for time series modeling
    """
    if data_source == "postgres":
        # Use PostgresDB to get data from database
        postgres_db = PostgresDB()
        if postgres_db.connect():
            # Get all records from stock_ohlcv table
            records = postgres_db.read_records(StockOHLCV)
            postgres_db.disconnect()
            
            # Convert SQLAlchemy objects to dictionaries
            data = []
            for record in records:
                data.append({
                    "id": record.id,
                    "date_time": record.date_time,
                    "symbol": record.symbol,
                    "open": record.open,
                    "high": record.high,
                    "low": record.low,
                    "close": record.close,
                    "volume": record.volume,
                    "created_at": record.created_at
                })
            
            # Create Spark DataFrame from the data
            df = spark.createDataFrame(data)
        else:
            raise Exception("Failed to connect to PostgreSQL database")
    else:
        # Assume data_source is a DataFrame
        df = data_source
    
    # Convert date_time to date format
    df = df.withColumn("date", to_date(col("date_time")))
    
    # Sort by date for time series analysis
    df = df.orderBy("date_time")
    
    # Create lag features for time series
    windowSpec = Window.orderBy("date_time")
    
    # Create lag features (previous day's values) for the target column
    for i in range(1, 8):  # Create 7 days of lag features
        df = df.withColumn(f"{target_column}_lag_{i}", lag(target_column, i).over(windowSpec))
        df = df.withColumn(f"volume_lag_{i}", lag("volume", i).over(windowSpec))
    
    # Calculate returns based on target column
    df = df.withColumn("daily_return", 
                      (col(target_column) - col(f"{target_column}_lag_1")) / col(f"{target_column}_lag_1"))
    
    # Calculate moving averages for target column
    df = df.withColumn(f"{target_column}_ma_5", 
                      (col(target_column) + col(f"{target_column}_lag_1") + col(f"{target_column}_lag_2") + 
                       col(f"{target_column}_lag_3") + col(f"{target_column}_lag_4")) / 5)
    
    # Calculate high-low spread
    df = df.withColumn("high_low_spread", col("high") - col("low"))
    
    # Calculate volatility indicator
    df = df.withColumn("volatility", (col("high") - col("low")) / col("close"))
    
    # Drop rows with null values (due to lag features)
    df = df.na.drop()
    
    # Select relevant features
    feature_cols = [
        target_column, "volume", "daily_return", f"{target_column}_ma_5",
        "high_low_spread", "volatility",
        f"{target_column}_lag_1", f"{target_column}_lag_2", f"{target_column}_lag_3",
        "volume_lag_1", "volume_lag_2", "volume_lag_3"
    ]
    
    # Assemble features into a vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)
    
    # Standardize features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)
    
    return df

def convert_to_pandas_for_arima(spark_df, target_column="high"):
    """
    Convert Spark DataFrame to Pandas DataFrame for ARIMA modeling
    
    Args:
        spark_df: PySpark DataFrame
        target_column: Target column to use for time series
    
    Returns:
        Pandas DataFrame with time series data
    """
    # Select only necessary columns for time series
    pandas_df = spark_df.select("date_time", target_column).toPandas()
    
    # Convert to datetime and set as index
    pandas_df["date_time"] = pd.to_datetime(pandas_df["date_time"])
    pandas_df.set_index("date_time", inplace=True)
    
    # Sort index for time series
    pandas_df.sort_index(inplace=True)
    
    # Resample to make sure we have regular intervals (hourly)
    pandas_df = pandas_df.resample('1H').mean()
    
    # Forward fill missing values
    pandas_df.fillna(method='ffill', inplace=True)
    
    return pandas_df