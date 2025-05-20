from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, window, first, last, max, min, sum, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import redis
import json
from datetime import datetime

def transform_nvidia_stock_data():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NVIDIA Stock Transformation") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    # Redis configuration
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0
    stream_name = "finnhub_stream"  # Stream name used in the extraction code
    
    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    
    # Read data from Redis Stream
    # Get all messages from the stream (or use COUNT parameter to limit)
    stream_data = r.xread({stream_name: '0'}, count=10000)
    
    # Check if data exists
    if stream_data and len(stream_data) > 0 and len(stream_data[0]) > 1 and stream_data[0][1]:
        # Extract the data from stream
        raw_data = []
        for message_id, message_data in stream_data[0][1]:
            # Convert bytes to string for each field
            item = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}
            raw_data.append({
                "s": item["s"],  # symbol
                "p": float(item["p"]),  # price
                "v": float(item["v"]),  # volume
                "t": int(item["t"])  # timestamp
            })
        
        # Define schema for the raw data
        schema = StructType([
            StructField("s", StringType(), True),  # symbol
            StructField("p", DoubleType(), True),  # price
            StructField("v", DoubleType(), True),  # volume
            StructField("t", LongType(), True)     # unix milliseconds timestamp
        ])
        
        # Create DataFrame from raw data
        df = spark.createDataFrame(raw_data, schema)
        
        # Data transformation
        # 1. Convert unix timestamp to datetime format
        df = df.withColumn("timestamp", from_unixtime(col("t")/1000))
        
        # 2. Aggregate data hourly to get OHLCV (Open, High, Low, Close, Volume)
        hourly_df = df \
            .withColumn("hour_window", window(col("timestamp"), "1 hour")) \
            .groupBy("hour_window", "s") \
            .agg(
                first("p").alias("open"),
                max("p").alias("high"),
                min("p").alias("low"),
                last("p").alias("close"),
                sum("v").alias("volume")
            ) \
            .withColumn("timestamp", col("hour_window.start")) \
            .drop("hour_window")
        
        # 3. Format timestamp for readability
        hourly_df = hourly_df.withColumn("date_time", 
                                      date_format(col("timestamp"), "yyyy-MM-dd HH:00:00"))
        
        # Select only the necessary columns
        result_df = hourly_df.select("date_time", "s", "open", "high", "low", "close", "volume")
        
        # Show the transformed data before stopping Spark
        print("Data transformation completed successfully.")
        result_df.show(20, False)
        
        # Stop Spark session
        spark.stop()
        
        # Return the transformed data
        return result_df
    else:
        print("No data found in Redis Stream.")
        return None

# Execute the transformation
if __name__ == "__main__":
    transformed_data = transform_nvidia_stock_data()
    if transformed_data is not None:
        print("Transformation process completed.")
    else:
        print("Transformation failed or no data was found.")