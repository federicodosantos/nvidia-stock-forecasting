from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, window, first, last, max, min, sum, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def transform_to_ohlcv(data):
    spark = SparkSession.builder.appName("TransformNvidia").getOrCreate()

    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", DoubleType(), True),
        StructField("v", DoubleType(), True),
        StructField("t", LongType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("timestamp", from_unixtime(col("t") / 1000))

    result = df \
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
        .drop("hour_window") \
        .withColumn("date_time", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00")) \
        .select("date_time", "s", "open", "high", "low", "close", "volume")

    return result
