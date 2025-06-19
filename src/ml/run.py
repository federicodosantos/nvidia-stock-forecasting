import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.ml.train import ARIMAStockForecaster
import pandas as pd
from src.ml.preprocessing import preprocess_stock_data

# Load environment variables
load_dotenv()

# Create result directory if it doesn't exist
os.makedirs("src/ml/result", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("src/ml/result/forecasting.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting NVIDIA stock HIGH PRICE forecasting process")
    
    # Set Spark environment variables
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
    
    # Initialize Spark session with proper configuration
    spark = SparkSession.builder \
        .appName("NvidiaStockHighForecasting") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .master("local[*]") \
        .getOrCreate()
    
    try:

        logger.info("Running data preprocessing")
        preprocessed_df = preprocess_stock_data(spark, data_source="postgres", target_column="high")
        df = preprocessed_df.toPandas()
        df.to_csv('src/ml/result/nvidia_stock_high_preprocessed.csv', index=False)

        # Konversi ke Pandas DataFrame
        logger.info("Converting preprocessed data to Pandas DataFrame")
        pandas_df = preprocessed_df.select("date_time", "high").toPandas()
        pandas_df["date_time"] = pd.to_datetime(pandas_df["date_time"])
        pandas_df.set_index("date_time", inplace=True)
        pandas_df.sort_index(inplace=True)
        
        # Resample menjadi per jam dan forward fill jika ada missing
        pandas_df = pandas_df.resample('1H').mean().ffill()

        # Initialize forecaster
        forecaster = ARIMAStockForecaster(spark_session=spark)
        forecaster.data = pandas_df
        
        # Prepare data
        logger.info("Preparing data for forecasting")
        train_data, test_data = forecaster.prepare_data_from_preprocessed(test_size=0.2)
        
        # Fit ARIMA model
        logger.info("Fitting ARIMA model")
        model = forecaster.fit_model()
        
        # Evaluate model
        logger.info("Evaluating model performance")
        metrics = forecaster.evaluate_model()
        
        # Save metrics to file
        with open('src/ml/result/model_metrics.txt', 'w') as f:
            f.write("ARIMA Model Performance Metrics\n")
            f.write("===============================\n\n")
            for metric, value in metrics.items():
                f.write(f"{metric}: {value:.4f}\n")
        
        # Generate 1-day forecast (24 hours)
        logger.info("Generating 1-day (24 hours) forecast")
        forecast_1day = forecaster.forecast(steps=24)
        
        # Save 1-day forecast to CSV
        forecast_1day.to_csv('src/ml/result/nvidia_high_forecast_1day.csv')
        logger.info("1-day forecast saved to src/ml/result/nvidia_high_forecast_1day.csv")
        
        # Create 1-day forecast plot
        forecaster.plot_forecast(forecast_1day, 
                                title="NVIDIA Stock High Price - 1 Day Forecast",
                                filename="forecast_plot_1day.png")
        
        # Save 1-day forecast summary
        forecaster.save_forecast_summary(forecast_1day, 
                                       filename="forecast_summary_1day.txt")
        
        # Generate 5-day forecast (120 hours)
        logger.info("Generating 5-day (120 hours) forecast")
        forecast_5day = forecaster.forecast(steps=120)
        
        # Save 5-day forecast to CSV
        forecast_5day.to_csv('src/ml/result/nvidia_high_forecast_5day.csv')
        logger.info("5-day forecast saved to src/ml/result/nvidia_high_forecast_5day.csv")
        
        # Create 5-day forecast plot
        forecaster.plot_forecast(forecast_5day, 
                                title="NVIDIA Stock High Price - 5 Day Forecast",
                                filename="forecast_plot_5day.png")
        
        # Save 5-day forecast summary
        forecaster.save_forecast_summary(forecast_5day, 
                                       filename="forecast_summary_5day.txt")
        
        # Create a combined summary report
        logger.info("Creating combined summary report")
        create_combined_report(forecast_1day, forecast_5day, metrics)
        
        logger.info("Forecasting process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in forecasting process: {str(e)}", exc_info=True)
    
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

def create_combined_report(forecast_1day, forecast_5day, metrics):
    """Create a combined summary report"""
    try:
        with open('src/ml/result/combined_forecast_report.txt', 'w') as f:
            f.write("NVIDIA Stock High Price Forecast Report\n")
            f.write("=======================================\n\n")
            f.write(f"Report Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Model Performance
            f.write("Model Performance Metrics:\n")
            f.write("--------------------------\n")
            for metric, value in metrics.items():
                f.write(f"{metric}: {value:.4f}\n")
            f.write("\n")
            
            # 1-Day Forecast Summary
            f.write("1-Day Forecast Summary:\n")
            f.write("-----------------------\n")
            if not forecast_1day.empty:
                f.write(f"Start: {forecast_1day.index[0].strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"End: {forecast_1day.index[-1].strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Opening Price: ${forecast_1day.iloc[0]['forecast']:.2f}\n")
                f.write(f"Closing Price: ${forecast_1day.iloc[-1]['forecast']:.2f}\n")
                f.write(f"Highest Price: ${forecast_1day['forecast'].max():.2f}\n")
                f.write(f"Lowest Price: ${forecast_1day['forecast'].min():.2f}\n")
                f.write(f"Average Price: ${forecast_1day['forecast'].mean():.2f}\n")
                change_1day = forecast_1day.iloc[-1]['forecast'] - forecast_1day.iloc[0]['forecast']
                change_pct_1day = (change_1day / forecast_1day.iloc[0]['forecast']) * 100
                f.write(f"Change: ${change_1day:.2f} ({change_pct_1day:.2f}%)\n\n")
            
            # 5-Day Forecast Summary
            f.write("5-Day Forecast Summary:\n")
            f.write("-----------------------\n")
            if not forecast_5day.empty:
                f.write(f"Start: {forecast_5day.index[0].strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"End: {forecast_5day.index[-1].strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Opening Price: ${forecast_5day.iloc[0]['forecast']:.2f}\n")
                f.write(f"Closing Price: ${forecast_5day.iloc[-1]['forecast']:.2f}\n")
                f.write(f"Highest Price: ${forecast_5day['forecast'].max():.2f}\n")
                f.write(f"Lowest Price: ${forecast_5day['forecast'].min():.2f}\n")
                f.write(f"Average Price: ${forecast_5day['forecast'].mean():.2f}\n")
                change_5day = forecast_5day.iloc[-1]['forecast'] - forecast_5day.iloc[0]['forecast']
                change_pct_5day = (change_5day / forecast_5day.iloc[0]['forecast']) * 100
                f.write(f"Change: ${change_5day:.2f} ({change_pct_5day:.2f}%)\n\n")
            
            # Trend Analysis
            f.write("Trend Analysis:\n")
            f.write("---------------\n")
            if not forecast_1day.empty and not forecast_5day.empty:
                if change_1day > 0:
                    f.write("1-Day Trend: BULLISH ↑\n")
                else:
                    f.write("1-Day Trend: BEARISH ↓\n")
                
                if change_5day > 0:
                    f.write("5-Day Trend: BULLISH ↑\n")
                else:
                    f.write("5-Day Trend: BEARISH ↓\n")
            
            f.write("\n")
            f.write("Note: These forecasts are based on historical data and should not be used as the sole basis for investment decisions.\n")
        
        logger.info("Combined forecast report saved to src/ml/result/combined_forecast_report.txt")
        
    except Exception as e:
        logger.error(f"Error creating combined report: {str(e)}")

if __name__ == "__main__":
    main()