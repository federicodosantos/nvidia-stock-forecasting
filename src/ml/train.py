import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error
from db.postgres import PostgresDB
from db.model import StockOHLCV
import os

class ARIMAStockForecaster:
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.data = None
        self.train_data = None
        self.test_data = None
        self.model = None
        self.order = (1, 1, 1)  # Default order (p,d,q)
        self.differencing = 0
        # Create results directory if it doesn't exist
        self.results_dir = "src/ml/result"
        os.makedirs(self.results_dir, exist_ok=True)
    
    def load_data_from_db(self):
        """Load stock data from PostgreSQL database"""
        db = PostgresDB()
        if db.connect():
            records = db.read_records(StockOHLCV)
            db.disconnect()
            
            # Convert SQLAlchemy objects to dictionaries
            data = []
            for record in records:
                data.append({
                    "date_time": record.date_time,
                    "symbol": record.symbol,
                    "open": record.open,
                    "high": record.high,
                    "low": record.low,
                    "close": record.close,
                    "volume": record.volume
                })
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Convert to datetime and set as index
            df['date_time'] = pd.to_datetime(df['date_time'])
            
            # Check for duplicate timestamps
            if df['date_time'].duplicated().any():
                print(f"Warning: Found {df['date_time'].duplicated().sum()} duplicate timestamps")
                
                # Keep only the first occurrence of each timestamp
                df = df.drop_duplicates(subset='date_time', keep='first')
                print(f"Kept first occurrence of each timestamp. Remaining records: {len(df)}")
            
            # Set date_time as index after handling duplicates
            df.set_index('date_time', inplace=True)
            
            # Sort by date
            df.sort_index(inplace=True)
            
            # Drop non-numeric columns before resampling
            numeric_df = df.select_dtypes(include=['number'])
            
            # Ensure we have the columns we need
            if 'high' not in numeric_df.columns:
                raise ValueError("'high' column not found in the data")
            
            # Store the original data
            self.data = numeric_df
            
            print(f"Loaded {len(numeric_df)} records from database")
            return numeric_df
        else:
            raise Exception("Failed to connect to database")
    
    def check_stationarity(self, series):
        """Test stationarity using Augmented Dickey-Fuller test"""
        result = adfuller(series.dropna())
        print(f'ADF Statistic: {result[0]}')
        print(f'p-value: {result[1]}')
        
        for key, value in result[4].items():
            print(f'Critical Value ({key}): {value}')
        
        is_stationary = result[1] < 0.05
        print(f"Series is {'stationary' if is_stationary else 'non-stationary'}")
        return is_stationary
    
    def make_stationary(self, series):
        """Apply differencing to make series stationary"""
        is_stationary = self.check_stationarity(series)
        diff_order = 0
        diff_series = series.copy()
        
        while not is_stationary and diff_order < 2:
            print(f"Applying differencing (order {diff_order + 1})...")
            diff_series = diff_series.diff().dropna()
            is_stationary = self.check_stationarity(diff_series)
            diff_order += 1
        
        self.differencing = diff_order
        return diff_series, diff_order
    
    def plot_acf_pacf(self, series, max_lags=40):
        """Plot ACF and PACF to identify ARIMA parameters"""
        # Ensure max_lags is appropriate for the data
        max_lags = min(max_lags, len(series) - 1)
        
        if max_lags <= 0:
            print("Warning: Not enough data points for ACF/PACF plots")
            return
        
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            plot_acf(series, ax=ax1, lags=max_lags)
            plot_pacf(series, ax=ax2, lags=max_lags)
            
            plt.tight_layout()
            plt.savefig(os.path.join(self.results_dir, 'acf_pacf_plot.png'))
            print(f"ACF and PACF plots saved in '{self.results_dir}/acf_pacf_plot.png' with {max_lags} lags")
        except Exception as e:
            print(f"Error creating ACF/PACF plots: {str(e)}")
    
    def find_best_parameters(self):
        """Find optimal ARIMA parameters by grid search"""
        print("Finding optimal ARIMA parameters by grid search...")
        
        # If we have very few data points, use simple model
        if len(self.train_data) < 10:
            self.order = (1, self.differencing, 0)
            print(f"Warning: Too few data points for grid search. Using default order {self.order}")
            return self.order
        
        best_aic = float("inf")
        best_order = None
        
        # Define the p, d and q parameters to take any value between 0 and 2
        p_values = range(0, 3)
        d_values = [self.differencing]  # Use differencing order from stationarity test
        q_values = range(0, 3)
        
        for p in p_values:
            for d in d_values:
                for q in q_values:
                    try:
                        model = ARIMA(self.train_data, order=(p, d, q))
                        model_fit = model.fit()
                        
                        # Get AIC
                        aic = model_fit.aic
                        
                        # Update if better model found
                        if aic < best_aic:
                            best_aic = aic
                            best_order = (p, d, q)
                            print(f"New best order: ARIMA({p},{d},{q}) - AIC: {aic}")
                    except Exception as e:
                        print(f"Error fitting ARIMA({p},{d},{q}): {str(e)[:100]}...")
                        continue
        
        if best_order is None:
            # If no model was successfully fit, use default
            best_order = (1, self.differencing, 0)
            print(f"Warning: Could not find valid model. Using default order {best_order}")
        else:
            print(f"Best ARIMA order: {best_order} with AIC: {best_aic}")
        
        self.order = best_order
        return best_order
    
    def prepare_data(self, test_size=0.2):
        """Prepare data for modeling"""
        if self.data is None:
            self.load_data_from_db()
        
        # Use high price for forecasting
        series = self.data['high']
        
        # Check if we have enough data
        if len(series) < 10:
            print("Warning: Very few data points available for analysis")
        
        # Check stationarity and make stationary if needed
        stationary_series, diff_order = self.make_stationary(series)
        
        # Plot ACF and PACF with appropriate lags
        max_lags = min(40, len(stationary_series) // 2)
        if max_lags > 0:
            self.plot_acf_pacf(stationary_series, max_lags=max_lags)
        else:
            print("Warning: Not enough data points for ACF/PACF plots")
        
        # Split into train and test sets
        split_idx = int(len(series) * (1 - test_size))
        if split_idx <= 0:
            # If too few points, use all for training
            self.train_data = series
            self.test_data = series.iloc[-1:]  # Use last point for testing
            print("Warning: Too few data points. Using all data for training.")
        else:
            self.train_data = series[:split_idx]
            self.test_data = series[split_idx:]
        
        # Find best parameters if we have enough data
        if len(self.train_data) >= 3:
            self.find_best_parameters()
        else:
            # Use default parameters for very small datasets
            self.order = (1, diff_order, 0)
            print(f"Warning: Too few data points for parameter search. Using default order {self.order}")
        
        print(f"Training data: {len(self.train_data)} records")
        print(f"Testing data: {len(self.test_data)} records")
        
        return self.train_data, self.test_data
    
    def fit_model(self):
        """Fit ARIMA model to training data"""
        if self.train_data is None:
            raise ValueError("Data not prepared. Call prepare_data() first.")
        
        p, d, q = self.order
        
        try:
            print(f"Fitting ARIMA({p},{d},{q}) model...")
            self.model = ARIMA(self.train_data, order=(p, d, q))
            self.fitted_model = self.model.fit()
            
            print(self.fitted_model.summary())
            return self.fitted_model
        except Exception as e:
            print(f"Error fitting ARIMA model: {str(e)}")
            # Try a simpler model as fallback
            try:
                print("Trying fallback model ARIMA(1,1,0)...")
                self.model = ARIMA(self.train_data, order=(1, 1, 0))
                self.fitted_model = self.model.fit()
                self.order = (1, 1, 0)
                print(self.fitted_model.summary())
                return self.fitted_model
            except Exception as e2:
                print(f"Error fitting fallback model: {str(e2)}")
                raise ValueError("Failed to fit any ARIMA model to the data")
    
    def forecast(self, steps=24):
        """Generate forecasts"""
        if self.fitted_model is None:
            raise ValueError("Model not fitted. Call fit_model() first.")
        
        try:
            # Generate forecast
            forecast_result = self.fitted_model.get_forecast(steps=steps)
            
            # Get forecast values and confidence intervals
            forecast_mean = forecast_result.predicted_mean
            forecast_ci = forecast_result.conf_int()
            
            # Create forecast dates (continuing from the last date in the data)
            last_date = self.test_data.index[-1] if self.test_data is not None else self.train_data.index[-1]
            
            # Determine frequency from the data
            if hasattr(self.data.index, 'freq') and self.data.index.freq is not None:
                freq = self.data.index.freq
            else:
                # Try to infer frequency or use hourly as default
                try:
                    freq = pd.infer_freq(self.data.index)
                    if freq is None:
                        freq = 'H'  # Default to hourly
                except:
                    freq = 'H'  # Default to hourly
            
            forecast_dates = pd.date_range(start=last_date, periods=steps+1, freq=freq)[1:]
            
            # Ensure lengths match
            if len(forecast_dates) > len(forecast_mean):
                forecast_dates = forecast_dates[:len(forecast_mean)]
            elif len(forecast_dates) < len(forecast_mean):
                forecast_mean = forecast_mean[:len(forecast_dates)]
                forecast_ci = forecast_ci.iloc[:len(forecast_dates)]
            
            # Create DataFrame with proper index
            forecast_df = pd.DataFrame({
                'forecast': forecast_mean.values if hasattr(forecast_mean, 'values') else forecast_mean,
                'lower_ci': forecast_ci.iloc[:, 0].values if hasattr(forecast_ci, 'iloc') else forecast_ci[:, 0],
                'upper_ci': forecast_ci.iloc[:, 1].values if hasattr(forecast_ci, 'iloc') else forecast_ci[:, 1]
            }, index=forecast_dates)
            
            return forecast_df
        
        except Exception as e:
            print(f"Error generating forecast: {str(e)}")
            # Return empty DataFrame
            return pd.DataFrame(columns=['forecast', 'lower_ci', 'upper_ci'])
    
    def evaluate_model(self):
        """Evaluate model on test data"""
        if self.fitted_model is None or self.test_data is None:
            raise ValueError("Model not fitted or test data not available")
        
        try:
            # Generate predictions for test period
            predictions = self.fitted_model.get_forecast(steps=len(self.test_data))
            predicted_mean = predictions.predicted_mean
            
            # Ensure alignment by creating a new Series with the test data index
            if len(predicted_mean) != len(self.test_data):
                print(f"Warning: Length mismatch - predictions: {len(predicted_mean)}, test data: {len(self.test_data)}")
                # Truncate to the shorter length
                min_length = min(len(predicted_mean), len(self.test_data))
                predicted_mean = predicted_mean[:min_length]
                test_data_aligned = self.test_data.iloc[:min_length]
            else:
                test_data_aligned = self.test_data
            
            # Create a Series with the test data index
            predicted_series = pd.Series(
                predicted_mean.values if hasattr(predicted_mean, 'values') else predicted_mean,
                index=test_data_aligned.index
            )
            
            # Calculate metrics
            mse = mean_squared_error(test_data_aligned, predicted_series)
            rmse = np.sqrt(mse)
            mae = mean_absolute_error(test_data_aligned, predicted_series)
            
            # Calculate MAPE safely
            mape = np.mean(np.abs((test_data_aligned - predicted_series) / test_data_aligned)) * 100
            
            metrics = {
                'MSE': mse,
                'RMSE': rmse,
                'MAE': mae,
                'MAPE': mape
            }
            
            print("Model Evaluation Metrics:")
            for metric, value in metrics.items():
                print(f"{metric}: {value:.4f}")
            
            return metrics
        
        except Exception as e:
            print(f"Error evaluating model: {str(e)}")
            # Return placeholder metrics
            return {
                'MSE': float('nan'),
                'RMSE': float('nan'),
                'MAE': float('nan'),
                'MAPE': float('nan')
            }
    
    def plot_forecast(self, forecast_df, title="NVIDIA Stock High Price Forecast", filename="forecast_plot.png"):
        """
        Create a clean, simple line plot of historical data and forecast
        """
        if forecast_df is None or forecast_df.empty:
            print("No forecast data available for plotting")
            return
        
        try:
            plt.figure(figsize=(12, 6))
            plt.style.use('seaborn-v0_8-whitegrid')  # Use a cleaner style
            
            # Calculate the date 1 month ago from the last date in the data
            last_date = self.test_data.index[-1] if self.test_data is not None else self.train_data.index[-1]
            one_month_ago = last_date - pd.Timedelta(days=30)
            
            # Filter test data to show only the last month
            if self.test_data is not None:
                test_last_month = self.test_data[self.test_data.index >= one_month_ago]
                if not test_last_month.empty:
                    plt.plot(test_last_month.index, test_last_month, 
                            label='Historical High Price', color='#1f77b4', linewidth=2)
            
            # Plot forecast as a simple line
            plt.plot(forecast_df.index, forecast_df['forecast'], 
                    label='Forecast', color='#ff7f0e', linewidth=2)
            
            # Add confidence intervals
            plt.fill_between(forecast_df.index, 
                           forecast_df['lower_ci'], 
                           forecast_df['upper_ci'], 
                           color='#ff7f0e', alpha=0.2)
            
            # Add a vertical line to separate historical data from forecast
            plt.axvline(x=last_date, color='gray', linestyle='--', alpha=0.7)
            
            # Customize the plot
            plt.title(title, fontsize=16)
            plt.xlabel('Date', fontsize=12)
            plt.ylabel('Stock High Price ($)', fontsize=12)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Save the plot
            full_path = os.path.join(self.results_dir, filename)
            plt.savefig(full_path, dpi=300, bbox_inches='tight')
            print(f"Forecast plot saved as '{full_path}'")
            plt.close()
            
        except Exception as e:
            print(f"Error creating forecast plot: {str(e)}")

    def save_forecast_summary(self, forecast_df, filename="forecast_summary.txt"):
        """
        Generate a summary of the forecast
        """
        try:
            if forecast_df.empty:
                print("No forecast data available for summary")
                return
            
            # Calculate summary statistics
            forecast_mean = forecast_df['forecast'].mean()
            forecast_max = forecast_df['forecast'].max()
            forecast_min = forecast_df['forecast'].min()
            
            # Get the first and last values
            first_value = forecast_df.iloc[0]['forecast']
            last_value = forecast_df.iloc[-1]['forecast']
            
            # Create summary file
            full_path = os.path.join(self.results_dir, filename)
            with open(full_path, 'w') as f:
                f.write("NVIDIA Stock High Price Forecast Summary\n")
                f.write("========================================\n\n")
                f.write(f"Forecast Period: {forecast_df.index[0].strftime('%Y-%m-%d %H:%M')} to {forecast_df.index[-1].strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Number of periods: {len(forecast_df)}\n\n")
                
                f.write("Summary Statistics:\n")
                f.write(f"First Value : ${first_value:.2f}\n")
                f.write(f"Last Value  : ${last_value:.2f}\n")
                f.write(f"Average     : ${forecast_mean:.2f}\n")
                f.write(f"Maximum     : ${forecast_max:.2f}\n")
                f.write(f"Minimum     : ${forecast_min:.2f}\n")
                f.write(f"Change      : ${(last_value - first_value):.2f} ({((last_value - first_value) / first_value * 100):.2f}%)\n\n")
                
                # Add hourly/daily breakdown
                if len(forecast_df) <= 24:
                    f.write("Hourly Forecast:\n")
                    f.write("----------------\n")
                    for idx, row in forecast_df.iterrows():
                        f.write(f"{idx.strftime('%Y-%m-%d %H:%M')} : ${row['forecast']:.2f} (${row['lower_ci']:.2f} - ${row['upper_ci']:.2f})\n")
                else:
                    f.write("Daily Summary:\n")
                    f.write("--------------\n")
                    # Group by day
                    daily_groups = forecast_df.groupby(forecast_df.index.date)
                    for date, group in daily_groups:
                        daily_open = group.iloc[0]['forecast']
                        daily_close = group.iloc[-1]['forecast']
                        daily_high = group['forecast'].max()
                        daily_low = group['forecast'].min()
                        f.write(f"{date}: Open ${daily_open:.2f}, High ${daily_high:.2f}, Low ${daily_low:.2f}, Close ${daily_close:.2f}\n")
            
            print(f"Forecast summary saved to '{full_path}'")
            
        except Exception as e:
            print(f"Error generating forecast summary: {str(e)}")