import websocket
import json
import redis
import os
from dotenv import load_dotenv
import threading
import time

load_dotenv()

API_KEY = os.getenv("FINNHUB_TOKEN")  # 'd0m8fgpr01qkesvj7s6gd0m8fgpr01qkesvj7s70'
STREAM_NAME = "finnhub_stream"

# Global variables untuk WebSocket
ws_app = None
ws_thread = None

# Redis connection setup
try:
    r = redis.Redis(host="nvidia-stock-forecasting-redis-1", port=6379, db=0)
    r.ping()
    print("Connected to Redis successfully.")
except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis: {e}")
    r = None


def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for item in data.get("data", []):
            print(f"Trade: {item}")
            if r:
                try:
                    r.xadd(STREAM_NAME, {
                        "s": item["s"],
                        "p": str(item["p"]),
                        "t": str(item["t"]),
                        "v": str(item["v"])
                    })
                except redis.exceptions.RedisError as e:
                    print(f"Redis XADD error: {e}")


def on_open(ws):
    print("Connected to Finnhub WebSocket")
    subscribe_message = {
        "type": "subscribe",
        "symbol": "NVDA"
    }
    try:
        ws.send(json.dumps(subscribe_message))
        print(f"Subscribing to NVDA")
    except websocket.WebSocketConnectionClosedException:
        print("Failed to send subscribe message: WebSocket is closed.")
    except Exception as e:
        print(f"Error sending subscribe message: {e}")


def on_error(ws, error):
    print(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed. Status: {close_status_code}, Message: {close_msg}")


def subscribe_to_symbol(symbol):
    """Subscribe to a specific symbol"""
    global ws_app
    if ws_app and ws_app.sock and ws_app.sock.connected:
        subscribe_message = {
            "type": "subscribe",
            "symbol": symbol
        }
        try:
            ws_app.send(json.dumps(subscribe_message))
            print(f"Subscribing to {symbol}")
            return True
        except Exception as e:
            print(f"Error subscribing to {symbol}: {e}")
            return False
    else:
        print("WebSocket is not connected")
        return False


def unsubscribe_from_symbol(symbol):
    """Unsubscribe from a specific symbol"""
    global ws_app
    if ws_app and ws_app.sock and ws_app.sock.connected:
        unsubscribe_message = {
            "type": "unsubscribe",
            "symbol": symbol
        }
        try:
            ws_app.send(json.dumps(unsubscribe_message))
            print(f"Unsubscribing from {symbol}")
            return True
        except Exception as e:
            print(f"Error unsubscribing from {symbol}: {e}")
            return False
    else:
        print("WebSocket is not connected")
        return False


def is_running():
    """Check if WebSocket client is running"""
    global ws_thread
    return ws_thread is not None and ws_thread.is_alive()


def stop():
    """Stop the WebSocket client"""
    global ws_app, ws_thread
    
    if ws_app:
        print("Stopping WebSocket client...")
        try:
            if ws_app.sock and ws_app.sock.connected:
                ws_app.close()
        except Exception as e:
            print(f"Error closing WebSocket: {e}")
    
    if ws_thread and ws_thread.is_alive():
        ws_thread.join(5)  # Wait up to 5 seconds
        
    if ws_thread and ws_thread.is_alive():
        print("WebSocket thread is still alive after attempted close.")
    else:
        print("WebSocket client stopped successfully.")
        
    ws_app = None
    ws_thread = None


def run(timeout_hours=7, symbols=None):
    """
    Run the Finnhub WebSocket client
    
    Args:
        timeout_hours (int): How long to run in hours (default: 7)
        symbols (list): List of symbols to subscribe to (default: ["NVDA"])
    
    Returns:
        bool: True if started successfully, False otherwise
    """
    global ws_app, ws_thread
    
    if not API_KEY:
        print("Error: FINNHUB_TOKEN environment variable not set.")
        return False
    
    # Stop existing connection if running
    if is_running():
        print("WebSocket client is already running. Stopping existing connection...")
        stop()
        time.sleep(1)  # Give it a moment to clean up
    
    if symbols is None:
        symbols = ["NVDA"]
    
    websocket_url = f"wss://ws.finnhub.io?token={API_KEY}"
    
    # Create custom on_open function to subscribe to multiple symbols
    def custom_on_open(ws):
        print("Connected to Finnhub WebSocket")
        for symbol in symbols:
            subscribe_message = {
                "type": "subscribe",
                "symbol": symbol
            }
            try:
                ws.send(json.dumps(subscribe_message))
                print(f"Subscribing to {symbol}")
            except websocket.WebSocketConnectionClosedException:
                print(f"Failed to send subscribe message for {symbol}: WebSocket is closed.")
            except Exception as e:
                print(f"Error sending subscribe message for {symbol}: {e}")
    
    ws_app = websocket.WebSocketApp(websocket_url,
                                  on_open=custom_on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
    
    ws_thread = threading.Thread(target=ws_app.run_forever)
    ws_thread.daemon = True
    
    print(f"Starting WebSocket client for symbols: {symbols}")
    ws_thread.start()
    
    timeout_duration_seconds = timeout_hours * 60 * 60
    
    try:
        ws_thread.join(timeout_duration_seconds)
        
        if ws_thread.is_alive():
            print(f"Timeout of {timeout_hours} hours reached. Closing WebSocket...")
            ws_app.close()
            ws_thread.join(5)
        else:
            print("WebSocket client connection closed before timeout.")
            
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Closing WebSocket...")
        stop()
    
    return True


def run_async(timeout_hours=7, symbols=None):
    """
    Run the WebSocket client in background without blocking
    
    Args:
        timeout_hours (int): How long to run in hours (default: 7)
        symbols (list): List of symbols to subscribe to (default: ["NVDA"])
    
    Returns:
        bool: True if started successfully, False otherwise
    """
    def run_in_background():
        run(timeout_hours, symbols)
    
    background_thread = threading.Thread(target=run_in_background)
    background_thread.daemon = True
    background_thread.start()
    
    # Give it a moment to start
    time.sleep(1)
    return is_running()


if __name__ == "__main__":
    # Contoh penggunaan langsung
    run(timeout_hours=7, symbols=["NVDA"])