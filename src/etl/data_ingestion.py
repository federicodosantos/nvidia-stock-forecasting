import websocket
import json
import redis
import os
from dotenv import load_dotenv
import threading
import time

load_dotenv()

API_KEY = os.getenv("FINNHUB_TOKEN")
STREAM_NAME = os.getenv("FINNHUB_DB")

# Global variables for WebSocket
ws_app = None
ws_thread = None

# Redis connection setup
try:
    # Assumes Redis is running on the specified host and port
    r = redis.Redis(host="nvidia-stock-forecasting-redis-1", port=6379, db=0, decode_responses=True)
    r.ping()
    print("Connected to Redis successfully.")
except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis: {e}")
    r = None


def on_message(ws, message):
    """Callback function to handle incoming WebSocket messages."""
    data = json.loads(message)
    if data.get("type") == "trade":
        for item in data.get("data", []):
            print(f"Trade: {item}")
            if r:
                try:
                    # Add trade data to the Redis Stream
                    r.xadd(STREAM_NAME, {
                        "s": item["s"],
                        "p": str(item["p"]),
                        "t": str(item["t"]),
                        "v": str(item["v"])
                    })
                except redis.exceptions.RedisError as e:
                    print(f"Redis XADD error: {e}")


def on_open(ws):
    """Callback function for when the WebSocket connection is opened."""
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
    """Callback function for WebSocket errors."""
    print(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Callback function for when the WebSocket connection is closed."""
    print(f"WebSocket closed. Status: {close_status_code}, Message: {close_msg}")


def subscribe_to_symbol(symbol):
    """Subscribes to a specific symbol while the connection is active."""
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
    """Unsubscribes from a specific symbol."""
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
    """Checks if the WebSocket client thread is running."""
    global ws_thread
    return ws_thread is not None and ws_thread.is_alive()


def stop():
    """Stops the WebSocket client and sets a 2-hour expiry on the Redis stream."""
    global ws_app, ws_thread
    
    # 1. Close the WebSocket connection
    if ws_app and ws_app.sock and ws_app.sock.connected:
        print("Stopping WebSocket client...")
        try:
            ws_app.close()
        except Exception as e:
            print(f"Error closing WebSocket: {e}")
    
    # 2. Wait for the thread to terminate
    if ws_thread and ws_thread.is_alive():
        ws_thread.join(5)  # Wait up to 5 seconds
        
    if ws_thread and ws_thread.is_alive():
        print("WebSocket thread is still alive after attempted close.")
    else:
        print("WebSocket client stopped successfully.")
    
    # Reset global variables
    ws_app = None
    ws_thread = None
    
    # 3. Set a 2-hour expiry on the Redis stream after stopping
    if r:
        try:
            # TTL in seconds (2 hours * 60 minutes/hour * 60 seconds/minute)
            expiry_seconds = 2 * 60 * 60
            # The EXPIRE command sets a timeout on a key
            if r.exists(STREAM_NAME):
                r.expire(STREAM_NAME, expiry_seconds)
                print(f"Redis stream '{STREAM_NAME}' is set to expire in 2 hours.")
            else:
                print(f"Redis stream '{STREAM_NAME}' does not exist. No expiry set.")
        except redis.exceptions.RedisError as e:
            print(f"Could not set expiry on Redis stream: {e}")
            


def run(timeout_hours=7, symbols=None):
    """
    Runs the Finnhub WebSocket client.
    
    Args:
        timeout_hours (int): How long to run in hours before stopping.
        symbols (list): A list of stock symbols to subscribe to.
    
    Returns:
        bool: True if started successfully, False otherwise.
    """
    global ws_app, ws_thread
    
    if not API_KEY:
        print("Error: FINNHUB_TOKEN environment variable not set.")
        return False
    
    if is_running():
        print("WebSocket client is already running. Stopping the existing connection first.")
        stop()
        time.sleep(1)  # Give a moment for resources to be released
    
    if symbols is None:
        symbols = ["NVDA"]
    
    websocket_url = f"wss://ws.finnhub.io?token={API_KEY}"
    
    # This custom on_open function allows subscribing to multiple symbols at startup
    def custom_on_open(ws):
        print("Connection opened. Subscribing to symbols...")
        for symbol in symbols:
            subscribe_to_symbol(symbol)
    
    ws_app = websocket.WebSocketApp(websocket_url,
                                  on_open=custom_on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
    
    ws_thread = threading.Thread(target=ws_app.run_forever)
    ws_thread.daemon = True
    
    print(f"Starting WebSocket client for symbols: {symbols}")
    ws_thread.start()
    
    # After starting, remove any existing expiry on the stream key
    if r and r.exists(STREAM_NAME):
        r.persist(STREAM_NAME)
        print(f"Removed any existing expiry from '{STREAM_NAME}'.")
        
    timeout_duration_seconds = timeout_hours * 60 * 60
    
    try:
        # This will block until the thread finishes or the timeout occurs
        ws_thread.join(timeout_duration_seconds)
        
        # If the thread is still alive after the timeout, it means the timeout was reached
        if ws_thread.is_alive():
            print(f"Timeout of {timeout_hours} hours reached. Shutting down.")
            stop()
        else:
            print("WebSocket client connection closed before timeout.")
            # Call stop() to ensure the expiry is set even if closed unexpectedly
            stop()
            
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Shutting down.")
        stop()
    
    return True


if __name__ == "__main__":
    # Example: Run for a very short period for demonstration
    # In a real scenario, you would use a longer timeout, e.g., timeout_hours=7
    print("Starting Finnhub client. It will run for 1 minute.")
    print("Press Ctrl+C to stop it earlier.")
    run(timeout_hours=(1/60), symbols=["NVDA", "AAPL", "BINANCE:BTCUSDT"])
    print("Script finished.")
