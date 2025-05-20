import websocket
import json
import redis
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FINNHUB_TOKEN")
STREAM_NAME = "finnhub_stream"

r = redis.Redis(host="localhost", port=6379, db=0)

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for item in data.get("data", []):
            print(f"Trade: {item}")
            r.xadd(STREAM_NAME, {
                "s": item["s"],
                "p": str(item["p"]),
                "t": str(item["t"]),
                "v": str(item["v"])
            })

def on_open(ws):
    print("Connected to Finnhub")
    sub_msg = json.dumps({
        "type": "subscribe",
        "symbol": "NVDA"
    })
    print(f"Subscribing to NVDA")
    ws.send(sub_msg)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

if __name__ == "__main__":
    url = f"wss://ws.finnhub.io?token={API_KEY}"
    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()