import redis
import json

def extract_from_redis(stream_name="finnhub_stream", count=10000):
    r = redis.Redis(host="localhost", port=6379, db=0)
    stream_data = r.xread({stream_name: '0'}, count=count)
    
    records = []
    if stream_data and len(stream_data) > 0 and stream_data[0][1]:
        for message_id, message_data in stream_data[0][1]:
            item = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}
            records.append({
                "s": item["s"],
                "p": float(item["p"]),
                "v": float(item["v"]),
                "t": int(item["t"])
            })
    return records