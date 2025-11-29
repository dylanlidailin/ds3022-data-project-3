import json
import os
import redis
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

# 1. Setup Quixstreams
app = Application(
    broker_address=os.getenv("KAFKA_BROKER", "localhost:19092"),
    consumer_group="franchise-consumer-redis-v1", 
    auto_offset_reset="earliest",
    producer_extra_config={"broker.address.family": "v4"}
)

# 2. Setup Redis
# host='localhost' if running locally, 'redis' if inside docker
r = redis.Redis(host='localhost', port=6379, db=0)

print("👷 Consumer Started... Writing to Redis List 'franchise_data'...")

total_processed = 0

with app.get_consumer() as consumer:
    consumer.subscribe(["mega_franchise_metrics"])

    while True:
        msg = consumer.poll(1)
        if msg is None or msg.error(): continue

        # Get data
        data = json.loads(msg.value())
        m = data.get('metrics', {})
        
        # Flatten it for easy Pandas loading later
        record = {
            "timestamp": data.get('timestamp'),
            "title": data.get('title'),
            "active_watchers": m.get('active_watchers', 0),
            "total_plays": m.get('total_plays', 0),
            "hype_score": m.get('hype_score', 0.0),
            "brand_equity": m.get('brand_equity', 0),
            "cost_basis": m.get('cost_basis', 1),
            "netflix_hours": m.get('netflix_hours', 0)
        }

        # WRITE TO REDIS (Push to list)
        r.rpush("franchise_data", json.dumps(record))
        
        total_processed += 1
        if total_processed % 1000 == 0:
            print(f"   📥 To Redis: {data.get('title')} (Total: {total_processed})")
            
        consumer.store_offsets(msg)