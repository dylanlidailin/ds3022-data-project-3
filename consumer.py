import duckdb
import json
import os
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

app = Application(
    broker_address=os.getenv("KAFKA_BROKER", "localhost:19092"),
    consumer_group="franchise-consumer-v6",
    auto_offset_reset="earliest",
    producer_extra_config={"broker.address.family": "v4"}
)

# Initialize Table
with duckdb.connect('franchise_data.db') as db:
    db.execute("""
        CREATE TABLE IF NOT EXISTS franchise_metrics (
            timestamp DOUBLE, 
            title VARCHAR, 
            active_watchers INTEGER,
            total_plays INTEGER,
            hype_score DOUBLE,
            brand_equity INTEGER,
            cost_basis INTEGER,
            netflix_hours BIGINT
        )
    """)

print("👷 Consumer Started... Waiting for Metrics...")

with app.get_consumer() as consumer:
    consumer.subscribe(["franchise_metrics"])

    while True:
        msg = consumer.poll(1)
        if msg is None or msg.error(): continue

        data = json.loads(msg.value())
        m = data.get('metrics', {})
        
        with duckdb.connect('franchise_data.db') as db:
            db.execute("""
            INSERT INTO franchise_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get('timestamp'), 
                data.get('title'),
                m.get('active_watchers', 0),
                m.get('total_plays', 0),
                m.get('hype_score', 0.0),
                m.get('brand_equity', 0),
                m.get('cost_basis', 1),
                m.get('netflix_hours', 0)
            ))
        
        # Don't print every single historical insert (too spammy)
        # Only print live updates
        if m.get('active_watchers', 0) > 0:
            print(f"   📥 Live Update: {data.get('title')}")
            
        consumer.store_offsets(msg)