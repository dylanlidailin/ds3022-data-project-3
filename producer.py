import os
import time
import json
import requests
import pandas as pd
from quixstreams import Application
from dotenv import load_dotenv
from pytrends.request import TrendReq

load_dotenv()

# --- CONFIGURATION ---
TRAKT_CLIENT_ID = os.getenv("TRAKT_CLIENT_ID")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")

if not TRAKT_CLIENT_ID or not TMDB_API_KEY:
    raise ValueError("❌ Missing API Keys! Check your .env file.")

# 30 Shows x 5 Years x 365 Days = ~55,000 Records
TARGET_SHOWS = [
    {"title": "Stranger Things", "slug": "stranger-things", "tmdb_id": 66732},
    {"title": "Wednesday", "slug": "wednesday", "tmdb_id": 119051},
    {"title": "The Last of Us", "slug": "the-last-of-us", "tmdb_id": 100088},
    {"title": "Severance", "slug": "severance", "tmdb_id": 115036},
    {"title": "The Mandalorian", "slug": "the-mandalorian", "tmdb_id": 82856},
    {"title": "House of the Dragon", "slug": "house-of-the-dragon", "tmdb_id": 94997},
    {"title": "Bridgerton", "slug": "bridgerton", "tmdb_id": 91239},
    {"title": "Squid Game", "slug": "squid-game", "tmdb_id": 93405},
    {"title": "The Crown", "slug": "the-crown", "tmdb_id": 65494},
    {"title": "The Witcher", "slug": "the-witcher", "tmdb_id": 71912},
    {"title": "Black Mirror", "slug": "black-mirror", "tmdb_id": 42009},
    {"title": "Better Call Saul", "slug": "better-call-saul", "tmdb_id": 60059},
    {"title": "Breaking Bad", "slug": "breaking-bad", "tmdb_id": 1396},
    {"title": "Game of Thrones", "slug": "game-of-thrones", "tmdb_id": 1399},
    {"title": "Succession", "slug": "succession", "tmdb_id": 76331},
    {"title": "Yellowstone", "slug": "yellowstone", "tmdb_id": 73586},
    {"title": "The Boys", "slug": "the-boys", "tmdb_id": 76479},
    {"title": "Ted Lasso", "slug": "ted-lasso", "tmdb_id": 97546},
    {"title": "The Bear", "slug": "the-bear", "tmdb_id": 136315},
    {"title": "Euphoria", "slug": "euphoria", "tmdb_id": 85552},
    {"title": "Rick and Morty", "slug": "rick-and-morty", "tmdb_id": 60625},
    {"title": "Arcane", "slug": "arcane", "tmdb_id": 94605},
    {"title": "Cyberpunk: Edgerunners", "slug": "cyberpunk-edgerunners", "tmdb_id": 105248},
    {"title": "One Piece", "slug": "one-piece", "tmdb_id": 37854},
    {"title": "Avatar: The Last Airbender", "slug": "avatar-the-last-airbender-2024", "tmdb_id": 82452},
    {"title": "3 Body Problem", "slug": "3-body-problem", "tmdb_id": 108545},
    {"title": "Fallout", "slug": "fallout", "tmdb_id": 106379},
    {"title": "Reacher", "slug": "reacher", "tmdb_id": 108978},
    {"title": "Invincible", "slug": "invincible", "tmdb_id": 95557},
    {"title": "Fargo", "slug": "fargo", "tmdb_id": 60622}
]

class FranchiseStreamer:
    def __init__(self):
        self.app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="franchise-producer-v6",
            producer_extra_config={"broker.address.family": "v4"}
        )
        self.topic = self.app.topic(name="franchise_metrics", value_serializer="json")
        print(f"🚀 Connected to Redpanda at {KAFKA_BROKER}")

    def backfill_google_trends(self, show_title):
        """Fetches last 5 years of daily search interest."""
        print(f"🔎 Google Trends Backfill for: {show_title}...")
        try:
            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload([show_title], cat=0, timeframe='today 5-y')
            data = pytrends.interest_over_time()
            
            if not data.empty:
                count = 0
                for index, row in data.iterrows():
                    event = {
                        "timestamp": index.timestamp(),
                        "title": show_title,
                        "metrics": {
                            # Using 'hype_score' to store Google Trends value (0-100)
                            "hype_score": int(row[show_title]), 
                            "active_watchers": 0,
                            "total_plays": 0,
                            "brand_equity": 0,
                            "cost_basis": 0,
                            "netflix_hours": 0
                        }
                    }
                    self.publish(key=show_title, data=event)
                    count += 1
                print(f"   ✅ Backfilled {count} daily records!")
            time.sleep(2) # Prevent rate limiting
        except Exception as e:
            print(f"   ❌ Google Trends Error: {e}")

    def get_trakt_stats(self, slug):
        url = f"https://api.trakt.tv/shows/{slug}/stats"
        headers = {'Content-Type': 'application/json', 'trakt-api-version': '2', 'trakt-api-key': TRAKT_CLIENT_ID}
        try:
            res = requests.get(url, headers=headers)
            return res.json() if res.status_code == 200 else {}
        except: return {}

    def get_tmdb_stats(self, tmdb_id):
        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}"
        try:
            res = requests.get(url)
            return res.json() if res.status_code == 200 else {}
        except: return {}

    def publish(self, key, data):
        msg = self.topic.serialize(key=key, value=data)
        with self.app.get_producer() as producer:
            producer.produce(topic=self.topic.name, key=msg.key, value=msg.value)

    def run(self):
        print("📊 Starting Business Intelligence Stream...")
        
        # --- MASSIVE BACKFILL ---
        print("\n--- STARTING GOOGLE TRENDS BACKFILL (Target: 50k+ Rows) ---")
        for show in TARGET_SHOWS:
            self.backfill_google_trends(show["title"])
        print("--- BACKFILL COMPLETE ---\n")

        print("🔴 Switching to Live Stream Mode...")
        while True:
            timestamp = time.time()
            for show in TARGET_SHOWS:
                trakt = self.get_trakt_stats(show["slug"])
                tmdb = self.get_tmdb_stats(show["tmdb_id"])
                
                if not trakt or not tmdb: continue

                event = {
                    "timestamp": timestamp,
                    "title": show["title"],
                    "metrics": {
                        "active_watchers": trakt.get("watchers", 0),
                        "total_plays": trakt.get("plays", 0),
                        "hype_score": tmdb.get("popularity", 0),
                        "brand_equity": tmdb.get("vote_count", 0),
                        "cost_basis": tmdb.get("number_of_seasons", 1),
                        "netflix_hours": 0
                    }
                }
                self.publish(key=show["slug"], data=event)
                print(f"   ✓ Live update: {show['title']}")
                
            time.sleep(60)

if __name__ == "__main__":
    streamer = FranchiseStreamer()
    streamer.run()