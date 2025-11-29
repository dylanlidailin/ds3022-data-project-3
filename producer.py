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

# THE MEGA LIST: 50+ Global Hits
SHOW_NAMES = [
    "Stranger Things", "Wednesday", "The Last of Us", "Game of Thrones", "Breaking Bad",
    "Better Call Saul", "The Crown", "Bridgerton", "Squid Game", "Yellowstone",
    "The Mandalorian", "Andor", "Obi-Wan Kenobi", "The Witcher", "House of the Dragon",
    "The Rings of Power", "Westworld", "Black Mirror", "Severance", "Silo",
    "Fallout", "3 Body Problem", "Dark", "Arcane", "Cyberpunk: Edgerunners",
    "Succession", "The Bear", "Euphoria", "The White Lotus", "Peaky Blinders",
    "The Sopranos", "The Wire", "Mad Men", "Narcos", "Money Heist",
    "Mindhunter", "Reacher", "The Night Agent", "Ozark", "Shameless",
    "The Office", "Parks and Recreation", "Ted Lasso", "Friends", "Seinfeld",
    "Brooklyn Nine-Nine", "It's Always Sunny in Philadelphia", "Rick and Morty",
    "BoJack Horseman", "Fleabag", "Barry", "The Good Place", "Community"
]

class FranchiseStreamer:
    def __init__(self):
        self.app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="franchise-producer-mega",
            producer_extra_config={"broker.address.family": "v4"}
        )
        # NEW TOPIC NAME to avoid conflict with Project 1
        self.topic = self.app.topic(name="mega_franchise_metrics", value_serializer="json")
        self.resolved_shows = [] 
        print(f"🚀 Connected to Redpanda at {KAFKA_BROKER}")

    def resolve_show_metadata(self, title):
        search_url = f"https://api.themoviedb.org/3/search/tv?api_key={TMDB_API_KEY}&query={title}"
        try:
            res = requests.get(search_url).json()
            if res.get('results'):
                top = res['results'][0]
                return {
                    "title": top['name'],
                    "tmdb_id": top['id'],
                    "slug": top['name'].lower().replace(' ', '-').replace(':', '').replace("'", "")
                }
        except: pass
        return None

    def backfill_google_trends(self, show_title):
        print(f"   🔎 Google Trends: {show_title}...")
        try:
            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload([show_title], cat=0, timeframe='today 5-y')
            data = pytrends.interest_over_time()
            
            if not data.empty:
                for index, row in data.iterrows():
                    event = {
                        "timestamp": index.timestamp(),
                        "title": show_title,
                        "metrics": {
                            "hype_score": int(row[show_title]), 
                            "active_watchers": 0, "total_plays": 0,
                            "brand_equity": 0, "cost_basis": 0, "netflix_hours": 0
                        }
                    }
                    self.publish(key=show_title, data=event)
                print(f"      ✅ +{len(data)} records")
            time.sleep(1.5) 
        except: print(f"      ❌ Skipped (Rate Limit)")

    def backfill_netflix_history(self, show_title):
        url = "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv"
        try:
            df = pd.read_csv(url, sep='\t')
            show_data = df[df['show_title'].str.contains(show_title, case=False, na=False)]
            if not show_data.empty:
                for _, row in show_data.iterrows():
                    event = {
                        "timestamp": pd.to_datetime(row['week']).timestamp(),
                        "title": show_title,
                        "metrics": {
                            "active_watchers": 0, "total_plays": 0, "hype_score": 0,
                            "brand_equity": 0, "cost_basis": 0,
                            "netflix_hours": int(row['weekly_hours_viewed'])
                        }
                    }
                    self.publish(key=show_title, data=event)
                print(f"      ✅ +{len(show_data)} Netflix records")
        except: pass

    def get_trakt_stats(self, slug):
        url = f"https://api.trakt.tv/shows/{slug}/stats"
        headers = {'Content-Type': 'application/json', 'trakt-api-version': '2', 'trakt-api-key': TRAKT_CLIENT_ID}
        try: return requests.get(url, headers=headers).json()
        except: return {}

    def get_tmdb_stats(self, tmdb_id):
        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}"
        try:
            res = requests.get(url).json()
            return {"popularity": res.get("popularity"), "vote_count": res.get("vote_count"), "season_count": res.get("number_of_seasons", 1)}
        except: return {}

    def publish(self, key, data):
        msg = self.topic.serialize(key=key, value=data)
        with self.app.get_producer() as producer:
            producer.produce(topic=self.topic.name, key=msg.key, value=msg.value)

    def run(self):
        print("📊 Starting MEGA-STREAMER...")
        
        print(f"\n--- 1. RESOLVING METADATA FOR {len(SHOW_NAMES)} SHOWS ---")
        for name in SHOW_NAMES:
            meta = self.resolve_show_metadata(name)
            if meta:
                self.resolved_shows.append(meta)
                print(f"   Found ID {meta['tmdb_id']}: {meta['title']}")
            time.sleep(0.1)

        print(f"\n--- 2. STARTING MASSIVE BACKFILL ---")
        for show in self.resolved_shows:
            self.backfill_netflix_history(show['title'])
            self.backfill_google_trends(show['title'])
        print("--- BACKFILL COMPLETE ---\n")

        print("🔴 Switching to Live Stream Mode...")
        while True:
            timestamp = time.time()
            for show in self.resolved_shows:
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
                        "cost_basis": tmdb.get("season_count", 1),
                        "netflix_hours": 0
                    }
                }
                self.publish(key=show["slug"], data=event)
            
            print(f"   ...updated {len(self.resolved_shows)} shows.")
            time.sleep(60)

if __name__ == "__main__":
    streamer = FranchiseStreamer()
    streamer.run()