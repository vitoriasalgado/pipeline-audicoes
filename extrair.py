import requests, os, json
from dotenv import load_dotenv

load_dotenv()
API_Key = os.environ["LASTFM_API_KEY"]
USER = os.environ["LASTFM_USER"]

url = "https://ws.audioscrobbler.com/2.0/"
params = {
    "method": "user.getRecentTracks",
    "user": USER,
    "api_key": API_Key,
    "format": "json",
    "limit": 200,
}

response = requests.get(url, params=params, timeout=30)
response.raise_for_status()
data = response.json()

os.makedirs("data/raw", exist_ok=True)

with open("data/raw/recent.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

total_tracks = data["recenttracks"]["@attr"]["total"]

print(f"Salvo! você tem {total_tracks} scrobbles.")