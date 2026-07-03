import os
import requests
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
    "limit": 10,
}

response = requests.get(url, params=params, timeout=30)
response.raise_for_status()
data = response.json()

tracks = data["recenttracks"]["track"]
for t in tracks:
    artist = t["artist"]["#text"]
    track_name = t["name"]
    print(f"{artist} - {track_name}")
    