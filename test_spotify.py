import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

auth_manager = SpotifyOAuth(
    client_id=os.environ["SPOTIFY_CLIENT_ID"],
    client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
    scope="user-top-read user-library-read"
)

sp= spotipy.Spotify(auth_manager=auth_manager)

results = sp.current_user_top_tracks(limit=10, time_range="long_term")
for item in results["items"]:
    artist = item["artists"][0]["name"]
    track_name = item["name"]
    print(f"{artist} - {track_name}")