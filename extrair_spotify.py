import os, boto3, json, spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

auth_manager = SpotifyOAuth(
    client_id=os.environ["SPOTIFY_CLIENT_ID"],
    client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
    scope="user-top-read user-library-read"
)

def salvar(data, key):
    corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket="raw", Key=key, Body=corpo)
    print(f"gravado: {key}")

sp = spotipy.Spotify(auth_manager=auth_manager)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

for tr in ["short_term", "medium_term", "long_term"]:
    tracks = sp.current_user_top_tracks(limit=50, time_range=tr)
    salvar(tracks, f"spotify/top_tracks_{tr}.json")
    artists = sp.current_user_top_artists(limit=50, time_range=tr)
    salvar(artists, f"spotify/top_artists_{tr}.json")

saved = sp.current_user_saved_tracks(limit=50)
salvar(saved, "spotify/saved_tracks.json")