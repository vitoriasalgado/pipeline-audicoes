import boto3, json, io
import pandas as pd
 

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

linhas = []
for tr in ["short_term", "medium_term", "long_term"]:
    resposta = s3.get_object(Bucket="raw", Key=f"spotify/top_tracks_{tr}.json")
    conteudo = resposta["Body"].read().decode("utf-8")
    data = json.loads(conteudo)

    for posicao, item in enumerate(data["items"], start=1):
        linhas.append({
            "time_range": tr,
            "posicao": posicao,
            "faixa": item["name"],
            "artista": item["artists"][0]["name"],
            "album": item["album"]["name"],
            "spotify_track_id": item["id"],
            "spotify_artist_id": item["artists"][0]["id"]
        })

df = pd.DataFrame(linhas)
print(df.shape)

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
s3.put_object(
    Bucket="processed",
    Key="spotify/top_tracks.parquet",
    Body=buffer.getvalue()
)
print("gravado: spotify/top_tracks.parquet")

linhas_artistas = []
for tr in ["short_term", "medium_term", "long_term"]:
    resposta = s3.get_object(Bucket="raw", Key=f"spotify/top_artists_{tr}.json")
    conteudo = resposta["Body"].read().decode("utf-8")
    data = json.loads(conteudo)

    for posicao, item in enumerate(data["items"], start=1):
        linhas_artistas.append({
            "time_range": tr,
            "posicao": posicao,
            "artista": item["name"],
            "spotify_artist_id": item["id"]
        })

df = pd.DataFrame(linhas_artistas)
print(df.shape)

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
s3.put_object(
    Bucket="processed",
    Key="spotify/top_artists.parquet",
    Body=buffer.getvalue()
)
print("gravado: spotify/top_artists.parquet")

linhas_saved = []
resposta = s3.get_object(Bucket="raw", Key="spotify/saved_tracks.json")
conteudo = resposta["Body"].read().decode("utf-8")
data = json.loads(conteudo)
for item in (data["items"]):
        linhas_saved.append({
            "faixa": item["track"]["name"],
            "artista": item["track"]["artists"][0]["name"],
            "spotify_track_id": item["track"]["id"],
            "biblioteca_added_at": item["added_at"],
        })

df = pd.DataFrame(linhas_saved)
print(df.shape)

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
s3.put_object(
    Bucket="processed",
    Key="spotify/saved_tracks.parquet",
    Body=buffer.getvalue()
)
print("gravado: spotify/saved_tracks.parquet")