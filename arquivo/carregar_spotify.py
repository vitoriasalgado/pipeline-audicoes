import boto3, json, io, psycopg2
import pandas as pd
from datetime import date

s3 = boto3.client(
    's3',
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

psycopg2_conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="warehouse",
    user="warehouse",
    password="warehouse",
)

cur = psycopg2_conn.cursor()

resposta = s3.get_object(Bucket="processed", Key="spotify/top_artists.parquet")
dados = resposta["Body"].read()
df_artistas = pd.read_parquet(io.BytesIO(dados))
print(f"parquet lido: {len(df_artistas)} linhas", flush=True)

for index, row in df_artistas.iterrows():
    cur.execute(
        "UPDATE dim_artista SET spotify_artist_id = %s WHERE nome = %s",
        (row["spotify_artist_id"], row["artista"]),
    )

psycopg2_conn.commit()
print("artistas gravados!", flush=True)

resposta = s3.get_object(Bucket="processed", Key="spotify/top_tracks.parquet")
dados = resposta["Body"].read()
df_faixas = pd.read_parquet(io.BytesIO(dados))
print(f"parquet de faixas lido: {len(df_faixas)} linhas", flush=True)

for index, row in df_faixas.iterrows():
    cur.execute(
        """
        UPDATE dim_faixa SET spotify_track_id = %s
        WHERE nome = %s
          AND artista_id = (SELECT id FROM dim_artista WHERE nome = %s)
        """,
        (row["spotify_track_id"], row["faixa"], row["artista"]),
    )

psycopg2_conn.commit()
print("faixas gravadas!", flush=True)

resposta = s3.get_object(Bucket="processed", Key="spotify/saved_tracks.parquet")
dados = resposta["Body"].read()
df_saved = pd.read_parquet(io.BytesIO(dados))
print(f"parquet de biblioteca lido: {len(df_saved)} linhas", flush=True)

for index, row in df_saved.iterrows():
    cur.execute(
        """
        UPDATE dim_faixa SET na_biblioteca = TRUE, biblioteca_added_at = %s
        WHERE nome = %s
          AND artista_id = (SELECT id FROM dim_artista WHERE nome = %s)
        """,
        (row["biblioteca_added_at"], row["faixa"], row["artista"]),
    )

psycopg2_conn.commit()
print("biblioteca gravada!", flush=True)

hoje = date.today()
for index, row in df_artistas.iterrows():
    cur.execute(
        """
        INSERT INTO fato_top_spotify (snapshot_date, time_range, tipo, posicao, artista_id)
        VALUES (%s, %s, 'artist', %s, (SELECT id FROM dim_artista WHERE nome = %s))
        ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING
        """,
        (hoje, row["time_range"], row["posicao"], row["artista"]),
    )

psycopg2_conn.commit()
print("fato artistas gravada!", flush=True)

for index, row in df_faixas.iterrows():
    cur.execute(
        """
        INSERT INTO fato_top_spotify (snapshot_date, time_range, tipo, posicao, faixa_id)
        VALUES (%s, %s, 'track', %s,
                (SELECT id FROM dim_faixa
                 WHERE nome = %s
                   AND artista_id = (SELECT id FROM dim_artista WHERE nome = %s)))
        ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING
        """,
        (hoje, row["time_range"], row["posicao"], row["faixa"], row["artista"]),
    )

psycopg2_conn.commit()
print("fato faixas gravada!", flush=True)