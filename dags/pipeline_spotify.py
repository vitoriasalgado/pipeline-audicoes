from datetime import datetime, timedelta, date
import json, os, requests, boto3, io, psycopg2, spotipy
import pandas as pd

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from spotipy.oauth2 import SpotifyOAuth

def extrair():

    auth_manager = SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
        scope="user-top-read user-library-read",
        cache_path="/opt/airflow/.cache",
        open_browser=False,   # sem navegador no container: usa o cache ou falha limpo
    )

    def salvar(data, key):
        corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket="raw", Key=key, Body=corpo)
        print(f"gravado: {key}")

    sp = spotipy.Spotify(auth_manager=auth_manager) #type: ignore

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
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
    
def transformar():

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
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
    for item in data["items"]:
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

def carregar():

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    psycopg2_conn = psycopg2.connect(
        host="warehouse",
        port=5432,
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

with DAG(
    dag_id="pipeline_spotify",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["spotify"],
) as dag:
    extrair_task = PythonOperator(
        task_id="extrair",
        python_callable=extrair,
    )
    transformar_task = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
    )
    carregar_task = PythonOperator(
        task_id="carregar",
        python_callable=carregar,
    )
    extrair_task >> transformar_task >> carregar_task

