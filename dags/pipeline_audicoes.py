from datetime import datetime, timedelta
import json, os, requests, boto3, io, psycopg2
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

def extrair_para_minio():
    api_key = os.environ['LASTFM_API_KEY']
    username = os.environ['LASTFM_USER']

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getRecentTracks",
        "user": username,
        "api_key": api_key,
        "format": "json",
        "limit": 200
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket="raw", Key="lastfm/recent.json", Body=corpo)
    print("Ingestão concluída: lastfm/recent.json no bucket raw")
def transformar():

    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    resposta = s3.get_object(Bucket="raw", Key="lastfm/recent.json")
    conteudo = resposta["Body"].read().decode("utf-8")
    data = json.loads(conteudo)
    lista_de_faixas = data["recenttracks"]["track"]
    df = pd.json_normalize(lista_de_faixas)
    df = df[["name", "artist.#text", "album.#text", "date.uts", "mbid", "artist.mbid"]]
    df.rename(columns={
        "name": "faixa",
        "artist.#text": "artista",
        "album.#text": "album",
        "date.uts": "scrobbles_uts",
        "mbid": "faixa_mbid",
        "artist.mbid": "artista_mbid"
    }, inplace=True)
    print(df["scrobbles_uts"].isna().sum())

    df["scrobble_uts"] = pd.to_numeric(df["scrobbles_uts"], errors="coerce")
    df["data_hora"] = pd.to_datetime(df["scrobble_uts"], unit="s", errors="coerce")
    df = df.dropna(subset=["scrobble_uts"])   # descarta o nowplaying (vem sem date)
    df = df.drop_duplicates(subset=["scrobble_uts", "faixa"])
    df = df.drop(columns=["scrobbles_uts"])

    print(df.dtypes)
    print(df.columns)
    print(df.head())
    print(len(df))

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(
        Bucket="processed",
        Key="lastfm/recent.parquet",
        Body=buffer.getvalue()
    )
    print("Parquet gravado em processed/lastfm/recent.parquet")

def carregar():

    s3 = boto3.client(
        's3',
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
    cur.execute("SELECT count(*) FROM fato_audicoes;")

    resposta = s3.get_object(Bucket="processed", Key="lastfm/recent.parquet")
    dados = resposta["Body"].read()

    df_check = pd.read_parquet(io.BytesIO(dados))

    for index, row in df_check.iterrows():
        cur.execute(
            "INSERT INTO dim_artista (nome, mbid) VALUES (%s, %s) ON CONFLICT (nome) DO UPDATE SET mbid = EXCLUDED.mbid " \
            "RETURNING id",
            (row["artista"], row["artista_mbid"]),
        )
        artista_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO dim_faixa (nome, album, artista_id) VALUES (%s, %s, %s)
            ON CONFLICT (nome, artista_id) DO UPDATE SET album = EXCLUDED.album
            RETURNING id;
            """,
            (row["faixa"], row["album"], artista_id),
        )
        faixa_id = cur.fetchone()[0]

        dt = row["data_hora"]
        cur.execute(
            """
            INSERT INTO dim_tempo (data, hora, ano, mes, dia, dia_semana)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (data, hora) DO UPDATE SET ano = EXCLUDED.ano
            RETURNING id;
            """,
            (dt.date(), dt.hour, dt.year, dt.month, dt.day, dt.dayofweek),
        )
        tempo_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO fato_audicoes (scrobble_uts, faixa_id, tempo_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (scrobble_uts, faixa_id) DO NOTHING;
            """,
            (row["scrobble_uts"], faixa_id, tempo_id),
    )

    psycopg2_conn.commit()

with DAG(
    dag_id="pipeline_audicoes",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["lastfm"],
) as dag:
    extrair_task = PythonOperator(
        task_id="extrair",
        python_callable=extrair_para_minio,
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


