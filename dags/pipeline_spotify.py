from datetime import datetime, timedelta, date
import json, os, boto3, io, psycopg2, spotipy
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from spotipy.oauth2 import SpotifyOAuth

from alertas import avisar_falha
from validacoes import validar_spotify

def extrair(ts_nodash):

    auth_manager = SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
        scope="user-top-read user-library-read",
        cache_path="/opt/airflow/.cache",
        open_browser=False,   # sem navegador no container: usa o cache ou falha limpo
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    prefixo = f"spotify/{ts_nodash}/"

    def salvar(data, nome):
        corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket="raw", Key=f"{prefixo}{nome}", Body=corpo)
        print(f"gravado: {prefixo}{nome}", flush=True)

    for tr in ["short_term", "medium_term", "long_term"]:
        salvar(sp.current_user_top_tracks(limit=50, time_range=tr), f"top_tracks_{tr}.json")
        salvar(sp.current_user_top_artists(limit=50, time_range=tr), f"top_artists_{tr}.json")

    pagina = 1
    resposta = sp.current_user_saved_tracks(limit=50)
    total = resposta["total"]
    salvar(resposta, f"saved_tracks_{pagina:04d}.json")
    while resposta["next"]:
        resposta = sp.next(resposta)
        pagina += 1
        salvar(resposta, f"saved_tracks_{pagina:04d}.json")
    print(f"biblioteca: {total} faixas em {pagina} pagina(s)", flush=True)

    return {"prefixo": prefixo, "coletado_em": date.today().isoformat()}


def transformar(ti):

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    prefixo = ti.xcom_pull(task_ids="extrair")["prefixo"]
    print(f"lendo de {prefixo}", flush=True)

    def ler_json(nome):
        conteudo = s3.get_object(Bucket="raw", Key=f"{prefixo}{nome}")["Body"].read()
        return json.loads(conteudo.decode("utf-8"))

    linhas = []
    for tr in ["short_term", "medium_term", "long_term"]:
        data = ler_json(f"top_tracks_{tr}.json")

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
        data = ler_json(f"top_artists_{tr}.json")

        for posicao, item in enumerate(data["items"], start=1):
            linhas_artistas.append({
                "time_range": tr,
                "posicao": posicao,
                "artista": item["name"],
                "spotify_artist_id": item["id"]
            })

    df = pd.DataFrame(linhas_artistas)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(
        Bucket="processed",
        Key="spotify/top_artists.parquet",
        Body=buffer.getvalue()
    )
    print("gravado: spotify/top_artists.parquet")

    linhas_saved = []
    paginas = s3.list_objects_v2(Bucket="raw", Prefix=f"{prefixo}saved_tracks_")
    for obj in paginas["Contents"]:
        conteudo = s3.get_object(Bucket="raw", Key=obj["Key"])["Body"].read()
        for item in json.loads(conteudo.decode("utf-8"))["items"]:
            linhas_saved.append({
                "faixa": item["track"]["name"],
                "artista": item["track"]["artists"][0]["name"],
                "spotify_track_id": item["track"]["id"],
                "biblioteca_added_at": item["added_at"],
            })

    df = pd.DataFrame(linhas_saved)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(
        Bucket="processed",
        Key="spotify/saved_tracks.parquet",
        Body=buffer.getvalue()
    )
    print("gravado: spotify/saved_tracks.parquet")

def carregar(ti):

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

    def ler_parquet(chave):
        dados = s3.get_object(Bucket="processed", Key=chave)["Body"].read()
        return pd.read_parquet(io.BytesIO(dados))

    def upsert_artista(nome, spotify_artist_id=None):
        cur.execute(
            """
            INSERT INTO dim_artista (nome, spotify_artist_id) VALUES (%s, %s)
            ON CONFLICT (lower(nome)) DO UPDATE
               SET spotify_artist_id = COALESCE(EXCLUDED.spotify_artist_id,
                                                dim_artista.spotify_artist_id)
            RETURNING id;
            """,
            (nome, spotify_artist_id),
        )
        return cur.fetchone()[0]

    def upsert_faixa(nome, album, artista_id, spotify_track_id=None):
        cur.execute(
            """
            INSERT INTO dim_faixa (nome, album, artista_id, spotify_track_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (lower(nome), artista_id) DO UPDATE
               SET spotify_track_id = COALESCE(EXCLUDED.spotify_track_id,
                                               dim_faixa.spotify_track_id),
                   album = COALESCE(NULLIF(dim_faixa.album, ''), NULLIF(EXCLUDED.album, ''))
            RETURNING id;
            """,
            (nome, album, artista_id, spotify_track_id),
        )
        return cur.fetchone()[0]

    coletado_em = ti.xcom_pull(task_ids="extrair")["coletado_em"]

    # --- top de artistas ---
    df_artistas = ler_parquet("spotify/top_artists.parquet")
    print(f"top_artists: {len(df_artistas)} linhas", flush=True)

    for index, row in df_artistas.iterrows():
        artista_id = upsert_artista(row["artista"], row["spotify_artist_id"])
        cur.execute(
            """
            INSERT INTO fato_top_spotify (snapshot_date, time_range, tipo, posicao, artista_id)
            VALUES (%s, %s, 'artist', %s, %s)
            ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING;
            """,
            (coletado_em, row["time_range"], row["posicao"], artista_id),
        )

    # --- top de faixas ---
    df_faixas = ler_parquet("spotify/top_tracks.parquet")
    print(f"top_tracks: {len(df_faixas)} linhas", flush=True)

    for index, row in df_faixas.iterrows():
        artista_id = upsert_artista(row["artista"], row["spotify_artist_id"])
        faixa_id = upsert_faixa(row["faixa"], row["album"], artista_id,
                                row["spotify_track_id"])
        cur.execute(
            """
            INSERT INTO fato_top_spotify (snapshot_date, time_range, tipo, posicao, faixa_id)
            VALUES (%s, %s, 'track', %s, %s)
            ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING;
            """,
            (coletado_em, row["time_range"], row["posicao"], faixa_id),
        )

    # --- biblioteca salva ---
    df_saved = ler_parquet("spotify/saved_tracks.parquet")
    print(f"saved_tracks: {len(df_saved)} linhas", flush=True)

    cur.execute("UPDATE dim_faixa SET na_biblioteca = FALSE, biblioteca_added_at = NULL "
                "WHERE na_biblioteca;")

    for index, row in df_saved.iterrows():
        artista_id = upsert_artista(row["artista"])
        faixa_id = upsert_faixa(row["faixa"], None, artista_id,
                                row["spotify_track_id"])
        cur.execute(
            "UPDATE dim_faixa SET na_biblioteca = TRUE, biblioteca_added_at = %s WHERE id = %s;",
            (row["biblioteca_added_at"], faixa_id),
        )

    psycopg2_conn.commit()
    cur.close()
    psycopg2_conn.close()
    print("carga do Spotify concluida", flush=True)

with DAG(
    dag_id="pipeline_spotify",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": avisar_falha,
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
    validar_task = PythonOperator(
        task_id="validar",
        python_callable=validar_spotify,
    )
    extrair_task >> transformar_task >> carregar_task >> validar_task

