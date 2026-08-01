import boto3, json, io, psycopg2
import pandas as pd

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