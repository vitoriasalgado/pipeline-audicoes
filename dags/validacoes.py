"""Checagens de qualidade, rodadas depois da carga.

Cada checagem é uma pergunta que deve responder zero. Se alguma responder outra
coisa, a task falha — e o alerta dispara, porque a falha passa pelo mesmo
caminho de qualquer outra.

As perguntas saem dos defeitos que o projeto já teve: FK órfã na fato do
Spotify, dimensão fragmentada por caixa, e linha da prata que não chegou ao ouro.
"""

import io

import boto3
import pandas as pd
import psycopg2


def _conectar():
    conn = psycopg2.connect(
        host="warehouse",
        port=5432,
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
    )
    return conn, conn.cursor()


def _ler_parquet(chave):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    dados = s3.get_object(Bucket="processed", Key=chave)["Body"].read()
    return pd.read_parquet(io.BytesIO(dados))


def _rodar(cur, checagens):
    falhas = []
    for nome, sql, parametros in checagens:
        cur.execute(sql, parametros)
        valor = cur.fetchone()[0]
        marca = "ok   " if valor == 0 else "FALHA"
        print(f"  {marca} {nome}: {valor}", flush=True)
        if valor != 0:
            falhas.append(f"{nome} = {valor}")
    if falhas:
        raise ValueError("validacao reprovada: " + "; ".join(falhas))
    print("todas as checagens passaram", flush=True)


def validar_lastfm():
    conn, cur = _conectar()
    try:
        df = _ler_parquet("lastfm/recent.parquet")
        uts = [int(u) for u in df["scrobble_uts"].tolist()]

        _rodar(cur, [
            ("scrobbles da prata ausentes no ouro",
             """SELECT count(*) FROM unnest(%s::bigint[]) AS u(uts)
                 WHERE NOT EXISTS (SELECT 1 FROM fato_audicoes f
                                    WHERE f.scrobble_uts = u.uts)""",
             (uts,)),
            ("fato_audicoes com FK nula",
             "SELECT count(*) FROM fato_audicoes WHERE faixa_id IS NULL OR tempo_id IS NULL",
             None),
            ("scrobbles no futuro",
             "SELECT count(*) FROM fato_audicoes WHERE scrobble_uts > extract(epoch FROM now())",
             None),
            ("artistas duplicados por caixa",
             """SELECT count(*) FROM (SELECT lower(nome) FROM dim_artista
                 GROUP BY 1 HAVING count(*) > 1) x""",
             None),
            ("faixas duplicadas por caixa",
             """SELECT count(*) FROM (SELECT artista_id, lower(nome) FROM dim_faixa
                 GROUP BY 1, 2 HAVING count(*) > 1) x""",
             None),
        ])
    finally:
        cur.close()
        conn.close()


def validar_spotify(ti):
    conn, cur = _conectar()
    try:
        coletado_em = ti.xcom_pull(task_ids="extrair")["coletado_em"]
        esperado = len(_ler_parquet("spotify/top_tracks.parquet")) + \
                   len(_ler_parquet("spotify/top_artists.parquet"))

        _rodar(cur, [
            ("linhas do top que nao chegaram ao ouro",
             """SELECT %s - count(*) FROM fato_top_spotify
                 WHERE snapshot_date = %s""",
             (esperado, coletado_em)),
            ("fato_top_spotify com FK nula",
             """SELECT count(*) FROM fato_top_spotify
                 WHERE (tipo = 'track'  AND faixa_id   IS NULL)
                    OR (tipo = 'artist' AND artista_id IS NULL)""",
             None),
            ("artistas duplicados por caixa",
             """SELECT count(*) FROM (SELECT lower(nome) FROM dim_artista
                 GROUP BY 1 HAVING count(*) > 1) x""",
             None),
            ("faixas duplicadas por caixa",
             """SELECT count(*) FROM (SELECT artista_id, lower(nome) FROM dim_faixa
                 GROUP BY 1, 2 HAVING count(*) > 1) x""",
             None),
        ])
    finally:
        cur.close()
        conn.close()
