"""
backfill.py — carga histórica (one-off), rodada no host (não é a DAG).

Puxa TODAS as páginas do histórico do Last.fm (~305 de 200) e roda as três
camadas medalhão numa tacada:
  1. bronze — grava cada página como um objeto imutável no bucket `raw`
  2. prata  — junta tudo, limpa e grava um Parquet no bucket `processed`
  3. ouro   — carga em LOTE no Postgres warehouse (dims + fato), idempotente

Diferente do `carregar.py` da DAG (upsert linha a linha, bom pra 200 linhas),
aqui a carga é em lote com execute_values — senão 60k linhas viram minutos.

Rodar no host, com a venv ativa:  python backfill.py
"""

import os
import io
import json
import time

import boto3
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

LASTFM_API_KEY = os.environ["LASTFM_API_KEY"]
LASTFM_USER = os.environ["LASTFM_USER"]

API_URL = "https://ws.audioscrobbler.com/2.0/"
PAGINA_TAMANHO = 200
PAUSA_ENTRE_PAGINAS = 0.25  # segundos — educação com a API do Last.fm

# Clientes de infra (rodando no host → localhost)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


# ---------------------------------------------------------------------------
# 1. BRONZE — extrair todas as páginas e gravar cada uma no bucket `raw`
# ---------------------------------------------------------------------------
def buscar_pagina(pagina, tentativas=3):
    """Pega uma página do getRecentTracks, com retry simples em caso de falha de rede."""
    params = {
        "method": "user.getRecentTracks",
        "user": LASTFM_USER,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": PAGINA_TAMANHO,
        "page": pagina,
    }
    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as erro:
            if tentativa == tentativas:
                raise
            print(f"  ! página {pagina} falhou ({erro}); tentando de novo ({tentativa}/{tentativas})")
            time.sleep(2)


def extrair_tudo():
    """Percorre todas as páginas, grava o bronze no MinIO e devolve a lista completa de faixas."""
    # A primeira página nos diz quantas existem no total (@attr.totalPages).
    primeira = buscar_pagina(1)
    total_paginas = int(primeira["recenttracks"]["@attr"]["totalPages"])
    total_faixas = primeira["recenttracks"]["@attr"]["total"]
    print(f"Histórico: {total_faixas} scrobbles em {total_paginas} páginas.\n")

    todas_as_faixas = []

    def guardar(pagina, payload):
        # bronze imutável: uma página = um objeto
        chave = f"lastfm/backfill/page_{pagina:04d}.json"
        corpo = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket="raw", Key=chave, Body=corpo)
        todas_as_faixas.extend(payload["recenttracks"]["track"])

    guardar(1, primeira)
    print(f"  página 1/{total_paginas} ✓")

    for pagina in range(2, total_paginas + 1):
        time.sleep(PAUSA_ENTRE_PAGINAS)
        payload = buscar_pagina(pagina)
        guardar(pagina, payload)
        if pagina % 10 == 0 or pagina == total_paginas:
            print(f"  página {pagina}/{total_paginas} ✓")

    print(f"\nBronze pronto: {len(todas_as_faixas)} registros brutos no bucket raw.")
    return todas_as_faixas


# ---------------------------------------------------------------------------
# 2. PRATA — achatar, limpar e gravar o Parquet no bucket `processed`
#    (mesma lógica do transformar.py, aplicada ao histórico inteiro)
# ---------------------------------------------------------------------------
def transformar(lista_de_faixas):
    df = pd.json_normalize(lista_de_faixas)
    df = df[["name", "artist.#text", "album.#text", "date.uts", "mbid", "artist.mbid"]]
    df = df.rename(columns={
        "name": "faixa",
        "artist.#text": "artista",
        "album.#text": "album",
        "date.uts": "scrobbles_uts",
        "mbid": "faixa_mbid",
        "artist.mbid": "artista_mbid",
    })

    df["scrobble_uts"] = pd.to_numeric(df["scrobbles_uts"], errors="coerce")
    df["data_hora"] = pd.to_datetime(df["scrobble_uts"], unit="s", errors="coerce")
    df = df.dropna(subset=["scrobble_uts"])          # descarta o nowplaying (sem date)
    df = df.drop_duplicates(subset=["scrobble_uts", "faixa"])
    df = df.drop(columns=["scrobbles_uts"])

    # colunas de tempo, derivadas da data_hora (alimentam a dim_tempo)
    df["data"] = df["data_hora"].dt.date
    df["hora"] = df["data_hora"].dt.hour.astype(int)
    df["ano"] = df["data_hora"].dt.year.astype(int)
    df["mes"] = df["data_hora"].dt.month.astype(int)
    df["dia"] = df["data_hora"].dt.day.astype(int)
    df["dia_semana"] = df["data_hora"].dt.dayofweek.astype(int)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket="processed", Key="lastfm/backfill.parquet", Body=buffer.getvalue())
    print(f"Prata pronta: {len(df)} linhas limpas em processed/lastfm/backfill.parquet.")
    return df


# ---------------------------------------------------------------------------
# 3. OURO — carga em LOTE no warehouse (dims + fato), idempotente
# ---------------------------------------------------------------------------
def carregar(df):
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
    )
    cur = conn.cursor()

    # -- dim_artista: insere os artistas distintos e lê os ids de volta --
    artistas = list(
        df[["artista", "artista_mbid"]].drop_duplicates("artista").itertuples(index=False, name=None)
    )
    execute_values(
        cur,
        "INSERT INTO dim_artista (nome, mbid) VALUES %s ON CONFLICT (nome) DO NOTHING",
        artistas,
    )
    cur.execute("SELECT nome, id FROM dim_artista")
    artista_id = {nome: id_ for nome, id_ in cur.fetchall()}

    # -- dim_faixa: precisa do artista_id; insere distintas e lê os ids --
    faixas_df = df[["faixa", "album", "artista"]].drop_duplicates(subset=["faixa", "artista"]).copy()
    faixas_df["artista_id"] = faixas_df["artista"].map(artista_id)
    faixas = list(faixas_df[["faixa", "album", "artista_id"]].itertuples(index=False, name=None))
    execute_values(
        cur,
        "INSERT INTO dim_faixa (nome, album, artista_id) VALUES %s ON CONFLICT (nome, artista_id) DO NOTHING",
        faixas,
    )
    cur.execute("SELECT nome, artista_id, id FROM dim_faixa")
    faixa_id = {(nome, a_id): id_ for nome, a_id, id_ in cur.fetchall()}

    # -- dim_tempo: instantes distintos (data, hora) --
    tempos_df = df[["data", "hora", "ano", "mes", "dia", "dia_semana"]].drop_duplicates(subset=["data", "hora"])
    tempos = list(tempos_df.itertuples(index=False, name=None))
    execute_values(
        cur,
        "INSERT INTO dim_tempo (data, hora, ano, mes, dia, dia_semana) VALUES %s ON CONFLICT (data, hora) DO NOTHING",
        tempos,
    )
    cur.execute("SELECT data, hora, id FROM dim_tempo")
    tempo_id = {(data, hora): id_ for data, hora, id_ in cur.fetchall()}

    # -- fato_audicoes: monta (scrobble_uts, faixa_id, tempo_id) e insere em lote --
    df = df.copy()
    df["faixa_id"] = [faixa_id.get((f, artista_id.get(a))) for f, a in zip(df["faixa"], df["artista"])]
    df["tempo_id"] = [tempo_id.get((d, h)) for d, h in zip(df["data"], df["hora"])]
    fatos = [
        (int(s), int(f), int(t))
        for s, f, t in df[["scrobble_uts", "faixa_id", "tempo_id"]].itertuples(index=False, name=None)
        if pd.notna(f) and pd.notna(t)
    ]
    execute_values(
        cur,
        "INSERT INTO fato_audicoes (scrobble_uts, faixa_id, tempo_id) VALUES %s "
        "ON CONFLICT (scrobble_uts, faixa_id) DO NOTHING",
        fatos,
    )

    conn.commit()

    cur.execute("SELECT count(*) FROM fato_audicoes")
    total_fato = cur.fetchone()[0]
    print(
        f"Ouro pronto: {len(artistas)} artistas, {len(faixas)} faixas, {len(tempos)} instantes; "
        f"fato_audicoes agora com {total_fato} linhas."
    )
    cur.close()
    conn.close()


if __name__ == "__main__":
    inicio = time.time()
    faixas = extrair_tudo()
    df = transformar(faixas)
    carregar(df)
    print(f"\nBackfill concluído em {time.time() - inicio:.0f}s.")