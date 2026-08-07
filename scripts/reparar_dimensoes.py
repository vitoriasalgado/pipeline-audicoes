"""
reparar_dimensoes.py — recupera do bronze o que a carga apagou (one-off).

Até agosto/2026 a carga da DAG do Last.fm fazia `SET mbid = EXCLUDED.mbid` e
`SET album = EXCLUDED.album`. O Last.fm devolve esses campos vazios em parte dos
scrobbles, então bastava um deles para apagar o valor bom que outro trouxe. Como
o `processed` é sobrescrito, o bronze é a única cópia que ainda tem esses
valores — é lá que este script vai buscá-los.

Só preenche o que está vazio; não mexe em quem já tem valor. Entre dois valores
bons no bronze, vence o do scrobble mais antigo (menor `date.uts`) — desempatar
pela ordem de leitura do S3 daria resultado diferente a cada execução.

Rodar DEPOIS de corrigir os ON CONFLICT nas DAGs: reparando antes, a execução
diária seguinte apaga de novo. No host, a partir da raiz do projeto:

    python scripts/reparar_dimensoes.py             # só relatório, não escreve
    python scripts/reparar_dimensoes.py --aplicar   # escreve no warehouse
"""

import os
import sys
import json

import boto3
import psycopg2
from psycopg2.extras import execute_values

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
WAREHOUSE_HOST = os.environ.get("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = int(os.environ.get("WAREHOUSE_PORT", "5433"))
WAREHOUSE_DB   = os.environ.get("WAREHOUSE_DB", "warehouse")

PREFIXO_BRONZE = "lastfm/"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


# ---------------------------------------------------------------------------
# 1. Varrer o bronze
# ---------------------------------------------------------------------------
def listar_paginas():
    """Todas as chaves do bronze do Last.fm: backfill/ + incremental/<ts>/.

    Usa paginator porque o list_objects_v2 devolve no máximo 1000 chaves por
    chamada — o backfill sozinho já tem ~305, e as pastas incrementais crescem
    uma por dia.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket="raw", Prefix=PREFIXO_BRONZE):
        for obj in pagina.get("Contents", []):
            if obj["Key"].endswith(".json"):
                yield obj["Key"]


def melhor(candidatos, chave, uts, valor):
    """Guarda `valor` para `chave` se for o mais antigo visto até agora.

    `candidatos` é {chave: (uts, valor)}. Só entram valores não-vazios; entre
    dois não-vazios, vence o de menor uts.
    """
    if not valor:
        return
    anterior = candidatos.get(chave)
    if anterior is None or uts < anterior[0]:
        candidatos[chave] = (uts, valor)


def varrer_bronze():
    """Lê todo o bronze e devolve o melhor mbid por artista e o melhor álbum por faixa."""
    mbid_por_artista = {}          # lower(artista)                  -> (uts, mbid)
    album_por_faixa = {}           # (lower(artista), lower(faixa))  -> (uts, album)

    objetos = 0
    scrobbles = 0

    for chave in listar_paginas():
        conteudo = s3.get_object(Bucket="raw", Key=chave)["Body"].read()
        dados = json.loads(conteudo.decode("utf-8"))

        faixas = dados.get("recenttracks", {}).get("track", [])
        if isinstance(faixas, dict):        # um único resultado vem como objeto
            faixas = [faixas]

        for faixa in faixas:
            data = faixa.get("date")
            if not data:                    # nowplaying: sem date, sem lugar no tempo
                continue
            uts = int(data["uts"])
            scrobbles += 1

            artista = (faixa.get("artist") or {}).get("#text", "")
            if not artista:
                continue
            artista_k = artista.lower()

            melhor(mbid_por_artista, artista_k, uts,
                   (faixa.get("artist") or {}).get("mbid", ""))

            nome_faixa = faixa.get("name", "")
            if nome_faixa:
                melhor(album_por_faixa, (artista_k, nome_faixa.lower()), uts,
                       (faixa.get("album") or {}).get("#text", ""))

        objetos += 1
        if objetos % 50 == 0:
            print(f"  ... {objetos} objetos lidos, {scrobbles} scrobbles", flush=True)

    print(f"bronze varrido: {objetos} objetos, {scrobbles} scrobbles", flush=True)
    print(f"  mbids   não-vazios encontrados: {len(mbid_por_artista)} artistas", flush=True)
    print(f"  álbuns  não-vazios encontrados: {len(album_por_faixa)} faixas", flush=True)
    return mbid_por_artista, album_por_faixa


# ---------------------------------------------------------------------------
# 2. Cruzar com o warehouse
# ---------------------------------------------------------------------------
def buracos_no_warehouse(cur):
    """Artistas sem mbid e faixas sem álbum, com a chave de casamento do bronze."""
    cur.execute("""
        SELECT id, lower(nome)
          FROM dim_artista
         WHERE coalesce(mbid, '') = ''
    """)
    artistas = cur.fetchall()

    cur.execute("""
        SELECT f.id, lower(a.nome), lower(f.nome)
          FROM dim_faixa f
          JOIN dim_artista a ON a.id = f.artista_id
         WHERE coalesce(f.album, '') = ''
    """)
    faixas = cur.fetchall()

    return artistas, faixas


def main():
    aplicar = "--aplicar" in sys.argv

    mbid_por_artista, album_por_faixa = varrer_bronze()

    conn = psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user="warehouse",
        password="warehouse",
    )
    cur = conn.cursor()

    artistas, faixas = buracos_no_warehouse(cur)
    print(f"\nwarehouse: {len(artistas)} artistas sem mbid, {len(faixas)} faixas sem álbum",
          flush=True)

    reparos_artista = [
        (id_, mbid_por_artista[nome][1])
        for id_, nome in artistas
        if nome in mbid_por_artista
    ]
    reparos_faixa = [
        (id_, album_por_faixa[(artista, faixa)][1])
        for id_, artista, faixa in faixas
        if (artista, faixa) in album_por_faixa
    ]

    print(f"recuperáveis do bronze:", flush=True)
    print(f"  {len(reparos_artista)} de {len(artistas)} artistas (mbid)", flush=True)
    print(f"  {len(reparos_faixa)} de {len(faixas)} faixas (álbum)", flush=True)

    for id_, valor in reparos_artista[:5]:
        print(f"    exemplo artista id={id_} -> {valor}", flush=True)
    for id_, valor in reparos_faixa[:5]:
        print(f"    exemplo faixa   id={id_} -> {valor}", flush=True)

    if not aplicar:
        print("\n(nada foi escrito — rode com --aplicar para gravar)", flush=True)
        cur.close()
        conn.close()
        return

    if reparos_artista:
        execute_values(
            cur,
            """
            UPDATE dim_artista SET mbid = novo.mbid
              FROM (VALUES %s) AS novo(id, mbid)
             WHERE dim_artista.id = novo.id
            """,
            reparos_artista,
        )
    if reparos_faixa:
        execute_values(
            cur,
            """
            UPDATE dim_faixa SET album = novo.album
              FROM (VALUES %s) AS novo(id, album)
             WHERE dim_faixa.id = novo.id
            """,
            reparos_faixa,
        )

    conn.commit()      # um commit só: ou o reparo inteiro entra, ou nada entra
    print(f"\nreparo aplicado: {len(reparos_artista)} artistas, {len(reparos_faixa)} faixas",
          flush=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
