"""Transformação da prata do Last.fm: JSON cru entra, DataFrame limpo sai.

Fica fora da DAG e não importa Airflow, então dá pra importar e testar no host
(ver `tests/`). A DAG segue cuidando do I/O; aqui só mora a lógica.
"""

import pandas as pd

COLUNAS = ["name", "artist.#text", "album.#text", "date.uts", "mbid", "artist.mbid"]

TEXTOS = ["faixa", "artista", "album", "faixa_mbid", "artista_mbid"]

NOMES = {
    "name": "faixa",
    "artist.#text": "artista",
    "album.#text": "album",
    "date.uts": "scrobbles_uts",
    "mbid": "faixa_mbid",
    "artist.mbid": "artista_mbid",
}


def extrair_faixas(pagina):
    """Lista de faixas de uma página do getRecentTracks."""
    faixas = pagina["recenttracks"]["track"]
    if isinstance(faixas, dict):   # um único resultado vem como objeto, não lista
        return [faixas]
    return faixas


def limpar(faixas):
    """Achata, tipa e deduplica as faixas cruas; devolve o DataFrame da prata."""
    df = pd.json_normalize(faixas)
    df = df.reindex(columns=COLUNAS)
    df = df.rename(columns=NOMES)
    df[TEXTOS] = df[TEXTOS].astype(object).fillna("")

    df["scrobble_uts"] = pd.to_numeric(df["scrobbles_uts"], errors="coerce")
    df["data_hora"] = pd.to_datetime(df["scrobble_uts"], unit="s", errors="coerce")

    df = df.dropna(subset=["scrobble_uts"])   # descarta o nowplaying (vem sem date)
    df = df.drop_duplicates(subset=["scrobble_uts", "faixa"])
    df = df.drop(columns=["scrobbles_uts"])

    return df
