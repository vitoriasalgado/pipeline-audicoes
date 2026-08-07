"""Testes da transformação da prata do Last.fm.

Não sobem Airflow, MinIO nem Postgres: a lógica é JSON entra, DataFrame sai.
Rodar da raiz do projeto, com a venv ativa:  pytest
"""

import pandas as pd

from transformacoes import extrair_faixas, limpar


def faixa(nome, uts, artista="Tame Impala", album="Lonerism"):
    """Uma faixa como o getRecentTracks a devolve."""
    return {
        "name": nome,
        "mbid": "",
        "artist": {"#text": artista, "mbid": "63aa7c3e"},
        "album": {"#text": album},
        "date": {"uts": str(uts), "#text": "20 Oct 2020, 10:03"},
    }


def nowplaying(nome, artista="Tame Impala"):
    """O "tocando agora" — vem SEM a chave `date`."""
    return {
        "name": nome,
        "mbid": "",
        "artist": {"#text": artista, "mbid": "63aa7c3e"},
        "album": {"#text": "Currents"},
        "@attr": {"nowplaying": "true"},
    }


def test_track_como_objeto_vira_lista():
    """Com um único resultado o Last.fm manda `track` como objeto, não lista."""
    pagina = {"recenttracks": {"track": faixa("Elephant", 1603188238)}}

    assert extrair_faixas(pagina) == [faixa("Elephant", 1603188238)]


def test_track_como_lista_passa_direto():
    pagina = {"recenttracks": {"track": [faixa("Elephant", 1), faixa("Yes I'm", 2)]}}

    assert len(extrair_faixas(pagina)) == 2


def test_nowplaying_e_descartado():
    """Sem `date` não há instante — e sem instante não há chave na fato."""
    df = limpar([faixa("Elephant", 1603188238), nowplaying("The Less I Know")])

    assert list(df["faixa"]) == ["Elephant"]


def test_scrobble_repetido_e_deduplicado():
    """Mesma faixa no mesmo segundo aparece duas vezes quando a paginação desliza."""
    df = limpar([faixa("Elephant", 1603188238), faixa("Elephant", 1603188238)])

    assert len(df) == 1


def test_mesma_faixa_em_instantes_diferentes_fica():
    """Ouvir a mesma música duas vezes são dois scrobbles, não uma duplicata."""
    df = limpar([faixa("Elephant", 1603188238), faixa("Elephant", 1603190000)])

    assert len(df) == 2


def test_pagina_inteira_sem_album():
    """O json_normalize só cria a coluna se alguma faixa do lote trouxer a chave."""
    sem_album = faixa("Elephant", 1603188238)
    del sem_album["album"]

    df = limpar([sem_album])

    assert len(df) == 1
    assert df["album"].iloc[0] == ""


def test_pagina_so_com_nowplaying():
    """Sem nenhuma faixa datada, nem a coluna `date.uts` existe — e sobra zero linha."""
    df = limpar([nowplaying("The Less I Know")])

    assert len(df) == 0


def test_data_hora_derivada_do_uts():
    df = limpar([faixa("Elephant", 1603188238)])

    assert df["data_hora"].iloc[0] == pd.Timestamp("2020-10-20 10:03:58")
