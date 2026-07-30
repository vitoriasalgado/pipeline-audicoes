-- Esquema do warehouse (camada ouro): constelação com duas fatos e dimensões compartilhadas.
--
-- Este arquivo é SEGURO de rodar mais de uma vez: os CREATE são IF NOT EXISTS e não
-- tocam em dado existente.
--
-- ⚠️  Os DROP abaixo estão comentados de propósito. Rodá-los APAGA o warehouse inteiro,
--     incluindo os ~61 mil scrobbles do histórico (ago/2020 →), que só voltam rodando
--     `python scripts/backfill.py` de novo — dezenas de minutos e ~305 chamadas à API.
--     Descomente apenas se a intenção for realmente recriar tudo do zero.
--
-- DROP TABLE IF EXISTS fato_top_spotify;
-- DROP TABLE IF EXISTS fato_audicoes;
-- DROP TABLE IF EXISTS dim_tempo;
-- DROP TABLE IF EXISTS dim_faixa;
-- DROP TABLE IF EXISTS dim_artista;

CREATE TABLE IF NOT EXISTS dim_artista (
    id SERIAL PRIMARY KEY,
    nome TEXT UNIQUE,
    mbid TEXT,
    spotify_artist_id TEXT
);

CREATE TABLE IF NOT EXISTS dim_faixa (
    id SERIAL PRIMARY KEY,
    nome TEXT,
    album TEXT,
    artista_id INT REFERENCES dim_artista(id),
    spotify_track_id TEXT,
    na_biblioteca BOOLEAN,
    biblioteca_added_at TIMESTAMP,
    UNIQUE(nome, artista_id)
);

CREATE TABLE IF NOT EXISTS dim_tempo (
    id SERIAL PRIMARY KEY,
    data DATE,
    hora INT,
    ano INT,
    mes INT,
    dia INT,
    dia_semana INT,
    UNIQUE(data, hora)
);

CREATE TABLE IF NOT EXISTS fato_audicoes (
    id SERIAL PRIMARY KEY,
    scrobble_uts BIGINT,
    faixa_id INT REFERENCES dim_faixa(id),
    tempo_id INT REFERENCES dim_tempo(id),
    UNIQUE(scrobble_uts, faixa_id)
);

CREATE TABLE IF NOT EXISTS fato_top_spotify (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE,
    time_range TEXT,
    tipo TEXT,
    posicao INT,
    faixa_id INT REFERENCES dim_faixa(id),
    artista_id INT REFERENCES dim_artista(id),
    UNIQUE(snapshot_date, time_range, tipo, posicao)
);