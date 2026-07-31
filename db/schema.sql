-- Esquema do warehouse (ouro): constelação com duas fatos e dimensões compartilhadas.
-- Seguro de rodar de novo (IF NOT EXISTS).
--
-- ⚠️  Descomentar os DROP apaga o histórico inteiro; só volta com scripts/backfill.py.
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

-- O nome é a chave de negócio entre Last.fm e Spotify, e as duas fontes usam
-- caixas diferentes ('Zayn'/'ZAYN'). É por estes índices que os upserts das
-- DAGs fazem ON CONFLICT.
CREATE UNIQUE INDEX IF NOT EXISTS dim_artista_nome_lower_uq
    ON dim_artista (lower(nome));
CREATE UNIQUE INDEX IF NOT EXISTS dim_faixa_nome_lower_artista_uq
    ON dim_faixa (lower(nome), artista_id);

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