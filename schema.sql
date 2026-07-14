DROP TABLE IF EXISTS fato_audicoes;
DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_faixa;
DROP TABLE IF EXISTS dim_artista;

CREATE TABLE dim_artista (
    id SERIAL PRIMARY KEY,
    nome TEXT UNIQUE,
    mbid TEXT
);

CREATE TABLE dim_faixa (
    id SERIAL PRIMARY KEY,
    nome TEXT,
    album TEXT,
    artista_id INT REFERENCES dim_artista(id),
    UNIQUE(nome, artista_id)
);

CREATE TABLE dim_tempo (
    id SERIAL PRIMARY KEY,
    data DATE,
    hora INT,
    ano INT,
    mes INT,
    dia INT,
    dia_semana INT,
    UNIQUE(data, hora)
);

CREATE TABLE fato_audicoes (
    id SERIAL PRIMARY KEY,
    scrobble_uts BIGINT,
    faixa_id INT REFERENCES dim_faixa(id),
    tempo_id INT REFERENCES dim_tempo(id),
    UNIQUE(scrobble_uts, faixa_id)
);