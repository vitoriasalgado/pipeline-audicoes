-- Onde as duas fontes discordam, nos dois sentidos (Missão 18).
--
--     docker exec -i pipeline-audicoes-warehouse-1 \
--       psql -q -U warehouse -d warehouse < db/consultas/cruzamento_completo.sql
--
-- Companheira de cruzamento_lastfm_spotify.sql. As duas fazem a mesma junção
-- sobre os mesmos CTEs, mas respondem perguntas diferentes:
--
--   cruzamento_lastfm_spotify.sql  "o top do Spotify se sustenta nas minhas
--                                   execuções?" — LEFT JOIN a partir do Spotify,
--                                   recortado no top 20, legível numa tela
--   este arquivo                   "onde as duas discordam, nos dois sentidos?"
--                                   — FULL OUTER, sem recorte, ~244 linhas
--
-- Por que FULL OUTER: um INNER mostraria só onde as fontes concordam, e um LEFT
-- a partir do Spotify esconde exatamente o mesmo — os artistas que eu toquei e
-- que não entraram no top. Esse bloco é metade da resposta.
--
-- Sem LIMIT de propósito. Recortar o lado do Spotify no top 20 faria o bloco
-- "só Last.fm" misturar quem ficou fora do top 20 com quem ficou fora do top 50,
-- e é a fronteira entre as regiões que esta consulta existe para mostrar.
--
-- COALESCE e NULLS LAST não são enfeite: num FULL OUTER, a linha que existe só
-- de um lado traz o outro lado inteiro como NULL. Sem COALESCE, os artistas que
-- só aparecem no Last.fm sairiam sem nome; sem NULLS LAST, viriam no topo.

\pset pager off
\pset border 2

WITH lastfm AS (
    SELECT da.id    AS artista_id,
           da.nome  AS artista,
           count(*) AS scrobbles
    FROM fato_audicoes f
    JOIN dim_faixa   df ON f.faixa_id    = df.id
    JOIN dim_artista da ON df.artista_id = da.id
    WHERE to_timestamp(f.scrobble_uts) >= now() - interval '28 days'
    GROUP BY da.id, da.nome
),
spotify AS (
    SELECT da.id   AS artista_id,
           da.nome AS artista,
           t.posicao
    FROM fato_top_spotify t
    JOIN dim_artista da ON t.artista_id = da.id
    WHERE t.tipo       = 'artist'
      AND t.time_range = 'short_term'
      AND t.snapshot_date = (SELECT max(snapshot_date) FROM fato_top_spotify)
)
SELECT CASE WHEN s.posicao   IS NULL THEN 'so Last.fm'
            WHEN l.scrobbles IS NULL THEN 'so Spotify'
            ELSE 'nos dois' END        AS regiao,
       COALESCE(s.artista, l.artista)  AS artista,
       s.posicao                       AS top_spotify,
       l.scrobbles                     AS tocadas_lastfm
FROM spotify s
FULL OUTER JOIN lastfm l ON l.artista_id = s.artista_id
ORDER BY s.posicao NULLS LAST, l.scrobbles DESC NULLS LAST;
