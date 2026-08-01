-- Top computado (Spotify) × mais tocado (Last.fm), na mesma janela (Missão 18).
--
--     docker exec -i pipeline-audicoes-warehouse-1 \
--       psql -q -U warehouse -d warehouse < db/consultas/cruzamento_lastfm_spotify.sql
--
-- É a consulta que só a constelação permite: duas fatos medindo a mesma coisa
-- de jeitos diferentes, sobre a mesma camada de dimensões.
--
-- As janelas são casadas de propósito — short_term (~4 semanas) do Spotify
-- contra 28 dias de scrobbles. Sem isso, qualquer divergência poderia ser só a
-- diferença de período. Trocar as duas juntas dá as outras versões da pergunta:
-- medium_term ↔ '6 months', long_term ↔ '1 year'.
--
-- As duas fatos guardam naturezas diferentes: fato_audicoes é evento (conta-se),
-- fato_top_spotify é ranking já calculado (lê-se a posição).

\pset pager off
\pset border 2

WITH lastfm AS (
    SELECT da.id    AS artista_id,
           da.nome  AS artista,
           count(*) AS scrobbles,
           ROW_NUMBER() OVER (ORDER BY count(*) DESC) AS posicao_real
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
SELECT s.artista,
       s.posicao                  AS "top Spotify",
       l.posicao_real             AS "por execucoes",
       l.scrobbles                AS "execucoes",
       l.posicao_real - s.posicao AS "diferenca"
FROM spotify s
LEFT JOIN lastfm l ON l.artista_id = s.artista_id
WHERE s.posicao <= 20
ORDER BY s.posicao;
