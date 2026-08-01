-- Artista mais ouvido em cada mês (Missão 12).
-- A pergunta que originou o projeto: como o meu gosto mudou ao longo do tempo.
--
--     docker exec -i pipeline-audicoes-warehouse-1 \
--       psql -q -U warehouse -d warehouse < db/consultas/artista_por_mes.sql
--
-- O artista não está na fato: o caminho passa pela faixa
-- (fato_audicoes → dim_faixa → dim_artista), mais a dim_tempo para o mês.
--
-- GROUP BY dá todos os artistas de cada mês; para ficar só com o campeão é
-- preciso numerar dentro do mês (ROW_NUMBER + PARTITION BY) e filtrar em cima
-- do resultado — daí o CTE, já que o WHERE é avaliado antes da numeração.

WITH contagem AS (
    SELECT t.ano,
           t.mes,
           a.nome   AS artista,
           count(*) AS scrobbles
    FROM fato_audicoes f
    JOIN dim_faixa   df ON f.faixa_id    = df.id
    JOIN dim_artista a  ON df.artista_id = a.id
    JOIN dim_tempo   t  ON f.tempo_id    = t.id
    GROUP BY t.ano, t.mes, a.nome
),
ranking AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ano, mes ORDER BY scrobbles DESC) AS posicao
    FROM contagem
)
SELECT ano, mes, artista, scrobbles
FROM ranking
WHERE posicao = 1
ORDER BY ano, mes;
