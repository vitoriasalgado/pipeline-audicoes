-- 002 — na_biblioteca com dois estados, não três (jul/2026).
--
-- A coluna só era marcada TRUE para o que vinha de /me/tracks; o resto ficava
-- NULL. Em SQL, `WHERE na_biblioteca = FALSE` não devolve NULL, então a
-- pergunta "o que eu ouço mas nunca salvei" devolvia zero linhas.
--
-- Rodar no host, a partir da raiz do projeto:
--
--     docker exec -i pipeline-audicoes-warehouse-1 \
--       psql -U warehouse -d warehouse < db/migracoes/002_na_biblioteca_booleana.sql
--
-- Seguro de rodar de novo.

BEGIN;

UPDATE dim_faixa SET na_biblioteca = FALSE WHERE na_biblioteca IS NULL;

ALTER TABLE dim_faixa ALTER COLUMN na_biblioteca SET DEFAULT FALSE;
ALTER TABLE dim_faixa ALTER COLUMN na_biblioteca SET NOT NULL;

COMMIT;

SELECT count(*) FILTER (WHERE na_biblioteca)         AS na_biblioteca,
       count(*) FILTER (WHERE NOT na_biblioteca)     AS fora_da_biblioteca,
       count(*) FILTER (WHERE na_biblioteca IS NULL) AS ainda_nulo
FROM dim_faixa;
