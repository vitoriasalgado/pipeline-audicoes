-- 003 — remove as constraints UNIQUE redundantes (ago/2026).
--
--     docker exec -i pipeline-audicoes-warehouse-1 \
--       psql -U warehouse -d warehouse < db/migracoes/003_remove_constraints_redundantes.sql
--
-- Desde a migração 001 existem índices únicos por lower(nome). As constraints
-- antigas por nome exato viraram redundantes: toda duplicata exata também é
-- duplicata insensível a caixa, então a nova já cobre a antiga.
--
-- O motivo de removê-las não é economia. Enquanto elas existem, um
-- `ON CONFLICT (nome)` continua sendo SQL válido — e mira o índice errado, o
-- que faz o Postgres levantar violação quando a colisão acontece no índice por
-- lower(nome). Foi assim que o backfill quebrou numa instalação nova. Sem elas,
-- o mesmo comando falha na hora com "no unique or exclusion constraint
-- matching the ON CONFLICT specification": o alvo errado deixa de ser
-- escrevível.

BEGIN;

ALTER TABLE dim_artista DROP CONSTRAINT IF EXISTS dim_artista_nome_key;
ALTER TABLE dim_faixa   DROP CONSTRAINT IF EXISTS dim_faixa_nome_artista_id_key;

COMMIT;

SELECT conname AS constraints_restantes
FROM pg_constraint
WHERE conrelid IN ('dim_artista'::regclass, 'dim_faixa'::regclass)
  AND contype = 'u';
