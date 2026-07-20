# Pipeline de Audições — Last.fm (+ Spotify) → MinIO → Airflow → PostgreSQL

Projeto de portfólio de engenharia de dados. A ideia: entender **como meu gosto
musical mudou ao longo dos anos**, transformando essa dúvida numa pipeline de dados
de ponta a ponta.

A pipeline coleta o meu histórico de músicas (**API do Last.fm**), guarda em um data
lake (**MinIO**), trata os dados e carrega em um data warehouse (**PostgreSQL**),
tudo agendado e monitorado pelo **Apache Airflow**. Segue a arquitetura medalhão
(bronze → prata → ouro). O **Spotify** entra como enriquecimento opcional numa fase 2.

> **Status:** 🚧 em construção — pipeline de ponta a ponta rodando no Airflow (Last.fm → MinIO → PostgreSQL, bronze → prata → ouro), histórico completo carregado no warehouse (~6 anos de audições) e a primeira pergunta analítica já respondida por SQL. Falta só o enriquecimento opcional com Spotify.
> O código de cada etapa é escrito, missão a missão, seguindo um roteiro de estudo.

## Arquitetura

```
Last.fm API ─┐
(scrobbles)  │     MinIO (data lake)                  PostgreSQL (warehouse)
             ├──►  raw/        (JSON cru, bronze)
             └──►  processed/  (Parquet, prata)  ───►  analytics (ouro)
                                                       fato_audicoes + dimensões
Spotify API ·····(opcional: enriquece as dimensões)···┘

        (tudo agendado, executado e monitorado pelo Airflow)
```

Diagrama completo em [`docs/arquitetura.jpg`](docs/arquitetura.jpg).

## O que já roda

Uma DAG do Airflow (`pipeline_audicoes`) executa a pipeline inteira de ponta a ponta, em sequência — `extrair → transformar → carregar`, as três tasks verdes numa mesma execução:

- ✅ **Ingestão (bronze)** — puxa meu histórico do Last.fm e grava o JSON cru no data lake (MinIO), no horário agendado.
- ✅ **Transformação (prata)** — lê o JSON cru, limpa com pandas (descarta o `nowplaying`, tipa e deduplica) e salva em Parquet.
- ✅ **Carga (ouro)** — modela um esquema estrela (`fato_audicoes` + dimensões) e carrega no data warehouse PostgreSQL, de forma idempotente.

As tasks ficam verdes na interface, com retry automático se falharem.

Além da ingestão incremental do dia a dia (a DAG), a **carga histórica completa** (`backfill.py`) já povoou o warehouse com ~6 anos de audições — a base para as análises.

- ✅ **Análise (ouro)** — a pergunta que originou o projeto já é respondida por SQL sobre o esquema estrela: *"qual foi meu artista mais ouvido em cada mês"* (cruzando `fato_audicoes` com as dimensões, uma linha por mês ao longo dos anos).

## O que vem depois (Fase 2)

- ⏳ **Spotify** (opcional) — enriquecer as dimensões via OAuth.

## Documentação

- [`docs/PRD_Pipeline_Audicoes.md`](docs/PRD_Pipeline_Audicoes.md) — o PRD completo (escopo, fontes, modelo de dados, DAGs, riscos).

## Pré-requisitos

- Docker + Docker Compose
- Python 3.10+
- Conta no Last.fm + API key (grátis): https://www.last.fm/api/account/create

## Como começar

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # e preencha LASTFM_API_KEY e LASTFM_USER

docker compose up airflow-init     # 1ª vez: inicializa o banco de metadados do Airflow
docker compose up -d               # sobe tudo: Airflow + MinIO + Postgres + Redis
```

Serviços no ar:

- **Airflow** — http://localhost:8080 (`airflow` / `airflow`)
- **MinIO** (console do data lake) — http://localhost:9001 (`minioadmin` / `minioadmin`)
- **PostgreSQL** (warehouse, camada ouro) — `localhost:5433` (`warehouse` / `warehouse`, banco `warehouse`)

Para rodar a pipeline: no Airflow, ative a DAG **`pipeline_audicoes`** e clique em *Trigger* ▶️. Ela executa `extrair → transformar → carregar`: puxa o histórico do Last.fm e grava o JSON no bucket `raw` do MinIO (bronze), limpa e converte para Parquet no bucket `processed` (prata), e carrega o esquema estrela no PostgreSQL (ouro). Essa é a ingestão **incremental** — cada execução traz o que há de novo.

Para carregar o **histórico completo** de uma vez (não só as faixas recentes), rode o script de carga histórica:

```bash
python backfill.py
```

Ele pagina todo o histórico do Last.fm, grava cada página no bronze e faz a carga em lote no warehouse. É uma operação pontual — a DAG cuida do dia a dia daí em diante.

## Roteiro (casado com as fases do guia)

- [x] Missão 0 — Esqueleto do projeto, Git e venv
- [x] Missão 1 — Conversar com a API do Last.fm
- [x] Missão 2 — Guardar o dado cru (bronze, local)
- [x] Missão 3 — Subir o data lake (Docker + MinIO)
- [x] Missão 4 — Colocar o dado dentro do lake (ingestão)
- [x] Missão 5 — Preparar a extração para virar uma tarefa
- [x] Missão 6 — Subir o Airflow
- [x] Missão 7 — A DAG: ingestão orquestrada
- [x] Missão 8 — Deixar apresentável e publicar

**Fase 2 — do lake ao warehouse (prata e ouro):**

- [x] Missão 9 — Transformação: JSON cru → Parquet limpo (prata)
- [x] Missão 10 — Carga: esquema estrela no PostgreSQL (ouro)
- [x] Missão 11 — A DAG de ponta a ponta (extrair → transformar → carregar)
- [x] Missão 12 — A primeira query analítica ("artista mais ouvido por mês")
**Fase 2b (opcional) — Spotify: segunda fonte, enriquecendo as dimensões via OAuth:**

- [x] Missão 13 — O app do Spotify e o primeiro OAuth (autenticar → top tracks no terminal)
- [x] Missão 14 — Extrair → `raw`: top tracks/artists (3 time_ranges) + biblioteca salva (bronze)
- [ ] Missão 15 — Transformar → `processed`: JSON do Spotify → Parquet limpo (prata)
- [ ] Missão 16 — Enriquecer as dimensões + `fato_top_spotify`: esquema constelação (ouro)
- [ ] Missão 17 — A DAG `pipeline_spotify` (`@weekly`)
- [ ] Missão 18 — A query cruzada Last.fm × Spotify (top computado vs mais tocado)
