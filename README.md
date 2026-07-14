# Pipeline de Audições — Last.fm (+ Spotify) → MinIO → Airflow → PostgreSQL

Projeto de portfólio de engenharia de dados. A ideia: entender **como meu gosto
musical mudou ao longo dos anos**, transformando essa dúvida numa pipeline de dados
de ponta a ponta.

A pipeline coleta o meu histórico de músicas (**API do Last.fm**), guarda em um data
lake (**MinIO**), trata os dados e carrega em um data warehouse (**PostgreSQL**),
tudo agendado e monitorado pelo **Apache Airflow**. Segue a arquitetura medalhão
(bronze → prata → ouro). O **Spotify** entra como enriquecimento opcional numa fase 2.

> **Status:** 🚧 em construção — pipeline de ponta a ponta rodando no Airflow (Last.fm → MinIO → PostgreSQL, bronze → prata → ouro). Falta a primeira query analítica e o enriquecimento opcional com Spotify.
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

## O que vem depois (Fase 2)

- ⏳ **Primeira query analítica** — responder à pergunta original: "qual foi meu artista mais ouvido em cada mês".
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

Para rodar a pipeline: no Airflow, ative a DAG **`pipeline_audicoes`** e clique em *Trigger* ▶️. Ela executa `extrair → transformar → carregar`: puxa o histórico do Last.fm e grava o JSON no bucket `raw` do MinIO (bronze), limpa e converte para Parquet no bucket `processed` (prata), e carrega o esquema estrela no PostgreSQL (ouro).

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
- [ ] Missão 12 — A primeira query analítica ("artista mais ouvido por mês")
- [ ] Fase 2b (opcional) — Spotify: enriquecer as dimensões via OAuth
