# Pipeline de Audições — Last.fm (+ Spotify) → MinIO → Airflow → PostgreSQL

Projeto de portfólio de engenharia de dados. A ideia: entender **como meu gosto
musical mudou ao longo dos anos**, transformando essa dúvida numa pipeline de dados
de ponta a ponta.

A pipeline coleta o meu histórico de músicas (**API do Last.fm**), guarda em um data
lake (**MinIO**), trata os dados e carrega em um data warehouse (**PostgreSQL**),
tudo agendado e monitorado pelo **Apache Airflow**. Segue a arquitetura medalhão
(bronze → prata → ouro). O **Spotify** entra como enriquecimento opcional numa fase 2.

> **Status:** 🚧 em construção — ingestão orquestrada pelo Airflow (Last.fm → MinIO) rodando; transformação e carga na Fase 2.
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

docker compose up -d               # sobe o MinIO (data lake)
# console em http://localhost:9001  (minioadmin / minioadmin)
```

## Roteiro (casado com as fases do guia)

- [x] Missão 0 — Esqueleto do projeto, Git e venv
- [x] Missão 1 — Conversar com a API do Last.fm
- [x] Missão 2 — Guardar o dado cru (bronze, local)
- [x] Missão 3 — Subir o data lake (Docker + MinIO)
- [x] Missão 4 — Colocar o dado dentro do lake (ingestão)
- [x] Missão 5 — Preparar a extração para virar uma tarefa
- [x] Missão 6 — Subir o Airflow
- [x] Missão 7 — A DAG: ingestão orquestrada
- [ ] Missão 8 — Deixar apresentável e publicar
- [ ] Fase 2 — Transformação (Parquet), carga (esquema estrela) e Spotify (opcional)
