# Pipeline de Audições — Last.fm (+ Spotify) → MinIO → Airflow → PostgreSQL

Projeto de portfólio de engenharia de dados. A ideia: entender **como meu gosto
musical mudou ao longo dos anos**, transformando essa dúvida numa pipeline de dados
de ponta a ponta.

A pipeline coleta o meu histórico de músicas (**API do Last.fm**), guarda em um data
lake (**MinIO**), trata os dados e carrega em um data warehouse (**PostgreSQL**),
tudo agendado e monitorado pelo **Apache Airflow**. Segue a arquitetura medalhão
(bronze → prata → ouro). O **Spotify** entra como segunda fonte (fase 2b), enriquecendo
as dimensões via OAuth.

> **Status:** 🚧 em construção — **duas** pipelines rodando no Airflow: Last.fm (`pipeline_audicoes`, `@daily`) e Spotify (`pipeline_spotify`, `@weekly`), ambas bronze → prata → ouro. O histórico completo do Last.fm (~6 anos) está no warehouse e o Spotify já enriquece as dimensões — um **esquema constelação** (duas fatos, dimensões compartilhadas). Falta só a query que cruza as duas fontes.
> O código de cada etapa é escrito, missão a missão, seguindo um roteiro de estudo.

## Arquitetura

```
Last.fm API ─┐
(scrobbles)  │     MinIO (data lake)                  PostgreSQL (warehouse)
             ├──►  raw/        (JSON cru, bronze)
             └──►  processed/  (Parquet, prata)  ───►  analytics (ouro)
Spotify API ─────►  (mesmo caminho bronze→prata→ouro)  fato_audicoes + fato_top_spotify
(top + biblioteca, via OAuth)                          + dimensões (esquema constelação)

        (tudo agendado, executado e monitorado pelo Airflow:
         pipeline_audicoes @daily · pipeline_spotify @weekly)
```

Diagrama completo em [`docs/arquitetura.jpg`](docs/arquitetura.jpg).

## O que já roda

Uma DAG do Airflow (`pipeline_audicoes`) executa a pipeline inteira de ponta a ponta, em sequência — `extrair → transformar → carregar`, as três tasks verdes numa mesma execução:

- ✅ **Ingestão (bronze)** — puxa meu histórico do Last.fm e grava o JSON cru no data lake (MinIO), no horário agendado. A carga é **incremental**: uma task pergunta ao warehouse qual o scrobble mais recente já carregado (a *marca d'água*) e a extração busca só o que veio depois, paginando quando a janela é grande. Cada execução escreve numa pasta própria — o bronze nunca é sobrescrito.
- ✅ **Transformação (prata)** — lê o JSON cru, limpa com pandas (descarta o `nowplaying`, tipa e deduplica) e salva em Parquet.
- ✅ **Carga (ouro)** — modela um esquema estrela (`fato_audicoes` + dimensões) e carrega no data warehouse PostgreSQL, de forma idempotente.

Além da ingestão do dia a dia (a DAG), a **carga histórica completa** (`backfill.py`) já povoou o warehouse com ~6 anos de audições — a base para as análises.

- ✅ **Análise (ouro)** — a pergunta que originou o projeto já é respondida por SQL sobre o esquema estrela: *"qual foi meu artista mais ouvido em cada mês"* (cruzando `fato_audicoes` com as dimensões, uma linha por mês ao longo dos anos).

**Segunda fonte — Spotify (via OAuth).** Uma segunda DAG (`pipeline_spotify`, semanal) traz meus *tops* e minha biblioteca do Spotify pelo mesmo caminho bronze → prata → ouro. No warehouse, isso vira um **esquema constelação**: uma nova fato (`fato_top_spotify`) que compartilha as mesmas dimensões da `fato_audicoes`, além de enriquecer artistas e faixas com os ids do Spotify — casando as duas fontes **por nome**. O token OAuth roda sozinho dentro do container (sem navegador), reutilizando o *refresh token* em cache.

## O que vem depois

- ⏳ **A query cruzada Last.fm × Spotify** — comparar o *top computado* pelo Spotify (numa janela de tempo, só do que ouvi lá) com o *mais tocado* cru do Last.fm (evento a evento, ~6 anos, todas as fontes). Duas definições de "o que eu mais ouço", lado a lado.

## Documentação

- [`docs/PRD_Pipeline_Audicoes.md`](docs/PRD_Pipeline_Audicoes.md) — o PRD completo (escopo, fontes, modelo de dados, DAGs, riscos). A partir da v0.3 ele descreve a pipeline *as-built*, e a **§9.1 lista as dívidas técnicas conhecidas** — o que o projeto ainda não faz, e o encaminhamento de cada uma. Dívida resolvida sai da lista.

## Estrutura do projeto

```
dags/        as duas DAGs do Airflow — é isto que roda em produção
lastfm/      fonte 1 — cada etapa (extração, transformação, carga) como foi construída
spotify/     fonte 2 — idem, para o Spotify (OAuth)
db/          schema.sql — o esquema do warehouse (dimensões + fatos)
scripts/     backfill.py — carga histórica pontual
arquivo/     scripts das primeiras missões, aposentados (mantidos como registro)
docs/        PRD e documentação
```

> **O que roda no Airflow são as DAGs em `dags/`.** As pastas por fonte (`lastfm/`, `spotify/`)
> guardam cada etapa como ela foi escrita, missão a missão — as DAGs **replicam essa lógica
> inline**, não importam esses módulos. São duas cópias, e a que executa é a de `dags/`.
> Manter assim foi consequência do projeto ser construído passo a passo; extrair um módulo
> compartilhado é um refactor em aberto.

> Os scripts de host são rodados a partir da **raiz** do projeto (ex.: `python spotify/extrair_spotify.py`).
> Atenção: `lastfm/extrair_para_minio.py` e `lastfm/transformar.py` **não** rodam no host — eles
> apontam para `minio:9000`, nome de serviço que só resolve dentro de um container.

## Pré-requisitos

- Docker + Docker Compose
- Python 3.10+
- Conta no Last.fm + API key (grátis): https://www.last.fm/api/account/create
- *(opcional, fase 2b)* App no [Spotify for Developers](https://developer.spotify.com/dashboard) — client id/secret + `redirect_uri` para o OAuth

## Como começar

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # e preencha LASTFM_API_KEY e LASTFM_USER

docker compose up airflow-init     # 1ª vez: inicializa o banco de metadados do Airflow
docker compose up -d               # sobe tudo: Airflow + MinIO + Postgres + warehouse + Redis
```

Na primeira subida o compose também prepara o que a pipeline espera encontrar, sem passo manual:

- **os buckets `raw` e `processed`** no MinIO (serviço `minio-init`, que roda uma vez e encerra);
- **as tabelas do warehouse**, aplicando [`db/schema.sql`](db/schema.sql) — o PostgreSQL executa esse arquivo apenas quando o volume de dados está vazio, ou seja, só na criação. Num warehouse já povoado ele é ignorado, e mudança de schema com dado dentro pede `ALTER TABLE`.

Serviços no ar:

- **Airflow** — http://localhost:8080 (`airflow` / `airflow`)
- **MinIO** (console do data lake) — http://localhost:9001 (`minioadmin` / `minioadmin`)
- **PostgreSQL** (warehouse, camada ouro) — `localhost:5433` (`warehouse` / `warehouse`, banco `warehouse`)

Para rodar a pipeline: no Airflow, ative a DAG **`pipeline_audicoes`** e clique em *Trigger* ▶️. Ela executa `descobrir_marca_dagua → extrair → transformar → carregar` — a ingestão é **incremental**: cada execução pergunta ao warehouse até onde já carregou e busca só o que falta, paginando se for muito. Se não houver nada novo, as tasks são puladas em vez de falhar.

Há também uma segunda DAG, **`pipeline_spotify`** (`@weekly`), que faz o mesmo caminho para o Spotify — enriquecendo as dimensões e populando a `fato_top_spotify`. Ela requer as credenciais OAuth do Spotify no `.env` e a primeira autenticação feita localmente (o token fica em cache e é reaproveitado pelo container).

Para carregar o **histórico completo** de uma vez (não só as faixas recentes), rode o script de carga histórica:

```bash
python scripts/backfill.py
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
- [x] Missão 15 — Transformar → `processed`: JSON do Spotify → Parquet limpo (prata)
- [x] Missão 16 — Enriquecer as dimensões + `fato_top_spotify`: esquema constelação (ouro)
- [x] Missão 17 — A DAG `pipeline_spotify` (`@weekly`)
- [ ] Missão 18 — A query cruzada Last.fm × Spotify (top computado vs mais tocado)
