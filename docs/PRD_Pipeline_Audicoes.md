# PRD — Pipeline de Audições (Last.fm + Spotify)

| | |
|---|---|
| **Projeto** | Pipeline de engenharia de dados de histórico musical |
| **Versão** | 0.2 (Spotify incluído no escopo) |
| **Data** | Junho de 2026 |
| **Stacks** | MinIO (S3) · Apache Airflow · PostgreSQL |
| **Status** | Planejamento (antes de iniciar a implementação) |

---

## 1. Visão geral e problema

Construir uma pipeline de dados de ponta a ponta que coleta o meu **histórico de músicas ouvidas** e o meu **gosto computado** em duas plataformas, guarda em um data lake, trata os dados e os disponibiliza em um data warehouse para análise — tudo agendado e monitorado.

O objetivo é duplo: ter um **projeto de portfólio** que demonstre competência em engenharia de dados (ingestão de múltiplas fontes, OAuth, armazenamento, transformação, orquestração, modelagem e qualidade) e responder perguntas analíticas como "artista mais ouvido por mês", "horário de pico de escuta", "como meu gosto mudou ao longo do tempo" e "o que o Spotify considera meu 'top' versus o que eu realmente mais toquei (Last.fm)".

Segue a arquitetura **medalhão** (bronze → prata → ouro) do documento de stacks, com **duas fontes via API** (não um Postgres simulando OLTP).

---

## 2. Objetivos, não-objetivos e pré-requisitos

### Objetivos (escopo)
- Ingerir scrobbles do **Last.fm** de forma **incremental** (diária) + carga histórica completa.
- Ingerir do **Spotify** (via OAuth) os **top tracks/artists** e a **biblioteca salva**, para uma segunda visão do gosto e para enriquecer as dimensões.
- Persistir dado cru imutável (`raw`) e tratado em Parquet (`processed`).
- Modelar em **esquema estrela / constelação** (duas tabelas de fato compartilhando dimensões) e carregar no Postgres analítico.
- Orquestrar com Airflow (duas DAGs em cadências diferentes; retries; observabilidade).
- Entregar consultas SQL analíticas, incluindo o cruzamento Last.fm × Spotify.

### Não-objetivos (fora do escopo atual)
- Tasks de **qualidade de dados** dedicadas (planejado para a Fase 5, depois do núcleo).
- Dashboard visual (BI) — primeiro o dado correto, depois a visualização.
- Deploy em nuvem — roda 100% local/containerizado.
- Recomendação musical / ML — explicitamente fora.
- Recursos do Spotify **descontinuados** (audio features, recommendations, popularidade) — ver 4.2.

### Pré-requisitos e premissas
- Conta no **Last.fm** + API key (gratuita, sem OAuth).
- **Spotify Premium** — desde 2026 é **obrigatório** para usar a Web API e para registrar o app. *(Premissa: o autor tem ou terá Premium. Sem isso, o escopo Spotify cai.)*
- Conta do Spotify possivelmente já conectada ao Last.fm (nesse caso os scrobbles já incluem as reproduções do Spotify).
- Docker + Docker Compose; Python 3.10+.

---

## 3. Arquitetura (resumo)

```
Last.fm API ──► raw/  ──► processed/ ──►┐
(scrobbles, diário)  (JSON)   (Parquet)  │   PostgreSQL (warehouse - ouro)
                                         ├──► fato_audicoes
Spotify API ──► raw/  ──► processed/ ──►┤    fato_top_spotify
(OAuth, semanal)     (JSON)   (Parquet)  │    dim_artista · dim_faixa · dim_tempo
                                         ┘

      MinIO = data lake (bronze + prata) · Airflow orquestra as duas esteiras
```

| Camada | Ferramenta | Papel |
|---|---|---|
| Ingestão | Python (`requests` / `spotipy`) | Coletar das duas APIs |
| Lake bronze/prata | MinIO (S3) | JSON cru e Parquet tratado |
| Warehouse ouro | PostgreSQL | Esquema estrela/constelação |
| Orquestração | Apache Airflow | Agendar, executar, monitorar, retries |
| Metadados do Airflow | PostgreSQL | Estado interno do Airflow |

---

## 4. Fontes de dados — mapeamento das APIs

> Seção central: endpoints, parâmetros, formatos de resposta e campos, mapeados antes de codar.

### 4.1 Last.fm (fonte primária — eventos/histórico)

- **Base URL:** `https://ws.audioscrobbler.com/2.0/` (endpoint único; operação via `method`).
- **Auth:** apenas **API key** para leitura (sem OAuth).
- **Custo:** gratuita para uso não comercial. **Rate limit:** sem número rígido na doc; ser conservador (poucas req/s). Job diário em lote não é gargalo.
- **Docs:** https://www.last.fm/api/show/user.getRecentTracks

#### Endpoint principal: `user.getRecentTracks`

**Parâmetros**

| Parâmetro | Obrigatório | Descrição |
|---|---|---|
| `method` | sim | `user.getRecentTracks` |
| `user` | sim | usuário do Last.fm |
| `api_key` | sim | a chave |
| `format` | recomendado | `json` |
| `limit` | não | padrão 50, **máximo 200** |
| `page` | não | paginação |
| `from` | não | **timestamp UNIX (UTC)** — início da janela (incremental) |
| `to` | não | **timestamp UNIX (UTC)** — fim da janela |
| `extended` | não | `0`/`1` — dados extras do artista |

**Resposta (campos relevantes)**

```json
{
  "recenttracks": {
    "@attr": { "user": "...", "page": "1", "totalPages": "94613", "total": "94618" },
    "track": [
      {
        "artist": { "mbid": "63aa...", "#text": "Tame Impala" },
        "album":  { "mbid": "0d2c...", "#text": "Lonerism" },
        "name":   "Feels Like We Only Go Backwards",
        "url":    "https://www.last.fm/music/...",
        "date":   { "uts": "1603188238", "#text": "20 Oct 2020, 10:03" }
      }
    ]
  }
}
```

| Campo | Observação |
|---|---|
| `recenttracks.@attr.totalPages` | paginar até o fim |
| `track[].artist.#text` | nome do artista |
| `track[].artist.mbid` | id MusicBrainz — **pode vir vazio** |
| `track[].album.#text` | nome do álbum (pode faltar) |
| `track[].name` | nome da faixa |
| `track[].date.uts` | **timestamp UNIX** do scrobble |
| `track[]."@attr".nowplaying` | "tocando agora" vem **sem `date`** → filtrar |

### 4.2 Spotify (segunda fonte — gosto computado + biblioteca)

- **Base URL:** `https://api.spotify.com/v1`
- **Auth:** **OAuth 2.0 (Authorization Code)**. Exige **Spotify Premium**. Credenciais (`client_id`/`client_secret`) e `redirect_uri` no dashboard (https://developer.spotify.com/dashboard).
  - Authorize: `https://accounts.spotify.com/authorize`
  - Token: `https://accounts.spotify.com/api/token` (devolve `access_token` válido ~1h + `refresh_token`)
  - **Scopes:** `user-top-read`, `user-library-read`, `user-read-recently-played`
  - A lib `spotipy` (`SpotifyOAuth`) cuida do fluxo, do cache e do refresh do token.
- **Limitações (importantes para o escopo — situação 2026):**
  - **Premium obrigatório** para usar a Web API.
  - **Descontinuados para apps novos** (nov/2024): audio-features, audio-analysis, recommendations, related-artists, featured-playlists.
  - **Removidos em fev/2026:** o campo **`popularity`**, `available_markets`, e vários endpoints de catálogo (artist top tracks, new releases, markets, get several tracks, playlists de terceiros). **Não planejar nada em cima de popularidade/recomendações.**
  - A API **não** devolve histórico completo — só ~50 reproduções recentes. Histórico inteiro desde 2020 só via **"Download your data"** (export manual), fora do escopo da pipeline.
  - **Continuam disponíveis** os endpoints de personalização do próprio usuário usados aqui (top items, saved tracks, recently played).
- **Docs:** https://developer.spotify.com/documentation/web-api · changelog fev/2026: https://developer.spotify.com/documentation/web-api/references/changes/february-2026

#### Endpoint 1: `GET /me/top/{type}` — Get User's Top Items

`type` = `tracks` ou `artists`. Scope: `user-top-read`.

| Parâmetro | Descrição |
|---|---|
| `time_range` | `long_term` (~1 ano), `medium_term` (~6 meses), `short_term` (~4 semanas). Default `medium_term` |
| `limit` | padrão 20, **máximo 50** |
| `offset` | paginação |

**Resposta:** objeto paginado com `items[]` (objetos *track* ou *artist*). Campos úteis de um *track*:

| Campo | Uso |
|---|---|
| `items[].id` | id Spotify da faixa |
| `items[].name` | nome da faixa |
| `items[].artists[].id` / `.name` | id/nome do(s) artista(s) |
| `items[].album.id` / `.name` / `.images[]` | álbum e capa |
| `items[].duration_ms` | duração |
| `items[].external_urls.spotify` | link |
| (posição no `items[]` = ranking) | base do `fato_top_spotify` |

#### Endpoint 2: `GET /me/tracks` — Get User's Saved Tracks

Scope: `user-library-read`. Params: `limit` (máx 50), `offset`, `market` (opcional).

**Resposta:** `items[]` com `added_at` (ISO 8601) + `track` (mesma estrutura acima). Usado para marcar `dim_faixa.na_biblioteca` e a data de salvamento.

#### Endpoint 3 (opcional): `GET /me/player/recently-played`

Scope: `user-read-recently-played`. Devolve as **últimas ~50** reproduções com `track` + `played_at` (ISO 8601). Em grande parte redundante com o Last.fm; útil só como reconciliação pontual.

---

## 5. Modelo de dados (esquema estrela / constelação)

Duas tabelas de fato compartilhando as mesmas dimensões.

**Fatos**
- `fato_audicoes` — grão: 1 scrobble (Last.fm). `UNIQUE (scrobble_uts, faixa_id)` → idempotência.
- `fato_top_spotify` — grão: 1 item no ranking de uma coleta. (`snapshot_date`, `time_range`, `tipo` [track|artist], `posicao`, `faixa_id`/`artista_id`). Permite ver a evolução do "top" do Spotify e cruzar com o mais-tocado do Last.fm.

**Dimensões (enriquecidas com Spotify)**
- `dim_artista` (nome, mbid, **spotify_artist_id**)
- `dim_faixa` (nome, album, artista_id, **spotify_track_id**, **na_biblioteca**, **biblioteca_added_at**)
- `dim_tempo` (data, ano, mes, dia, hora, dia_semana)

**De-para API → warehouse**

| Origem | Campo da API | Destino |
|---|---|---|
| Last.fm | `track.artist.#text` | `dim_artista.nome` |
| Last.fm | `track.name` / `track.album.#text` | `dim_faixa.nome` / `.album` |
| Last.fm | `track.date.uts` | `fato_audicoes.scrobble_uts` + deriva `dim_tempo` |
| Spotify (top) | `items[].id` / `items[].artists[].id` | `dim_faixa.spotify_track_id` / `dim_artista.spotify_artist_id` |
| Spotify (top) | posição + `time_range` + data da coleta | `fato_top_spotify` |
| Spotify (saved) | presença em `/me/tracks` + `added_at` | `dim_faixa.na_biblioteca` / `.biblioteca_added_at` |

> Casamento entre fontes: faixas/artistas são resolvidos por nome (chave de negócio); os `spotify_*_id` e o `mbid` ficam como atributos de apoio, já que nem sempre vêm preenchidos.

---

## 6. Pipeline / fluxo ETL

Duas esteiras, cadências diferentes, mesmas camadas medalhão.

### DAG `pipeline_audicoes` (Last.fm — diária)
Gatilho `@daily`; janela = dia anterior; `catchup=False`. A janela vira o `from`/`to` (incremental).
1. **Extrair → `raw`:** `getRecentTracks` com `from`/`to`, paginando; descarta "now playing"; grava JSON em `raw/lastfm/...`.
2. **Transformar → `processed`:** achata, limpa (duplicados, tipos, `mbid` vazio), grava Parquet.
3. **Carregar → `analytics`:** upsert de dimensões + insert em `fato_audicoes` (`ON CONFLICT DO NOTHING`).

Carga histórica (1×): extração **sem** `from`, paginando até o fim (run manual/script à parte).

### DAG `pipeline_spotify` (Spotify — semanal)
Gatilho `@weekly` (o "top" é computado em janelas de semanas/meses; não faz sentido diário). Requer token OAuth válido (refresh automático via `spotipy`).
1. **Extrair → `raw`:** chama `/me/top/tracks` e `/me/top/artists` (nos três `time_range`) e `/me/tracks`; grava JSON em `raw/spotify/...`.
2. **Transformar → `processed`:** normaliza para Parquet (top items com posição/time_range; biblioteca com `added_at`).
3. **Carregar/enriquecer → `analytics`:** atualiza `dim_artista`/`dim_faixa` (ids do Spotify, `na_biblioteca`) e insere `fato_top_spotify` (idempotente por `snapshot_date` + `time_range` + posição).

**Falhas (ambas):** `retries=2`, `retry_delay=5min`; falha persistente deixa a task vermelha e alerta. Raw imutável + etapas idempotentes ⇒ reexecução segura.

---

## 7. Requisitos

### Funcionais
- **RF1** — Ingerir scrobbles do Last.fm incrementalmente (janela diária).
- **RF2** — Suportar carga histórica completa do Last.fm (backfill).
- **RF3** — Ingerir do Spotify (OAuth): top tracks/artists (3 time_ranges) e biblioteca salva.
- **RF4** — Persistir dado cru (`raw`) e tratado em Parquet (`processed`) para ambas as fontes.
- **RF5** — Carregar esquema constelação: `fato_audicoes`, `fato_top_spotify` e dimensões compartilhadas.
- **RF6** — Enriquecer dimensões com ids e biblioteca do Spotify.
- **RF7** — Garantir idempotência (reprocessar não duplica).
- **RF8** *(Fase 5)* — Validações de qualidade entre etapas.

### Não-funcionais
- **RNF1** — Orquestração observável (UI, logs, retries, alertas) via Airflow; duas DAGs.
- **RNF2** — Tudo containerizado (`docker-compose`), reproduzível localmente.
- **RNF3** — Credenciais fora do código (`.env`): Last.fm key e Spotify `client_id`/`client_secret`; **nunca** commitadas.
- **RNF4** — Token OAuth do Spotify gerenciado com cache + refresh automático (não pedir login a cada run).
- **RNF5** — Storage desacoplado de compute (MinIO ↔ Postgres).
- **RNF6** — Código versionado no GitHub desde o dia 1.

---

## 8. Critérios de sucesso (definição de pronto)

- [ ] DAG `pipeline_audicoes` **verde** de ponta a ponta; histórico do Last.fm carregado.
- [ ] DAG `pipeline_spotify` **verde**; dimensões enriquecidas e `fato_top_spotify` populado.
- [ ] Consultas analíticas respondendo, incluindo o cruzamento **Last.fm × Spotify** (mais-tocado vs top, e marcação "está na biblioteca").
- [ ] Falha provocada → Airflow tenta de novo e alerta; nada de lixo no warehouse.
- [ ] Repositório com README, diagrama, este PRD, `docker-compose.yml`, código das DAGs.
- [ ] Post de portfólio (problema → solução → decisões → aprendizados).

---

## 9. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| **Spotify Premium obrigatório** | premissa declarada; sem Premium, o escopo Spotify é removido e o projeto segue só com Last.fm |
| Complexidade do OAuth do Spotify | usar `spotipy` (cuida de fluxo, cache e refresh); scopes mínimos |
| **Volatilidade da API do Spotify** (mudanças nov/2024 e fev/2026) | usar só endpoints de personalização do usuário; não depender de popularidade/recommendations; fixar versões |
| Limite informal do Last.fm | paginação educada, poucas req/s |
| Chaves/segredos expostos | `.env` + `.gitignore` |
| Histórico do Spotify incompleto pela API | aceitar (só ~50 recentes); histórico é responsabilidade do Last.fm |
| Casamento de faixas entre fontes | resolver por nome (chave de negócio); ids como apoio |
| Subir o Airflow (parte sensível) | `docker-compose` fixado em versão; fallback no compose oficial |

---

## 10. Roadmap (alinhado às fases do guia)

| Fase | Entrega |
|---|---|
| 1 — Python | extração Last.fm funcionando, `raw` no MinIO |
| 2 — SQL/Postgres | esquema constelação criado; consultas analíticas |
| 3 — Docker/MinIO | `docker-compose` subindo o lake |
| 4 — Airflow | `transform`/`load` + DAG `pipeline_audicoes` verde |
| 4b — Spotify | app + OAuth; DAG `pipeline_spotify` (top + biblioteca) enriquecendo as dimensões |
| 5 — Qualidade | tasks de validação |
| 6 — Portfólio | README, diagrama, este PRD, post |

---

## 11. Considerações de engenharia da ingestão (Cap. 7, aplicado ao projeto)

> As perguntas-guia de ingestão de *Fundamentos de Engenharia de Dados* (Cap. 7, lido na Fase 4), respondidas para este projeto. Consolidam, sob a ótica da ingestão, decisões que aparecem espalhadas nas seções 4 (fontes), 6 (fluxo) e 9 (riscos).

**1. Quais são os casos de uso? Dá para reutilizar os dados em vez de versioná-los?**
Casos de uso: análise do histórico (artista/faixa mais ouvidos, padrões por hora/dia/mês, evolução do gosto) e o cruzamento Last.fm × Spotify. A reutilização é garantida pelo modelo medalhão: o `raw` é imutável e funciona como fonte única — qualquer análise nova reprocessa o `raw` existente, sem rechamar a API. Não se criam N cópias do mesmo dado; há uma origem (`raw`) e camadas derivadas a partir dela.

**2. A fonte gera/coleta de forma confiável? O dado está disponível quando preciso?**
O Last.fm é estável. O cuidado é o limite informal dos ToS (~5 req/s por IP); a mitigação é `sleep` entre páginas e os `retries` da task no Airflow (§6). Em lote diário isso não é gargalo. Aqui "disponível quando preciso" depende mais do **Airflow estar no ar** do que da API. Com `raw` imutável + etapas idempotentes, reexecutar é seguro.

**3. Qual o destino dos dados após a ingestão?**
Destino imediato da ingestão: **MinIO, bucket `raw`** (JSON cru, bronze). Destino final: **Postgres analítico** (`fato_audicoes` + dimensões, ouro), passando por `processed` (Parquet, prata). O `extract.py` se ocupa **apenas** do `raw` — ingestão não se mistura com transformação.

**4. Com que frequência preciso acessar os dados?**
Distinguir duas frequências: **ingestão** (DAG `@daily`, janela do dia anterior, incremental via `from`/`to`) e **consulta** (warehouse, ad hoc). Após a carga histórica, o parâmetro `from` puxa só o que é novo desde a última execução — **carga incremental**, evitando rebaixar a API inteira a cada run.

**5. Qual o volume esperado?**
Pequeno: cada scrobble são poucos bytes; o histórico todo fica na casa de milhares a dezenas de milhares de linhas. O que dimensiona a carga histórica é a paginação (`limit` máx. 200 → todo o histórico em poucas dezenas de chamadas). **Não há cenário de big data** — pandas + Parquet bastam; nada de Spark ou particionamento complexo.

**6. Em que formato vêm os dados? O downstream lida com ele?**
A API devolve **JSON** (`format=json`); o `transform.py` converte para **Parquet** na camada `processed`. JSON é bom para ingestão (é o que a API fala, preserva tudo) e ruim para análise (verboso, sem tipos); Parquet — colunar, tipado, comprimido — alimenta o Postgres sem dor. Cada formato no seu trecho da jornada.

**7. A origem está pronta para uso imediato? Por quanto tempo vale e o que a inutiliza?**
Não totalmente: o JSON cru exige tratamento — descartar `nowplaying` (vem sem `date`), lidar com `mbid`/`album` vazios e achatar a estrutura aninhada. O `raw` é histórico **imutável** (um scrobble de 2020 não muda; vale como registro permanente). O que "inutiliza" não é a idade, e sim **duplicação** (incremento mal feito) ou **schema drift** (a API mudar um campo). Defesa: deduplicação na transformação e validação na carga (Fase 5).

**8. Sendo streaming, precisa transformar em trânsito?**
**Não se aplica.** A fonte é **batch** (um lote da API por janela), não um fluxo contínuo. O modelo é **ETL/ELT em lote**: transforma-se **em repouso** (camada prata), não em trânsito. Transformação em trânsito (Kafka Streams, Flink) é para streaming de verdade — fora do escopo.

> **Prioridades para o `transform.py` / `load.py`:** as duas decisões que mais definem a corretude do núcleo são (1) **carga incremental** via `from` + persistir "até onde já carreguei" entre execuções, e (2) **idempotência / deduplicação** — filtrar `nowplaying` e usar `UNIQUE (scrobble_uts, faixa_id)` com `ON CONFLICT DO NOTHING` (§5 e §6). Ambas já previstas no modelo e no fluxo.

---

## Apêndice — Referências

- Last.fm — getRecentTracks: https://www.last.fm/api/show/user.getRecentTracks
- Last.fm — criar API key: https://www.last.fm/api/account/create
- Spotify Web API: https://developer.spotify.com/documentation/web-api
- Spotify — Get User's Top Items: https://developer.spotify.com/documentation/web-api/reference/get-users-top-artists-and-tracks
- Spotify — Get User's Saved Tracks: https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
- Spotify — mudanças nov/2024: https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api
- Spotify — changelog fev/2026: https://developer.spotify.com/documentation/web-api/references/changes/february-2026
- Spotipy (lib OAuth): https://spotipy.readthedocs.io/
- Apache Airflow: https://airflow.apache.org/docs/ · MinIO: https://min.io/docs/ · PyArrow: https://arrow.apache.org/docs/python/
