# PRD — Pipeline de Audições (Last.fm + Spotify)

| | |
|---|---|
| **Projeto** | Pipeline de engenharia de dados de histórico musical |
| **Versão** | 0.3 (*as-built* — alinhado ao código em produção) |
| **Data** | Julho de 2026 |
| **Stacks** | MinIO (S3) · Apache Airflow · PostgreSQL |
| **Status** | Em construção — núcleo Last.fm e Fase 2b (Spotify) implementados; ver §8 e §9.1 |


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

> Esta lista é o escopo **pretendido**, mantida como escrita no planejamento. O quanto cada
> item está de fato atendido está na **§7**; onde a prática ainda divirge, na **§9.1**.

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

Implementado em [`db/schema.sql`](../db/schema.sql). O que segue reflete o schema real.

**Fatos**
- `fato_audicoes` — grão: 1 scrobble (Last.fm). Colunas: `id`, `scrobble_uts`, `faixa_id` → `dim_faixa`, `tempo_id` → `dim_tempo`. `UNIQUE (scrobble_uts, faixa_id)` → idempotência.
- `fato_top_spotify` — grão: 1 item no ranking de uma coleta. Colunas: `id`, `snapshot_date`, `time_range`, `tipo` (`track`|`artist`), `posicao`, `faixa_id`, `artista_id`. `UNIQUE (snapshot_date, time_range, tipo, posicao)`. Permite ver a evolução do "top" do Spotify e cruzar com o mais-tocado do Last.fm.
  - As duas FKs são **nulas por construção**: linha de `tipo='track'` preenche `faixa_id` e deixa `artista_id` nulo, e vice-versa. Não há `CHECK` garantindo essa exclusividade — ver **D4**.

**Dimensões (enriquecidas com Spotify)**
- `dim_artista` (`id`, `nome` **UNIQUE**, `mbid`, `spotify_artist_id`)
- `dim_faixa` (`id`, `nome`, `album`, `artista_id`, `spotify_track_id`, `na_biblioteca`, `biblioteca_added_at`, **UNIQUE (`nome`, `artista_id`)**)
- `dim_tempo` (`id`, `data`, `hora`, `ano`, `mes`, `dia`, `dia_semana`, **UNIQUE (`data`, `hora`)**)

> Grão da `dim_tempo`: **hora cheia** (`UNIQUE (data, hora)`), não o instante do scrobble. O minuto/segundo exato vive só no `fato_audicoes.scrobble_uts`. Isso basta para as análises por hora/dia/mês previstas na §1.

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

Duas esteiras, cadências diferentes, mesmas camadas medalhão. **Esta seção descreve o
código como ele está hoje**; onde o comportamento fica aquém do pretendido, há uma
referência à dívida correspondente na §9.1.

### DAG `pipeline_audicoes` (Last.fm — diária)
[`dags/pipeline_audicoes.py`](../dags/pipeline_audicoes.py). Gatilho `@daily`, `catchup=False`, `retries=2`, `retry_delay=1min`. **Quatro** tasks em sequência: `descobrir_marca_dagua → extrair → transformar → carregar`.

1. **Descobrir a marca d'água → XCom:** `SELECT max(scrobble_uts) FROM fato_audicoes` e devolve o valor. É a resposta persistida à pergunta "até onde eu já carreguei". A marca é **derivada do próprio dado**, não um estado guardado em paralelo: se a carga funcionou, ela subiu; se falhou, ficou onde estava. Não existe cenário em que ela dessincroniza do warehouse — foi por isso que não se usou uma Airflow Variable nem um arquivo de controle.
2. **Extrair → `raw`:** lê a marca d'água do XCom e chama `user.getRecentTracks` com `from = marca + 1` (o `from` do Last.fm é inclusivo) e `limit=200`. **Pagina** por `@attr.totalPages`, com `sleep(0.25s)` entre páginas, e grava **uma página por objeto** em `raw/lastfm/incremental/<ts_nodash>/page_NNNN.json` — pasta única por execução, identificada pelo instante lógico do run. Devolve o prefixo no XCom. Casos de borda: marca d'água `None` (warehouse vazio) → falha explícita pedindo o backfill; janela vazia (`total = 0`) → `AirflowSkipException` **antes** de gravar, para não escrever objeto vazio.
3. **Transformar → `processed`:** recebe o prefixo pelo XCom, lista os objetos daquela pasta e concatena todas as páginas (tratando a esquisitice do Last.fm de devolver `track` como objeto, não lista, quando há um único resultado). Achata com `pd.json_normalize`, seleciona as 6 colunas de interesse, converte `date.uts`, **descarta o `nowplaying`** (vem sem `date` → `dropna`), deduplica por (`scrobble_uts`, `faixa`) e grava `processed/lastfm/recent.parquet`.
4. **Carregar → `analytics`:** lê o Parquet e itera **linha a linha**: upsert em `dim_artista` (`ON CONFLICT (nome)`), `dim_faixa` (`ON CONFLICT (nome, artista_id)`) e `dim_tempo` (`ON CONFLICT (data, hora)`), cada um com `RETURNING id`; por fim insere em `fato_audicoes` com `ON CONFLICT (scrobble_uts, faixa_id) DO NOTHING`.

**Duas garantias diferentes, e é importante não confundi-las.** A **idempotência** (`UNIQUE` + `ON CONFLICT`) garante que reprocessar não duplica. A **incrementalidade** (marca d'água + paginação) garante que nada é *deixado para trás*: a janela é determinada pelo que falta carregar, não por um número fixo de registros, e a paginação cobre janelas de qualquer tamanho — um intervalo longo sem execução entra inteiro na execução seguinte. As duas se cobrem mutuamente no ponto fraco da outra: paginar uma lista que continua crescendo é instável, e o que se duplicar por causa disso o `ON CONFLICT` descarta, enquanto o que escapar volta no run seguinte, porque a marca d'água não avançou além dele.

**Imutabilidade por camada.** O `raw` é imutável: cada execução escreve numa pasta própria e nunca sobrescreve a anterior. O `processed` é deliberadamente **sobrescrito** — ele é derivado e regenerável, e a marca d'água garante o reprocessamento: se a `transformar` funciona e a `carregar` falha, o `scrobble_uts` não entra no warehouse, a marca não avança, e a execução seguinte volta a buscar exatamente aquela janela. Perder a camada prata não custa nada; perder o bronze custaria uma nova rodada de chamadas à API.

### Carga histórica — `scripts/backfill.py` (one-off, fora do Airflow)
[`scripts/backfill.py`](../scripts/backfill.py), rodado no host (MinIO em `localhost:9000`, Postgres em `localhost:5433`). É aqui que o medalhão acontece como planejado:

1. **Bronze:** pagina o histórico inteiro via `@attr.totalPages`, com `sleep(0.25s)` entre páginas e retry próprio; grava **uma página por objeto** em `raw/lastfm/backfill/page_NNNN.json` — bronze de fato imutável.
2. **Prata:** mesma limpeza do item 2 acima, mais as colunas de tempo derivadas (`data`, `hora`, `ano`, `mes`, `dia`, `dia_semana`), em `processed/lastfm/backfill.parquet`.
3. **Ouro:** carga em **lote** com `execute_values` (não linha a linha — 60k linhas exigem isso), resolvendo os ids das dimensões via dicionários em memória.

Executado uma vez: **61.338 scrobbles, 72 meses (ago/2020 → jul/2026)**.

### DAG `pipeline_spotify` (Spotify — semanal)
[`dags/pipeline_spotify.py`](../dags/pipeline_spotify.py). Gatilho `@weekly` (o "top" é computado em janelas de semanas/meses; não faz sentido diário), `catchup=False`, mesmos retries. **Três** tasks: `extrair → transformar → carregar` — não há marca d'água aqui, porque o "top" do Spotify é um retrato do momento, não um histórico de eventos: não existe "o que falta buscar", cada coleta pega o estado atual inteiro. OAuth via `spotipy` com `cache_path=/opt/airflow/.cache` e `open_browser=False` — o container não abre navegador, então reutiliza o `refresh_token` do cache ou falha limpo.

1. **Extrair → `raw`:** para cada um dos três `time_range`, chama `/me/top/tracks` e `/me/top/artists` (`limit=50`); mais `/me/tracks` (`limit=50`, **sem `offset`** → biblioteca truncada, ver **D5**). Sete JSONs em `raw/spotify/`, chaves fixas.
2. **Transformar → `processed`:** normaliza em três Parquets — `top_tracks.parquet` (posição via `enumerate`, `time_range`, faixa, artista, álbum, ids), `top_artists.parquet` e `saved_tracks.parquet` (com `added_at`). Só o **primeiro artista** de cada faixa é considerado.
3. **Carregar/enriquecer → `analytics`:** `UPDATE` em `dim_artista.spotify_artist_id`, `dim_faixa.spotify_track_id` e `dim_faixa.na_biblioteca`/`biblioteca_added_at`, casando **por nome**; depois insere em `fato_top_spotify` com `ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING`, resolvendo as FKs por subselect de nome.

**Consequência do item 3:** o enriquecimento é `UPDATE` puro — ele **só alcança artistas/faixas que já existem** nas dimensões, ou seja, que já foram scrobblados no Last.fm. Um top track do Spotify nunca tocado fora dele não entra (**D3**), e o `INSERT` na fato com subselect sem correspondência grava a linha com FK **nula**, sem erro (**D4**).

### Falhas e reexecução
`retries=2`, `retry_delay=1min` nas duas DAGs; falha persistente deixa a task vermelha na UI. As etapas são **idempotentes** (`ON CONFLICT DO NOTHING`/`DO UPDATE`), então reexecutar é seguro — não gera duplicata. E, no Last.fm, reexecutar também **recupera**: uma falha não faz a marca d'água avançar, então a janela perdida volta na execução seguinte, de qualquer tamanho. Uma exceção declarada: janela vazia é `skipped`, não `success` — reexecutar não ajudaria, e tratar como falha dispararia retries inúteis.

---

## 7. Requisitos

Legenda: ✅ atendido · ⚠️ atendido parcialmente · ⏳ não iniciado.

### Funcionais

| | Requisito | Status |
|---|---|---|
| **RF1** | Ingerir scrobbles do Last.fm incrementalmente (janela diária) | ✅ marca d'água (`max(scrobble_uts)`) como `from`, com paginação |
| **RF2** | Suportar carga histórica completa do Last.fm (backfill) | ✅ `scripts/backfill.py`; 61.338 scrobbles carregados |
| **RF3** | Ingerir do Spotify (OAuth): top tracks/artists (3 time_ranges) e biblioteca salva | ⚠️ tops completos; biblioteca truncada em 50 — **D5** |
| **RF4** | Persistir dado cru (`raw`) e tratado em Parquet (`processed`) para ambas as fontes | ⚠️ `raw` imutável por execução no Last.fm; no Spotify as chaves ainda são fixas |
| **RF5** | Carregar esquema constelação: `fato_audicoes`, `fato_top_spotify` e dimensões compartilhadas | ✅ `db/schema.sql`, ambas as fatos populadas |
| **RF6** | Enriquecer dimensões com ids e biblioteca do Spotify | ⚠️ só alcança o que já existe nas dimensões — **D3** |
| **RF7** | Garantir idempotência (reprocessar não duplica) | ✅ `UNIQUE` + `ON CONFLICT` em todas as tabelas |
| **RF8** *(Fase 5)* | Validações de qualidade entre etapas | ⏳ planejado |

### Não-funcionais

| | Requisito | Status |
|---|---|---|
| **RNF1** | Orquestração observável (UI, logs, retries, alertas) via Airflow; duas DAGs | ⚠️ UI, logs e `retries=2` ok; **alertas não configurados** (sem e-mail/webhook) |
| **RNF2** | Tudo containerizado (`docker-compose`), reproduzível localmente | ⚠️ Airflow, MinIO, Postgres e Redis no compose; o `backfill.py` roda no host |
| **RNF3** | Credenciais fora do código (`.env`); **nunca** commitadas | ✅ `.env` no `.gitignore`, `.env.example` versionado |
| **RNF4** | Token OAuth do Spotify com cache + refresh automático | ✅ `.cache` montado no container, `open_browser=False` |
| **RNF5** | Storage desacoplado de compute (MinIO ↔ Postgres) | ✅ |
| **RNF6** | Código versionado no GitHub desde o dia 1 | ✅ |

---

## 8. Critérios de sucesso (definição de pronto)

- [x] DAG `pipeline_audicoes` **verde** de ponta a ponta; histórico do Last.fm carregado.
- [x] DAG `pipeline_spotify` **verde**; dimensões enriquecidas e `fato_top_spotify` populado.
- [ ] Consultas analíticas respondendo, incluindo o cruzamento **Last.fm × Spotify** (mais-tocado vs top, e marcação "está na biblioteca").
      → "artista mais ouvido por mês" ✅; o **cruzamento entre as fontes** é o que falta.
- [ ] Falha provocada → Airflow tenta de novo e alerta; nada de lixo no warehouse.
      → o retry funciona; **alerta não existe** (RNF1) e o "nada de lixo" tem a ressalva da **D4**.
- [x] Repositório com README, diagrama, este PRD, `docker-compose.yaml`, código das DAGs.
- [x] Post de portfólio (problema → solução → decisões → aprendizados). — dois posts publicados.

---

## 9. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| **Spotify Premium obrigatório** | premissa declarada; sem Premium, o escopo Spotify é removido e o projeto segue só com Last.fm |
| Complexidade do OAuth do Spotify | usar `spotipy` (cuida de fluxo, cache e refresh); scopes mínimos |
| **Volatilidade da API do Spotify** (mudanças nov/2024 e fev/2026) | usar só endpoints de personalização do usuário; não depender de popularidade/recommendations. *Obs.: "fixar versões" constava aqui como mitigação, mas o `requirements.txt` não tem nenhum pin — a mitigação não existe hoje (ver §9.1, menores).* |
| Limite informal do Last.fm | paginação educada, poucas req/s |
| Extração não-incremental (gap maior que 200 scrobbles não entra no carregamento automático) | **resolvido** — marca d'água + paginação (§6); qualquer tamanho de intervalo entra na execução seguinte |
| Chaves/segredos expostos | `.env` + `.gitignore` |
| Histórico do Spotify incompleto pela API | aceitar (só ~50 recentes); histórico é responsabilidade do Last.fm |
| Casamento de faixas entre fontes | resolver por nome (chave de negócio); ids como apoio |
| Subir o Airflow (parte sensível) | `docker-compose` fixado em versão; fallback no compose oficial |

### 9.1 Dívidas técnicas conhecidas

Levantadas na revisão v0.3, comparando este PRD com o código de fato existente.
Nenhuma delas impede o sistema de rodar; todas são divergências entre o que o
projeto **diz** e o que ele **faz**. Ordenadas por relevância.

| # | Dívida | Onde | Impacto | Encaminhamento |
|---|---|---|---|---|
| **D2** | **O `raw` do Spotify não é imutável.** As sete chaves (`spotify/top_*_<time_range>.json`, `spotify/saved_tracks.json`) são fixas e sobrescritas a cada execução semanal. | `dags/pipeline_spotify.py` (`extrair`) | Cada coleta é um **retrato** do gosto naquela semana; sobrescrever apaga o retrato anterior. A `fato_top_spotify` preserva a série por `snapshot_date`, mas o bronze correspondente não existe mais — não há como reprocessar uma semana passada. | Particionar por execução, como já é feito no Last.fm (`spotify/<ts_nodash>/…`). |
| **D3** | **O enriquecimento do Spotify é `UPDATE` puro.** Ele altera linhas existentes, nunca insere. | `dags/pipeline_spotify.py` (`carregar`) | Artista ou faixa que está no top do Spotify mas nunca foi scrobblado no Last.fm **não entra no warehouse** — o `UPDATE` não casa nenhuma linha e não dá erro. | Trocar por `INSERT ... ON CONFLICT DO UPDATE` (upsert), como já é feito na esteira do Last.fm. |
| **D4** | **`fato_top_spotify` aceita FK nula sem reclamar.** O `INSERT` resolve `faixa_id`/`artista_id` por subselect; sem correspondência, o subselect devolve `NULL` e a linha é gravada assim mesmo. | `dags/pipeline_spotify.py` (`carregar`) | Lixo silencioso na tabela de fato — consequência direta da **D3**. Diagnóstico: `SELECT count(*) FROM fato_top_spotify WHERE tipo='track' AND faixa_id IS NULL;` | Resolver a **D3** elimina a causa; um `CHECK` garantindo exatamente uma FK preenchida por linha fecha a porta. |
| **D5** | **A biblioteca do Spotify está truncada em 50 faixas.** `/me/tracks` é chamado com `limit=50` e sem `offset`. | `dags/pipeline_spotify.py` (`extrair`) | `dim_faixa.na_biblioteca` só é verdadeiro para as 50 faixas salvas mais recentes; o resto da biblioteca é invisível. | Paginar por `offset` até esgotar (o campo `total` da resposta diz quantas são). |
| **D6** | **`na_biblioteca` nunca recebe `FALSE`.** Só é marcada `TRUE` para o que veio de `/me/tracks`; as demais ficam `NULL`. | `db/schema.sql` + `pipeline_spotify.py` | Coluna com três estados (`TRUE`/`NULL`/`FALSE`) onde a semântica pretendida é booleana. Um `WHERE na_biblioteca = FALSE` devolve zero linhas quando deveria devolver muitas. | `DEFAULT FALSE NOT NULL` na coluna, ou marcar explicitamente o complemento na carga. |
| **D8** | **A fato do Spotify usa `date.today()` em vez da data lógica da execução.** | `dags/pipeline_spotify.py` (`carregar`) | Reexecutar uma run antiga carimba o `snapshot_date` de hoje, não o da execução original — a série histórica de "tops" fica errada nesse caso. | Usar `data_interval_start` / `logical_date` do contexto do Airflow. |

**Notas menores** (não numeradas): o `requirements.txt` **não fixa nenhuma versão** — só nomes de pacote, sem pin, apesar de a §9 listar "fixar versões" como mitigação da volatilidade das APIs; a carga da DAG do Last.fm é linha a linha, aceitável para 200 linhas mas não para lotes grandes — por isso o `backfill.py` usa `execute_values`; há um `SELECT count(*)` sem `fetch` sobrando em `carregar()`; conexões não são fechadas explicitamente ao fim das tasks; e o `transform` do Spotify considera apenas o **primeiro** artista de cada faixa (faixas colaborativas perdem os demais).

---

## 10. Roadmap (alinhado às fases do guia)

| Fase | Entrega | Status |
|---|---|---|
| 1 — Python | extração Last.fm funcionando, `raw` no MinIO | ✅ |
| 2 — SQL/Postgres | esquema constelação criado; primeira query analítica ("artista mais ouvido por mês") | ✅ a query que **cruza as duas fontes** pertence à fase 4b, abaixo |
| 3 — Docker/MinIO | `docker-compose` subindo o lake | ✅ |
| 4 — Airflow | `transform`/`load` + DAG `pipeline_audicoes` verde | ✅ |
| 4b — Spotify | app + OAuth; DAG `pipeline_spotify` (top + biblioteca) enriquecendo as dimensões | ✅ falta a query cruzada Last.fm × Spotify |
| 5 — Qualidade | tasks de validação | ⏳ |
| 6 — Portfólio | README, diagrama, este PRD, post | ✅ dois posts publicados |

**Trilha paralela — quitar as dívidas da §9.1.** Não faz parte das fases do guia e não
tem entrega de portfólio associada; é manutenção. Ordem sugerida: D3 + D4 (juntas, mesma
causa) → D2 → D5, D6, D8. Dívida quitada sai desta seção; quando a lista esvaziar, a seção
sai com ela.

---

## 11. Considerações de engenharia da ingestão (Cap. 7, aplicado ao projeto)

> As perguntas-guia de ingestão de *Fundamentos de Engenharia de Dados* (Cap. 7, lido na Fase 4), respondidas para este projeto. Consolidam, sob a ótica da ingestão, decisões que aparecem espalhadas nas seções 4 (fontes), 6 (fluxo) e 9 (riscos).

**1. Quais são os casos de uso? Dá para reutilizar os dados em vez de versioná-los?**
Casos de uso: análise do histórico (artista/faixa mais ouvidos, padrões por hora/dia/mês, evolução do gosto) e o cruzamento Last.fm × Spotify. A reutilização vem do modelo medalhão: há uma origem (`raw`) e camadas derivadas a partir dela, não N cópias do mesmo dado.

Isso é verdade na esteira do Last.fm: tanto o `backfill.py` (uma página por objeto em `lastfm/backfill/`) quanto a DAG diária (uma pasta por execução em `lastfm/incremental/<ts_nodash>/`) nunca sobrescrevem. Qualquer análise nova reprocessa esses objetos sem rechamar a API. A camada `processed` é a exceção deliberada — ela é derivada e regenerável, e a marca d'água garante o reprocessamento (§6).

**Onde ainda não vale:** na esteira do Spotify, que grava em sete chaves fixas e sobrescreve o retrato da semana anterior. Dívida **D2**.

**2. A fonte gera/coleta de forma confiável? O dado está disponível quando preciso?**
O Last.fm é estável. O cuidado é o limite informal dos ToS (~5 req/s por IP); a mitigação é `sleep` entre páginas e os `retries` da task no Airflow (§6). Em lote diário isso não é gargalo. Aqui "disponível quando preciso" depende mais do **Airflow estar no ar** do que da API.

Com `raw` imutável + etapas idempotentes + marca d'água, reexecutar é seguro nos dois sentidos: não duplica e não deixa buraco — o que falhou volta na execução seguinte. A ressalva que resta é do Spotify, cujo `raw` ainda é sobrescrito (**D2**).

**3. Qual o destino dos dados após a ingestão?**
Destino imediato da ingestão: **MinIO, bucket `raw`** (JSON cru, bronze). Destino final: **Postgres analítico** (`fato_audicoes` + dimensões, ouro), passando por `processed` (Parquet, prata). A task de extração (`lastfm/extrair_para_minio.py`, replicada inline na DAG) se ocupa **apenas** do `raw` — ingestão não se mistura com transformação.

**4. Com que frequência preciso acessar os dados?**
Distinguir duas frequências: **ingestão** (DAG `@daily`) e **consulta** (warehouse, ad hoc). Após a carga histórica, o parâmetro `from` puxa só o que é novo desde a última execução — **carga incremental**, evitando rebaixar a API inteira a cada run.

O "desde a última execução" é o ponto delicado, e a implementação merece registro: a pipeline não confia no calendário do agendador para saber o que falta. Ela **pergunta ao warehouse** (`max(scrobble_uts)`) no início de cada run e usa a resposta como `from`. A diferença importa porque o calendário mente quando a execução falha, atrasa ou é disparada à mão, enquanto `max(scrobble_uts)` é sempre a verdade sobre o que está carregado. Combinado com a paginação, isso torna o tamanho do intervalo irrelevante: um dia ou um mês sem rodar entram inteiros na execução seguinte.

**5. Qual o volume esperado?**
Pequeno: cada scrobble são poucos bytes; o histórico todo fica na casa de milhares a dezenas de milhares de linhas. O que dimensiona a carga histórica é a paginação (`limit` máx. 200 → todo o histórico em poucas dezenas de chamadas). **Não há cenário de big data** — pandas + Parquet bastam; nada de Spark ou particionamento complexo.

**6. Em que formato vêm os dados? O downstream lida com ele?**
A API devolve **JSON** (`format=json`); a task de transformação (`lastfm/transformar.py`, replicada inline na DAG) converte para **Parquet** na camada `processed`. JSON é bom para ingestão (é o que a API fala, preserva tudo) e ruim para análise (verboso, sem tipos); Parquet — colunar, tipado, comprimido — alimenta o Postgres sem dor. Cada formato no seu trecho da jornada.

**7. A origem está pronta para uso imediato? Por quanto tempo vale e o que a inutiliza?**
Não totalmente: o JSON cru exige tratamento — descartar `nowplaying` (vem sem `date`), lidar com `mbid`/`album` vazios e achatar a estrutura aninhada. Quanto à validade: um scrobble de 2020 não muda, então o dado vale como registro permanente e a idade não o inutiliza. O que inutiliza é **duplicação** (incremento mal feito) ou **schema drift** (a API mudar um campo). Defesa: deduplicação na transformação e validação na carga (Fase 5, ainda pendente).

**Ressalva:** "o `raw` é um histórico imutável" vale para o Last.fm (backfill e DAG diária), não para o Spotify — ver resposta 1 e a dívida **D2**.

**8. Sendo streaming, precisa transformar em trânsito?**
**Não se aplica.** A fonte é **batch** (um lote da API por janela), não um fluxo contínuo. O modelo é **ETL/ELT em lote**: transforma-se **em repouso** (camada prata), não em trânsito. Transformação em trânsito (Kafka Streams, Flink) é para streaming de verdade — fora do escopo.

> **Balanço.** As duas decisões que definem a corretude do núcleo são (1) **carga incremental** — usar `from` e persistir "até onde já carreguei" entre execuções — e (2) **idempotência / deduplicação** — filtrar `nowplaying` e usar `UNIQUE (scrobble_uts, faixa_id)` com `ON CONFLICT DO NOTHING`. **As duas estão implementadas** na esteira do Last.fm.
>
> Vale registrar como a (1) ficou de fora por várias missões sem ninguém notar: a idempotência **mascara o sintoma**. Buscando sempre os 200 mais recentes e descartando o que já existia, a pipeline se comportava como incremental no dia a dia — o dado chegava certo, nada duplicava, nenhuma task ficava vermelha. A falha só apareceria num intervalo longo sem execução, e apareceria em silêncio. Foi a revisão do PRD contra o código (v0.3) que expôs isso, não um erro em produção. É o argumento prático para reler a documentação contra a implementação de vez em quando: alguns defeitos não se manifestam como falha, só como ausência.

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
