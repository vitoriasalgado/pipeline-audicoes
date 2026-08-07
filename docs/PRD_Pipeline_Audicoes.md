# PRD — Pipeline de Audições (Last.fm + Spotify)

| | |
|---|---|
| **Projeto** | Pipeline de engenharia de dados de histórico musical |
| **Versão** | 0.4 (*as-built* — alinhado ao código em produção) |
| **Data** | Agosto de 2026 |
| **Stacks** | MinIO (S3) · Apache Airflow · PostgreSQL |
| **Status** | Escopo concluído e em operação — as duas esteiras rodando; ver §8 (critérios) e §9.1 (limitações) |


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
> item está de fato atendido está na **§7**, e o que fica de fora, na **§9.1**.

### Não-objetivos (fora do escopo atual)
- ~~Tasks de **qualidade de dados** dedicadas~~ — eram não-objetivo no planejamento; foram implementadas depois (RF8, §6).
- Dashboard visual (BI) — primeiro o dado correto, depois a visualização.
- Deploy em nuvem — roda 100% local/containerizado.
- Recomendação musical / ML — explicitamente fora.
- Recursos do Spotify **descontinuados** (audio features, recommendations, popularidade) — ver 4.2.

### Pré-requisitos e premissas
- Conta no **Last.fm** + API key (gratuita, sem OAuth).
- **Spotify Premium** — desde 2026 é **obrigatório** para usar a Web API e para registrar o app. *(Premissa: o autor tem ou terá Premium. Sem isso, o escopo Spotify cai.)*
- Conta do Spotify possivelmente já conectada ao Last.fm (nesse caso os scrobbles já incluem as reproduções do Spotify).
- Docker + Docker Compose; **Python 3.11+** no host (o `requirements.txt` fixa `pandas==3.0`, que não instala no 3.10). Dentro da imagem do Airflow o Python é 3.12, com versões próprias — ver §9.1.

---

## 3. Arquitetura (resumo)

```
Last.fm API ──► raw/  ──► processed/ ──►┐
(scrobbles, diário)  (JSON)   (Parquet)  │   PostgreSQL (warehouse - ouro)
                                         ├──► fato_audicoes ──► dim_tempo
Spotify API ──► raw/  ──► processed/ ──►┤    fato_top_spotify
(OAuth, semanal)     (JSON)   (Parquet)  │    as duas ──► dim_artista · dim_faixa
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
  - **Scopes:** `user-top-read` e `user-library-read` — os dois que a DAG pede. O `user-read-recently-played` seria necessário apenas para o endpoint 3 abaixo, que não é usado.
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

Duas tabelas de fato compartilhando as dimensões de artista e de faixa.

![Modelo em constelação: as duas DAGs alimentam duas fatos, que compartilham dim_artista e dim_faixa; a dim_tempo é exclusiva da fato_audicoes](modelo_constelacao.png)

*As duas esteiras chegam a fatos diferentes — `fato_audicoes` guarda um evento por audição, `fato_top_spotify` guarda uma posição por coleta — e ambas apontam para a mesma camada de dimensões. É essa camada compartilhada que torna possível cruzar as fontes com um `JOIN`, em vez de casar nomes na mão fora do banco.*

Implementado em [`db/schema.sql`](../db/schema.sql). O que segue reflete o schema real.

**Fatos**
- `fato_audicoes` — grão: 1 scrobble (Last.fm). Colunas: `id`, `scrobble_uts`, `faixa_id` → `dim_faixa`, `tempo_id` → `dim_tempo`. `UNIQUE (scrobble_uts, faixa_id)` → idempotência.
- `fato_top_spotify` — grão: 1 item no ranking de uma coleta. Colunas: `id`, `snapshot_date`, `time_range`, `tipo` (`track`|`artist`), `posicao`, `faixa_id`, `artista_id`. `UNIQUE (snapshot_date, time_range, tipo, posicao)`. Permite ver a evolução do "top" do Spotify e cruzar com o mais-tocado do Last.fm.
  - As duas FKs são **nulas por construção**: linha de `tipo='track'` preenche `faixa_id` e deixa `artista_id` nulo, e vice-versa. Não há `CHECK` garantindo a exclusividade, mas a carga torna a linha órfã impossível: a FK é o id devolvido pelo upsert da dimensão, não um subselect que pode não casar (§6).
  - **A `dim_tempo` não é compartilhada, e isso é escolha.** Só a `fato_audicoes` aponta para ela; a `fato_top_spotify` guarda `snapshot_date` como coluna própria. O motivo é o **grão**: a `dim_tempo` é de hora cheia, porque um scrobble acontece num instante; uma coleta do Spotify é um retrato semanal, e a hora dela não significa nada. Conformar exigiria ou inventar uma hora arbitrária para cada snapshot, ou rebaixar a `dim_tempo` para o dia e perder a análise por horário — que é uma das perguntas da §1. Então a constelação compartilha **duas** dimensões (`dim_artista`, `dim_faixa`), não três. Assimetria declarada, não esquecimento.

**Dimensões (enriquecidas com Spotify)**
- `dim_artista` (`id`, `nome`, `mbid`, `spotify_artist_id`) — único por **`lower(nome)`**
- `dim_faixa` (`id`, `nome`, `album`, `artista_id`, `spotify_track_id`, `na_biblioteca` **`NOT NULL DEFAULT FALSE`**, `biblioteca_added_at`) — único por **(`lower(nome)`, `artista_id`)**
  - A unicidade vem **só dos índices por `lower(nome)`**, não de constraints `UNIQUE` na coluna. As constraints por nome exato existiram até a migração `003`, que as removeu de propósito: enquanto elas existiam, um `ON CONFLICT (nome)` continuava sendo SQL válido mirando o índice errado, e só estourava violação quando a colisão acontecia no índice por `lower(nome)` — foi assim que o backfill quebrou numa instalação nova. Sem elas, o alvo errado deixa de ser escrevível e o erro aparece na hora.
  - `na_biblioteca` é booleana de verdade, com dois estados. Ela reflete a biblioteca do Spotify **no momento da última coleta**: a carga zera a coluna e remarca `TRUE` apenas o que veio na coleta atual, então faixa removida da biblioteca volta a `FALSE`. Sem isso, `WHERE na_biblioteca = FALSE` devolveria zero linhas, porque em SQL `NULL` não é `FALSE`.
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

> **Casamento entre fontes: a chave de negócio é o nome, comparado sem distinção de caixa.** Os `spotify_*_id` e o `mbid` ficam como atributos de apoio, já que nem sempre vêm preenchidos.
>
> A insensibilidade a caixa é obrigatória, não refinamento: as duas fontes escrevem a mesma pessoa de formas diferentes (`Zayn`/`ZAYN`, `Kiss of Life`/`KISS OF LIFE`), e o próprio Last.fm registra grafias distintas ao longo dos anos. É garantida por índice — `unique (lower(nome))` em `dim_artista` e `unique (lower(nome), artista_id)` em `dim_faixa` — e os upserts das duas DAGs usam esses índices no `ON CONFLICT`. A grafia da primeira ocorrência é a que fica; o `DO UPDATE` não altera a coluna `nome`.
>
> **Limite conhecido e aceito:** o casamento não resolve **sufixos de versão**. O Spotify (e às vezes o Last.fm) carimba `- 2014 Remaster`, `- Live At …`, `- Radio Edit`; hoje há ~184 faixas assim, e elas ficam como linhas distintas da versão sem sufixo. Normalizar exigiria uma lista de sufixos conhecidos, e há títulos legítimos com hífen (`Melô De Pra não Dar K.O - Reggae Funk`) que uma regra ingênua corromperia. Decisão: **não normalizar**. Uma gravação ao vivo é outra gravação; se remaster é a mesma faixa é modelagem em aberto, não defeito.

---

## 6. Pipeline / fluxo ETL

Duas esteiras, cadências diferentes, mesmas camadas medalhão. **Esta seção descreve o
código como ele está hoje**, incluindo as decisões que parecem erro e não são — elas
vêm com o motivo, para não serem "consertadas" depois. O que o projeto não faz está
na §9.1.

### DAG `pipeline_audicoes` (Last.fm — diária)
[`dags/pipeline_audicoes.py`](../dags/pipeline_audicoes.py). Gatilho `@daily`, `catchup=False`, `retries=2`, `retry_delay=1min`. **Cinco** tasks em sequência: `descobrir_marca_dagua → extrair → transformar → carregar → validar`.

1. **Descobrir a marca d'água → XCom:** `SELECT max(scrobble_uts) FROM fato_audicoes` e devolve o valor. É a resposta persistida à pergunta "até onde eu já carreguei". A marca é **derivada do próprio dado**, não um estado guardado em paralelo: se a carga funcionou, ela subiu; se falhou, ficou onde estava. Não existe cenário em que ela dessincroniza do warehouse — foi por isso que não se usou uma Airflow Variable nem um arquivo de controle.
2. **Extrair → `raw`:** lê a marca d'água do XCom e chama `user.getRecentTracks` com `from = marca + 1` (o `from` do Last.fm é inclusivo) e `limit=200`. **Pagina** por `@attr.totalPages`, com `sleep(0.25s)` entre páginas, e grava **uma página por objeto** em `raw/lastfm/incremental/<ts_nodash>/page_NNNN.json` — pasta única por execução, identificada pelo instante lógico do run. Devolve o prefixo no XCom. Casos de borda: marca d'água `None` (warehouse vazio) → falha explícita pedindo o backfill; janela vazia (`total = 0`) → `AirflowSkipException` **antes** de gravar, para não escrever objeto vazio.
3. **Transformar → `processed`:** recebe o prefixo pelo XCom, lista os objetos daquela pasta e concatena todas as páginas (tratando a esquisitice do Last.fm de devolver `track` como objeto, não lista, quando há um único resultado). Achata com `pd.json_normalize`, seleciona as 6 colunas de interesse, converte `date.uts`, **descarta o `nowplaying`** (vem sem `date` → `dropna`), deduplica por (`scrobble_uts`, `faixa`) e grava `processed/lastfm/recent.parquet`.
4. **Carregar → `analytics`:** lê o Parquet e itera **linha a linha**: upsert em `dim_artista` (`ON CONFLICT (lower(nome))`), `dim_faixa` (`ON CONFLICT (lower(nome), artista_id)`) e `dim_tempo` (`ON CONFLICT (data, hora)`), cada um com `RETURNING id`; por fim insere em `fato_audicoes` com `ON CONFLICT (scrobble_uts, faixa_id) DO NOTHING`.
5. **Validar:** cinco checagens sobre o que acabou de entrar — todo `scrobble_uts` da prata existe no ouro, nenhuma FK nula, nenhum scrobble no futuro, nenhum artista ou faixa duplicado por caixa. Cada uma deve responder zero; qualquer outra resposta reprova a task, que fica vermelha e dispara o alerta como qualquer outra falha.

**Duas garantias diferentes, e é importante não confundi-las.** A **idempotência** (`UNIQUE` + `ON CONFLICT`) garante que reprocessar não duplica. A **incrementalidade** (marca d'água + paginação) garante que a janela é determinada pelo que falta carregar, não por um número fixo de registros, e que ela cabe inteira em qualquer tamanho — um intervalo longo sem execução entra completo na execução seguinte.

**O que a marca d'água *não* garante.** Ela é `max(scrobble_uts)` — uma marca de **nível máximo**, não um "carreguei tudo contiguamente até aqui". Se um scrobble em `X` ficasse de fora enquanto outro em `Y > X` entrasse, a execução seguinte partiria de `Y+1` e o `X` estaria perdido **para sempre**. A marca protege contra a janela inteira ter falhado, não contra buraco no meio dela.

**Por que buraco no meio não acontece, e de onde vem a garantia de verdade.** Duas propriedades, nenhuma delas a marca d'água:

- **A carga é atômica.** `carregar()` faz um único `commit()` no fim, depois do laço. Ou a janela inteira entra, ou nada entra — não existe estado parcial que deixe um `X` para trás enquanto o `Y` avança a marca. ⚠️ **Isso é estrutural, não detalhe de implementação.** A nota da §9.1 sobre trocar a carga linha a linha por `execute_values` só é segura se o `commit` continuar único; commit por lote ou por página reintroduz estado parcial e a garantia de recuperação cai **em silêncio**.
- **O deslizamento da paginação só produz sobreposição.** O `getRecentTracks` devolve do mais recente para o mais antigo, e scrobbles novos entram pelo topo — então, durante o laço, os itens deslizam para páginas **posteriores**, que ainda não foram lidas. O efeito é ler algo duas vezes (o `ON CONFLICT` descarta), nunca pular. A direção inversa exigiria scrobbles serem **apagados** no Last.fm durante a extração; é possível, e é o único cenário conhecido em que a paginação criaria buraco.

Quem quiser eliminar até esse resíduo pode congelar a janela passando também `to = <instante da extração>`; as fronteiras de página param de se mover, ao custo de deixar para o run seguinte o que for ouvido durante a extração — o que é inofensivo, já que a marca d'água não avança além do que entrou.

**Imutabilidade por camada.** O `raw` é imutável: cada execução escreve numa pasta própria e nunca sobrescreve a anterior. O `processed` é deliberadamente **sobrescrito** — ele é derivado e regenerável, e a marca d'água garante o reprocessamento: se a `transformar` funciona e a `carregar` falha, o `scrobble_uts` não entra no warehouse, a marca não avança, e a execução seguinte volta a buscar exatamente aquela janela. Perder a camada prata não custa nada; perder o bronze custaria uma nova rodada de chamadas à API.

**O caso em que isso deixou de ser teoria (ago/2026).** Os upserts da carga faziam `SET mbid = EXCLUDED.mbid` e `SET album = EXCLUDED.album` — o valor recém-chegado sempre vencia. Como o Last.fm devolve `artist.mbid` vazio em parte dos scrobbles, bastava um scrobble sem mbid para **apagar** o mbid que outro tinha trazido, e a DAG diária repetia isso todo dia. O defeito não dispara nada: o banco continua consistente, sem FK órfã, sem duplicata, sem linha no futuro. Ele não se manifesta como falha, só como ausência — e foi encontrado lendo o SQL, não por alerta.

Na hora de recuperar, as três camadas estavam assim: o `processed` guarda só a última janela, o warehouse é onde o dado se perdeu, e o **bronze tinha tudo**. Varrendo os 346 objetos do `raw` (61.742 scrobbles) e cruzando com a dimensão, 70 artistas tinham no bronze um mbid que o warehouse não tinha mais — recuperados por `scripts/reparar_dimensoes.py`. O álbum, medido do mesmo jeito, não tinha perda alguma: era exposição, não estrago.

Duas conclusões que valem mais que o conserto. **Primeira:** a imutabilidade do bronze não é higiene, é a única cópia de recurso quando a camada de cima erra em silêncio — se o `raw` fosse uma pasta sobrescrita a cada execução, os 70 estariam perdidos. **Segunda:** a regra dos upserts virou uma só, aplicada nos três pontos das duas DAGs — `COALESCE(NULLIF(<existente>, ''), NULLIF(<novo>, ''))`, ou seja, *o primeiro valor não-vazio vence, com preferência pelo que já está no banco*. O `NULLIF` nos **dois** lados é necessário: sem ele no lado novo, o vazio sobrescreve; sem ele no lado existente, um `''` já gravado trava o campo para sempre, porque `''` não é `NULL` e o `COALESCE` o aceita como valor bom. É a mesma regra que o `nome` já seguia — a primeira leitura boa é a que fica.

### Carga histórica — `scripts/backfill.py` (one-off, fora do Airflow)
[`scripts/backfill.py`](../scripts/backfill.py), rodado no host (MinIO em `localhost:9000`, Postgres em `localhost:5433`). É aqui que o medalhão acontece como planejado:

1. **Bronze:** pagina o histórico inteiro via `@attr.totalPages`, com `sleep(0.25s)` entre páginas e retry próprio; grava **uma página por objeto** em `raw/lastfm/backfill/page_NNNN.json` — bronze de fato imutável.
2. **Prata:** mesma limpeza do item 2 acima, mais as colunas de tempo derivadas (`data`, `hora`, `ano`, `mes`, `dia`, `dia_semana`), em `processed/lastfm/backfill.parquet`.
3. **Ouro:** carga em **lote** com `execute_values` (não linha a linha — 60k linhas exigem isso), resolvendo os ids das dimensões via dicionários em memória.

Executado uma vez, em jul/2026: **61.338 scrobbles em 72 meses (ago/2020 →)**. Daí em diante a DAG diária mantém o warehouse em dia — o total corrente é maior e muda todo dia, então não é fixado aqui.

### DAG `pipeline_spotify` (Spotify — semanal)
[`dags/pipeline_spotify.py`](../dags/pipeline_spotify.py). Gatilho `@weekly` (o "top" é computado em janelas de semanas/meses; não faz sentido diário), `catchup=False`, mesmos retries. **Quatro** tasks: `extrair → transformar → carregar → validar` — não há marca d'água aqui, porque o "top" do Spotify é um retrato do momento, não um histórico de eventos: não existe "o que falta buscar", cada coleta pega o estado atual inteiro. OAuth via `spotipy` com `cache_path=/opt/airflow/.cache` e `open_browser=False` — o container não abre navegador, então reutiliza o `refresh_token` do cache ou falha limpo.

1. **Extrair → `raw`:** para cada um dos três `time_range`, chama `/me/top/tracks` e `/me/top/artists` (`limit=50`); mais `/me/tracks`, **paginado** seguindo o campo `next` até esgotar a biblioteca (~1.500 faixas, ~32 páginas). Grava tudo em `raw/spotify/<ts_nodash>/` — pasta por execução, uma página por objeto — e devolve por XCom o prefixo **e a data da coleta**.
2. **Transformar → `processed`:** recebe o prefixo pelo XCom, lê os seis JSONs de top e concatena as páginas da biblioteca, e normaliza em três Parquets — `top_tracks.parquet` (posição via `enumerate`, `time_range`, faixa, artista, álbum, ids), `top_artists.parquet` e `saved_tracks.parquet` (com `added_at`). Só o **primeiro artista** de cada faixa é considerado.
3. **Carregar/enriquecer → `analytics`:** para cada item, faz **upsert** em `dim_artista` e `dim_faixa` (`ON CONFLICT` nos índices por `lower(nome)`, ver §5) e usa o **id devolvido pelo upsert** como FK ao inserir em `fato_top_spotify`, com `ON CONFLICT (snapshot_date, time_range, tipo, posicao) DO NOTHING`. Antes de marcar a biblioteca, **zera `na_biblioteca`** e remarca só o que veio na coleta. Um único `commit` no fim, então ninguém enxerga o estado zerado.
4. **Validar:** quatro checagens — o número de linhas do top na prata bate com o que entrou no snapshot do ouro, nenhuma FK nula, nenhuma dimensão duplicada por caixa. A primeira é a que teria pego a falha do `UPDATE` puro no dia em que ela nasceu: o Parquet tinha 300 linhas e o snapshot recebia 276.

**Por que upsert e não `UPDATE`.** A versão anterior fazia `UPDATE … WHERE nome = %s`: alterava quem já existia e **descartava em silêncio** quem não existia — ou seja, todo artista ou faixa que aparece no top do Spotify mas nunca foi scrobblado no Last.fm. Pior, a linha correspondente na fato era gravada mesmo assim, com FK **nula**, porque o id vinha de um subselect que não casava. Com o upsert, o item entra na dimensão e a FK vem do id retornado — a linha órfã deixa de ser possível por construção, sem precisar de um `CHECK` para barrá-la depois.

**Por que a data da coleta vem da extração, e não de `date.today()` na carga.** O `snapshot_date` responde "quando esta foto foi tirada", e quem tira a foto é a `extrair`. Carimbar na carga funcionaria enquanto as duas rodassem na mesma janela, mas reexecutar só o `carregar` dias depois releria o Parquet da coleta antiga e gravaria a data de hoje. Passando o instante por XCom, a data viaja junto com o dado.

**Por que não a data lógica do Airflow.** Seria o reflexo natural — `logical_date`/`data_interval_start` é o idiomático em DAGs — mas aqui estaria errado. O `/me/top` do Spotify **não viaja no tempo**: não há parâmetro para pedir o top de uma data passada, a API sempre devolve o que está calculado agora. Reprocessar a execução de 15/07 traz o top de hoje; carimbá-lo como 15/07 seria gravar uma afirmação que a fonte não sustenta. Como a DAG usa `catchup=False`, o Airflow nunca refaz execução antiga sozinho — toda "execução passada" é reprocessamento manual, e nele a API responde com dados de agora, sem exceção. **Não trocar por `logical_date`.**

E esse dado tem valor próprio: *"faixas que o Spotify diz que eu mais ouço e que nunca apareceram no meu histórico do Last.fm"* é uma resposta analítica, não um erro — e alimenta o cruzamento entre fontes.

### Falhas e reexecução
`retries=2`, `retry_delay=1min` nas duas DAGs; falha persistente deixa a task vermelha na UI. As etapas são **idempotentes** (`ON CONFLICT DO NOTHING`/`DO UPDATE`), então reexecutar é seguro — não gera duplicata. E, no Last.fm, reexecutar também **recupera**: a carga é atômica (um `commit` só), então uma falha não deixa nada pela metade nem faz a marca d'água avançar, e a janela volta inteira na execução seguinte, de qualquer tamanho. Uma exceção declarada: janela vazia é `skipped`, não `success` — reexecutar não ajudaria, e tratar como falha dispararia retries inúteis.

---

## 7. Requisitos

Legenda: ✅ atendido · ⚠️ atendido parcialmente · ⏳ não iniciado.

### Funcionais

| | Requisito | Status |
|---|---|---|
| **RF1** | Ingerir scrobbles do Last.fm incrementalmente (janela diária) | ✅ marca d'água (`max(scrobble_uts)`) como `from`, com paginação |
| **RF2** | Suportar carga histórica completa do Last.fm (backfill) | ✅ `scripts/backfill.py`; 6 anos de histórico carregados |
| **RF3** | Ingerir do Spotify (OAuth): top tracks/artists (3 time_ranges) e biblioteca salva | ✅ tops completos; biblioteca paginada por inteiro (~1.500 faixas) |
| **RF4** | Persistir dado cru (`raw`) e tratado em Parquet (`processed`) para ambas as fontes | ✅ `raw` imutável por execução nas duas esteiras; `processed` sobrescrito de propósito (§6) |
| **RF5** | Carregar esquema constelação: `fato_audicoes`, `fato_top_spotify` e dimensões compartilhadas | ✅ `db/schema.sql`, ambas as fatos populadas |
| **RF6** | Enriquecer dimensões com ids e biblioteca do Spotify | ✅ upsert: o que só existe no Spotify entra na dimensão em vez de ser descartado |
| **RF7** | Garantir idempotência (reprocessar não duplica) | ✅ `UNIQUE` + `ON CONFLICT` em todas as tabelas |
| **RF8** | Validações de qualidade entre etapas | ✅ task `validar` no fim de cada DAG; reprova → task vermelha → alerta |

### Não-funcionais

| | Requisito | Status |
|---|---|---|
| **RNF1** | Orquestração observável (UI, logs, retries, alertas) via Airflow; duas DAGs | ✅ UI, logs, `retries=2` e aviso por webhook (`on_failure_callback`, só após esgotar os retries) |
| **RNF2** | Tudo containerizado (`docker-compose`), reproduzível localmente | ✅ Airflow, MinIO, Postgres e Redis no compose; o `backfill.py` roda no host **ou** no container, sem editar código — os endereços vêm de variável de ambiente |
| **RNF3** | Credenciais fora do código (`.env`); **nunca** commitadas | ✅ `.env` no `.gitignore`, `.env.example` versionado |
| **RNF4** | Token OAuth do Spotify com cache + refresh automático | ✅ `.cache` montado no container, `open_browser=False` |
| **RNF5** | Storage desacoplado de compute (MinIO ↔ Postgres) | ✅ |
| **RNF6** | Código versionado no GitHub desde o dia 1 | ✅ |

---

## 8. Critérios de sucesso (definição de pronto)

- [x] DAG `pipeline_audicoes` **verde** de ponta a ponta; histórico do Last.fm carregado.
- [x] DAG `pipeline_spotify` **verde**; dimensões enriquecidas e `fato_top_spotify` populado.
- [x] Consultas analíticas respondendo, incluindo o cruzamento **Last.fm × Spotify** (mais-tocado vs top, e marcação "está na biblioteca").
- [x] Falha provocada → Airflow tenta de novo e alerta; nada de lixo no warehouse.
      → verificado por injeção de falha: 2 retries e, só depois de esgotá-los, aviso por webhook.
- [x] Repositório com README, diagrama, este PRD, `docker-compose.yaml`, código das DAGs.
- [x] Post de portfólio (problema → solução → decisões → aprendizados). — dois posts publicados.

---

## 9. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| **Spotify Premium obrigatório** | premissa declarada; sem Premium, o escopo Spotify é removido e o projeto segue só com Last.fm |
| Complexidade do OAuth do Spotify | usar `spotipy` (cuida de fluxo, cache e refresh); scopes mínimos |
| **Volatilidade da API do Spotify** (mudanças nov/2024 e fev/2026) | usar só endpoints de personalização do usuário; não depender de popularidade/recommendations; **versões fixadas** no `requirements.txt` e no compose (conjuntos diferentes — §9.1) |
| Limite informal do Last.fm | paginação educada, poucas req/s |
| Extração não-incremental (gap maior que 200 scrobbles não entra no carregamento automático) | **resolvido** — marca d'água + paginação (§6); qualquer tamanho de intervalo entra na execução seguinte |
| Chaves/segredos expostos | `.env` + `.gitignore` |
| Histórico do Spotify incompleto pela API | aceitar (só ~50 recentes); histórico é responsabilidade do Last.fm |
| Casamento de faixas entre fontes | resolver por nome (chave de negócio); ids como apoio |
| Subir o Airflow (parte sensível) | `docker-compose` fixado em versão; fallback no compose oficial |

### 9.1 Limitações conhecidas

Esta seção começou na revisão v0.3 como uma lista de **dívidas** — oito divergências
entre o que este documento afirmava e o que o código fazia (`D1`–`D8`), levantadas
comparando um com o outro. Todas foram fechadas: sete por código, e a última
(`date.today()` no `snapshot_date`) reclassificada como decisão deliberada, com o
motivo registrado na §6.

O que resta abaixo não é dívida — é o que este projeto **não faz**, por escolha ou por
limite da fonte. Não há plano de ação associado.

**Por decisão, registrada com o motivo:**
- **Sufixos de versão não são normalizados** (`- 2014 Remaster`, `- Live At …`). São ~184 faixas. Ver §5.
- **A camada `processed` é sobrescrita**, não versionada por execução. Ver §6.
- **O `snapshot_date` do Spotify é a data da coleta**, não a data lógica da execução. Ver §6.

**Imperfeições aceitas:**
- **O host e o Airflow rodam versões diferentes das mesmas bibliotecas.** Ambos estão fixados (`requirements.txt` e `_PIP_ADDITIONAL_REQUIREMENTS` no compose), mas em conjuntos distintos: a imagem do Airflow 2.10.5 traz constraints próprias e resolve para versões mais antigas — no pandas, **3.0 no host contra 2.1 no container**, uma *major* de diferença. Na prática, código testado à mão pode se comportar diferente dentro da DAG. Alinhar exigiria ou forçar o pandas 3 na imagem (que o Airflow 2.10 não suporta) ou rebaixar o host a uma versão sem wheel para Python 3.14 — nenhuma das duas é barata, então a divergência fica registrada em vez de resolvida.
- A carga da DAG do Last.fm é **linha a linha** — aceitável para as centenas de linhas de uma janela diária, não para lotes grandes; por isso o `backfill.py` usa `execute_values`. ⚠️ Se converter, manter o **`commit` único** no fim: a atomicidade é o que garante a recuperação (§6).
- O casamento entre fontes não resolve **grafias alternativas nem alfabetos diferentes** — `The Neighbourhood`/`The Neighborhood` e `Girls' Generation`/`소녀시대` são linhas separadas. Resolver exigiria usar `mbid`/`spotify_artist_id` como chave alternativa.
- O `transformar` do Spotify considera apenas o **primeiro artista** de cada faixa — faixas colaborativas perdem os demais.
- **As validações não pegam sobrescrita silenciosa de atributo.** Elas perguntam sobre o estado do warehouse (FK órfã, duplicata por caixa, linha da prata ausente no ouro), e um valor bom trocado por vazio deixa o estado perfeitamente consistente — foi assim que o bug do `mbid` (§6) passou. "Este campo já teve valor?" é uma pergunta que só o bronze responde, comparando as camadas; nenhuma consulta pós-carga a alcança.
- **Os testes cobrem a transformação, não o SQL.** `dags/transformacoes.py` foi separado do I/O justamente para ser testável sem infraestrutura, e `tests/` cobre as armadilhas da API (`track` como objeto, `nowplaying` sem data, deduplicação). O que **não** existe é teste dos upserts: pegá-los exigiria um Postgres real, porque a semântica sob teste é a do `ON CONFLICT` com índice por `lower(nome)` — não há mock que a reproduza. É o teste que teria pego o bug do `mbid`, e a decisão foi não pagar as ~40 linhas de infraestrutura por ele: o conserto está registrado na §6, no `CLAUDE.md` e num comentário na própria linha.

---

## 10. Roadmap (alinhado às fases do guia)

| Fase | Entrega | Status |
|---|---|---|
| 1 — Python | extração Last.fm funcionando, `raw` no MinIO | ✅ |
| 2 — SQL/Postgres | esquema criado; primeira query analítica ("artista mais ouvido por mês") | ✅ |
| 3 — Docker/MinIO | `docker-compose` subindo o lake | ✅ |
| 4 — Airflow | `transform`/`load` + DAG `pipeline_audicoes` verde | ✅ |
| 4b — Spotify | app + OAuth; DAG `pipeline_spotify` (top + biblioteca) enriquecendo as dimensões; cruzamento entre as fontes | ✅ |
| 5 — Qualidade | tasks de validação | ✅ |
| 6 — Portfólio | README, diagrama, este PRD, post | ✅ |

**Trilha paralela — as dívidas da §9.1: concluída.** Não fazia parte das fases do guia e
não tinha entrega de portfólio associada; era manutenção. As oito divergências levantadas
na revisão v0.3 foram fechadas, e o que restou na §9.1 são limitações declaradas, não
pendências. Duas migrações versionadas ficaram do processo, em `db/migracoes/`.

---

## 11. Considerações de engenharia da ingestão (Cap. 7, aplicado ao projeto)

> As perguntas-guia de ingestão de *Fundamentos de Engenharia de Dados* (Cap. 7, lido na Fase 4), respondidas para este projeto. Consolidam, sob a ótica da ingestão, decisões que aparecem espalhadas nas seções 4 (fontes), 6 (fluxo) e 9 (riscos).

**1. Quais são os casos de uso? Dá para reutilizar os dados em vez de versioná-los?**
Casos de uso: análise do histórico (artista/faixa mais ouvidos, padrões por hora/dia/mês, evolução do gosto) e o cruzamento Last.fm × Spotify. A reutilização vem do modelo medalhão: há uma origem (`raw`) e camadas derivadas a partir dela, não N cópias do mesmo dado.

Isso vale nas duas esteiras: o `backfill.py` (uma página por objeto em `lastfm/backfill/`), a DAG diária (`lastfm/incremental/<ts_nodash>/`) e a semanal do Spotify (`spotify/<ts_nodash>/`) gravam pasta por execução e nunca sobrescrevem. Qualquer análise nova reprocessa esses objetos sem rechamar a API.

No Spotify isso importa por um motivo próprio: cada coleta é um **retrato** do gosto naquela semana, e a fonte não permite pedir o retrato de uma semana passada. Sobrescrever apagaria o único registro que existe dele.

A camada `processed` é a exceção deliberada — derivada, regenerável, e a marca d'água garante o reprocessamento (§6).

**2. A fonte gera/coleta de forma confiável? O dado está disponível quando preciso?**
O Last.fm é estável. O cuidado é o limite informal dos ToS (~5 req/s por IP); a mitigação é `sleep` entre páginas e os `retries` da task no Airflow (§6). Em lote diário isso não é gargalo. Aqui "disponível quando preciso" depende mais do **Airflow estar no ar** do que da API.

Com `raw` imutável + etapas idempotentes + marca d'água, reexecutar é seguro nos dois sentidos: não duplica e não deixa buraco — o que falhou volta na execução seguinte. Vale para as duas esteiras: o `raw` do Spotify também grava pasta por execução (`spotify/<ts_nodash>/`), então reexecutar não apaga o retrato anterior.

**3. Qual o destino dos dados após a ingestão?**
Destino imediato da ingestão: **MinIO, bucket `raw`** (JSON cru, bronze). Destino final: **Postgres analítico** (`fato_audicoes` + dimensões, ouro), passando por `processed` (Parquet, prata). A task `extrair` da DAG se ocupa **apenas** do `raw` — ingestão não se mistura com transformação.

**4. Com que frequência preciso acessar os dados?**
Distinguir duas frequências: **ingestão** (DAG `@daily`) e **consulta** (warehouse, ad hoc). Após a carga histórica, o parâmetro `from` puxa só o que é novo desde a última execução — **carga incremental**, evitando rebaixar a API inteira a cada run.

O "desde a última execução" é o ponto delicado, e a implementação merece registro: a pipeline não confia no calendário do agendador para saber o que falta. Ela **pergunta ao warehouse** (`max(scrobble_uts)`) no início de cada run e usa a resposta como `from`. A diferença importa porque o calendário mente quando a execução falha, atrasa ou é disparada à mão, enquanto `max(scrobble_uts)` é sempre a verdade sobre o que está carregado. Combinado com a paginação, isso torna o tamanho do intervalo irrelevante: um dia ou um mês sem rodar entram inteiros na execução seguinte.

**5. Qual o volume esperado?**
Pequeno: cada scrobble são poucos bytes; o histórico todo fica na casa de milhares a dezenas de milhares de linhas. O que dimensiona a carga histórica é a paginação (`limit` máx. 200 → todo o histórico em poucas dezenas de chamadas). **Não há cenário de big data** — pandas + Parquet bastam; nada de Spark ou particionamento complexo.

**6. Em que formato vêm os dados? O downstream lida com ele?**
A API devolve **JSON** (`format=json`); a task `transformar` da DAG converte para **Parquet** na camada `processed`. JSON é bom para ingestão (é o que a API fala, preserva tudo) e ruim para análise (verboso, sem tipos); Parquet — colunar, tipado, comprimido — alimenta o Postgres sem dor. Cada formato no seu trecho da jornada.

**7. A origem está pronta para uso imediato? Por quanto tempo vale e o que a inutiliza?**
Não totalmente: o JSON cru exige tratamento — descartar `nowplaying` (vem sem `date`), lidar com `mbid`/`album` vazios e achatar a estrutura aninhada. Quanto à validade: um scrobble de 2020 não muda, então o dado vale como registro permanente e a idade não o inutiliza. O que inutiliza é **duplicação** (incremento mal feito) ou **schema drift** (a API mudar um campo). Defesa: deduplicação na transformação e a task `validar` no fim de cada DAG (Fase 5, concluída — RF8, §6).

**Ressalva:** "imutável" vale para o `raw` das duas esteiras, não para o `processed`, que é derivado e sobrescrito de propósito — ver resposta 1 e §6.

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
