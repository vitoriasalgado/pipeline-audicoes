# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que este repositório é (leia primeiro)

Este NÃO é um projeto de produção. É o **primeiro projeto de engenharia de dados de uma iniciante**, construído como aprendizado e portfólio, missão a missão. O trabalho é guiado por dois documentos que são a fonte da verdade:

- `docs/PRD_Pipeline_Audicoes.md` — escopo completo, fontes/APIs, modelo de dados, DAGs, riscos. Da **v0.3** em diante é um documento *as-built*: descreve o que o código **faz**, não o que se pretendia fazer, e a **§9.1** lista as limitações conhecidas — o que ele não faz, por escolha ou por limite da fonte. As oito dívidas levantadas na v0.3 foram todas fechadas; se aparecer uma nova, registrar ali com o encaminhamento e remover ao quitar.
- `docs/roteiro_post.md` — passo a passo (**Missão 0 → 18**), com o código de referência de cada etapa e um bloco "✅ Você deve saber explicar" por missão. **Local/gitignored** (não versionado): existe só na máquina da usuária, não no repo público.

### Como atuar aqui: mentor, não implementador

A usuária pediu explicitamente que Claude aja como **mentor**. Isso muda o modo de operar em relação ao normal:

- **NÃO escreva o código das missões por ela.** As "regras de ouro" do roteiro dizem que ela digita, roda, quebra e entende — é isso que gera aprendizado. O papel do Claude é explicar o *porquê*, revisar o que ela escreveu, e ajudar a debugar.
- **Uma missão por vez.** Não pule para frente nem adiante código de missões futuras. Só avance quando a missão atual fechar numa vitória visível na tela.
- **Ao fim de cada missão**, confira o bloco "✅ Você deve saber explicar" do roteiro fazendo perguntas para confirmar entendimento antes de seguir.
- **Commit ao fim de cada missão.** Push para `origin/main`. Mensagens de commit em **inglês, no padrão Conventional Commits** (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`…). Sem co-autor Claude (já desligado via `attribution` no settings).
- **Ao fim de cada missão, revisar o PRD e o README** — e ajustar os dois se a missão mudou algo que eles afirmam. Não é burocracia: o PRD e o README descrevem o mesmo sistema para públicos diferentes (o PRD é o projeto por dentro, o README é a vitrine), e quando um dos dois fica velho eles passam a **divergir entre si e do código**. Foi exatamente isso que aconteceu até a v0.3 do PRD: ele descrevia a DAG usando `from`/`to` e paginação, que nunca existiram, enquanto o README chamava a ingestão de "incremental" — duas afirmações falsas convivendo por várias missões. Documento que mente é pior que documento nenhum, porque decisão futura se apoia nele. Perguntar a cada fechamento: *a missão mudou o fluxo, o modelo de dados, o que roda ou o que é verdade sobre o projeto?* Se sim, o ajuste faz parte da missão, não é tarefa separada.
- Tom acolhedor de iniciante; normalize a dificuldade (o Airflow, na Missão 6, é onde todo mundo apanha).

Exceção: refatorações mecânicas, configuração de ambiente, depuração e revisão são bem-vindas — a restrição é sobre não entregar pronto o código pedagógico das missões.

## Estado atual

O progresso vive numa **fonte única de verdade: o checklist "Roteiro" do `README.md`** — marque `[x]` ao fechar cada missão. NÃO rastreie o progresso missão-a-missão aqui. Este arquivo guarda só o que é estável — arquitetura, princípios, regras de mentoria.


Repo público: https://github.com/vitoriasalgado/pipeline-audicoes

Documentos-fonte das missões: `docs/roteiro_post.md` (passo a passo Missão 0→18, **local/gitignored**) e `docs/rascunho_post.md` (caderno de síntese do post, preenchido a cada missão).

Trabalho que **não** é missão (conserto de dívida técnica, ajuste de documentação) não entra no checklist do README nem no `rascunho_post.md` — a trilha de dívidas vive na §9.1 do PRD e é manutenção, não portfólio.

## Arquitetura (o big picture)

Pipeline medalhão de ponta a ponta do histórico musical:

```
Last.fm API ──► raw/ (JSON, bronze) ──► processed/ (Parquet, prata) ──► PostgreSQL analytics (ouro)
   Spotify API (fase 2, opcional, OAuth) ─── enriquece as dimensões ───┘
        MinIO = data lake · Apache Airflow orquestra tudo
```

Princípios que atravessam todo o código e devem ser respeitados nas decisões:

- **Camadas medalhão separadas por responsabilidade:** a extração só escreve o `raw` (JSON cru) e NÃO se mistura com transformação; a transformação lê `raw` e escreve Parquet em `processed`; a carga modela e carrega o Postgres. A lógica de cada etapa vive **inline dentro das DAGs** — elas não importam nada de `lastfm/` nem de `spotify/`. **O que roda no Airflow é sempre o código em `dags/`.** As pastas por fonte guardam scripts de host (rodados à mão, com `localhost`) e o registro de como cada etapa foi construída; quando um deles deixa de refletir a DAG, vai para `arquivo/` em vez de ficar dando a impressão de ser a etapa atual.
- **`raw` é imutável e é a fonte única** — nas duas esteiras, uma pasta por execução: `lastfm/incremental/<ts_nodash>/page_NNNN.json` e `spotify/<ts_nodash>/`. No Spotify isso é ainda mais crítico: cada coleta é o retrato do gosto naquela semana, e a API não devolve retrato passado. A camada `processed` é exceção deliberada — derivada, regenerável, e a marca d'água garante o reprocessamento.
- **Idempotência e carga incremental** são as duas decisões que definem a corretude do núcleo, e **as duas estão implementadas** no Last.fm. Idempotência: filtrar `nowplaying` (vem sem `date`) e `UNIQUE (scrobble_uts, faixa_id)` com `ON CONFLICT DO NOTHING`. Incrementalidade: a task `descobrir_marca_dagua` lê `max(scrobble_uts)` da `fato_audicoes` e passa por XCom para a `extrair`, que usa como `from` (inclusivo, daí o `+1`) e **pagina** por `@attr.totalPages`. A marca d'água vem do warehouse de propósito — é derivada do dado, então nunca dessincroniza; não trocar por Airflow Variable ou arquivo de controle.
- **Cada DAG termina numa task `validar`** (`dags/validacoes.py`): um punhado de perguntas que devem responder zero — FK nula, dimensão duplicada por caixa, linha da prata que não chegou ao ouro, scrobble no futuro. Reprovou, task vermelha, e o alerta dispara pelo mesmo caminho de qualquer falha. As perguntas saem de defeitos que o projeto **já teve**; ao consertar um bug novo, considerar acrescentar a checagem que o teria pego.
- **Alerta de falha** em `dags/alertas.py`, ligado por `on_failure_callback` nas `default_args` das duas DAGs. Dispara só **depois** de esgotar os retries — falha que a segunda tentativa resolve não avisa. Sem `ALERTA_WEBHOOK_URL` no ambiente, não avisa e não quebra.
- **Casos de borda da extração incremental** já tratados, não reintroduzir: janela vazia → `AirflowSkipException` **antes** de gravar no MinIO (senão sobrescreve com vazio); `max()` de tabela vazia → `None`, falha explícita pedindo o backfill; um único resultado → o Last.fm devolve `track` como objeto, não lista.
- **Modelo estrela/constelação:** duas fatos (`fato_audicoes` do Last.fm, `fato_top_spotify`) compartilhando `dim_artista`, `dim_faixa`, `dim_tempo`. `spotify_*_id` e `mbid` são atributos de apoio e podem vir vazios.
- **A chave de negócio é o nome, comparado sem distinção de caixa.** Garantido por índice: `unique (lower(nome))` em `dim_artista`, `unique (lower(nome), artista_id)` em `dim_faixa`. **Os quatro upserts das duas DAGs usam esses índices no `ON CONFLICT`** — trocar por `ON CONFLICT (nome)` reintroduz linha fantasma (`Zayn` e `ZAYN` viram dois artistas, o enriquecimento cai no que tem zero execuções) e passa a estourar violação de índice. O `DO UPDATE` nunca altera a coluna `nome`: a primeira grafia vista é a que fica.
- **Não normalizar sufixos de versão** (`- 2014 Remaster`, `- Live At …`). São ~184 faixas, e há títulos legítimos com hífen que uma regra ingênua corromperia. Decisão registrada no PRD §5.
- **`localhost` vs nome de serviço:** dentro da rede do Docker a infra atende por nome (`minio:9000`, `warehouse:5432`); do host, por `localhost` (`9000`, `5433`). Confundir é um erro clássico. As DAGs usam os nomes de serviço direto, porque só rodam em container. O `scripts/backfill.py`, que roda nos dois lugares, **não escolhe**: lê `MINIO_ENDPOINT`, `WAREHOUSE_HOST` e `WAREHOUSE_PORT` do ambiente, com os valores do host como padrão, e o compose sobrescreve com os nomes de serviço. Script novo que precise rodar nos dois lugares deve seguir esse padrão em vez de fixar um lado.

## Stack e anti-padrões

Python **3.14.6** (venv local) · Airflow **2.10.5** (Python 3.12 na imagem) · PostgreSQL **13** · MinIO. Bibliotecas: `requests`, `boto3`, `pandas`, `pyarrow`, `psycopg2-binary`, `spotipy`, `python-dotenv`.

⚠️ **Dois ambientes, versões diferentes.** O host usa o `requirements.txt`; o Airflow usa o `_PIP_ADDITIONAL_REQUIREMENTS` do compose, preso às constraints da imagem — **pandas 3.0 no host, 2.1 no container**. Os dois estão fixados, mas não são o mesmo conjunto: ao depurar diferença de comportamento entre script e DAG, checar isso antes de procurar bug no código.

**Não introduzir** Spark, dbt, ORM (SQLAlchemy), Kafka ou particionamento complexo. O volume é de milhares a dezenas de milhares de linhas: **pandas + Parquet + SQL puro bastam**, e a escolha é deliberada (PRD §11, resposta 5). SQL é escrito à mão com `psycopg2` — faz parte do aprendizado. Sugerir ferramenta nova só se a usuária pedir.

## Convenções de código

- **Código em português, commits em inglês.** Funções, variáveis, arquivos e colunas usam português em `snake_case` (`extrair_para_minio`, `transformar`, `carregar`, `faixa`, `artista`, `scrobble_uts`, `dim_tempo`). Mensagens de commit seguem Conventional Commits **em inglês**. Não "corrigir" nomes para inglês.
- **`print()` é a observabilidade do projeto** — não há logger configurado. Nos scripts rodados no host, usar `print(..., flush=True)`: o import do pandas na 3.14 é lento e um script silencioso parece travado.
- `# type: ignore` nos imports do Airflow (os pacotes só existem dentro do container, o Pylance reclama no host).

## Comandos

Ambiente (macOS/Linux; no Windows use `.venv\Scripts\activate`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencha LASTFM_API_KEY e LASTFM_USER
```

Rodar um script de host (ex.):

```bash
python lastfm/ler_parquet.py
```

**Todo `.py` fora de `dags/` e `arquivo/` roda no host.** Os de `lastfm/` e `spotify/` fixam `localhost`; o `scripts/backfill.py` lê do ambiente (`MINIO_ENDPOINT`, `WAREHOUSE_HOST`, `WAREHOUSE_PORT`, `WAREHOUSE_DB`) e roda nos dois lugares.

Estrutura das pastas: `dags/` (as **duas** DAGs — `pipeline_audicoes` e `pipeline_spotify`; o compose monta essa pasta, e é o que roda), `lastfm/` e `spotify/` (um utilitário de host cada: `ler_parquet.py` e `test_spotify.py`), `db/` (`schema.sql` + `migracoes/` + `consultas/`, as queries analíticas das missões 12 e 18), `scripts/` (backfill; também montada nos containers), `arquivo/` (código aposentado mantido como registro — as primeiras missões e as versões de etapa que a DAG deixou para trás).

**Critério de aposentadoria:** script que deixa de refletir o que a DAG faz vai para `arquivo/`. Já aconteceu com as duas cargas e as duas etapas do Spotify, que ficaram com `ON CONFLICT (nome)` e `UPDATE ... WHERE nome` depois que as DAGs passaram a usar `lower(nome)` e upsert.

O `backfill.py` roda nos dois lugares sem edição: `python scripts/backfill.py` no host, ou `docker compose exec airflow-worker python /opt/airflow/scripts/backfill.py` no container. Scripts sempre rodados a partir da **raiz** do projeto (ex.: `python spotify/extrair_spotify.py`), pra os caminhos relativos e o `.cache` do spotipy resolverem certo.

`db/schema.sql` descreve o **estado final** e é aplicado pelo compose só na criação do volume — **editar o arquivo não altera tabelas que já existem**. Toda mudança de schema em base povoada precisa de um par: o `schema.sql` atualizado (para clone novo) **e** um script em `db/migracoes/` (para as bases que já existem), rodado à mão no host. Sem o segundo, o clone novo funciona e o warehouse de verdade quebra — foi o que quase aconteceu com os índices por `lower(nome)` (migração `001`).

Infraestrutura (`docker-compose.yaml` — Airflow, MinIO, Postgres de metadados, warehouse e Redis):

```bash
docker compose up airflow-init   # só na 1ª vez: inicializa o banco de metadados
docker compose up -d             # sobe tudo
```

- MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`), buckets `raw` e `processed`.
- Airflow UI: http://localhost:8080 (`airflow` / `airflow`).
- Warehouse (camada ouro): `localhost:5433`, banco/usuário/senha `warehouse`. Dentro dos containers o host é `warehouse:5432`.

Não há suíte de testes nem linter configurados — a "verificação" de cada missão é o checkpoint visível descrito no roteiro (saída no terminal, arquivo no bucket, task verde no Airflow).

## Mudanças que exigem cuidado

O warehouse tem **~62 mil scrobbles** (ago/2020 →, e crescendo a cada execução diária) que não são reproduzíveis rapidamente. Não fixar o número exato em documento: ele muda todo dia. Antes de qualquer uma destas, confirmar com a usuária:

- **Nunca descomentar os `DROP TABLE` do `db/schema.sql`.** Apagam o warehouse inteiro.
- **Schema já povoado muda com `ALTER TABLE`**, não editando o `schema.sql` (que é `IF NOT EXISTS` e ignora tabelas existentes).
- **`date.today()` no `snapshot_date` do Spotify é deliberado — não trocar por `logical_date`.** Parece antipattern de Airflow e não é: o `/me/top` não viaja no tempo, sempre devolve o top calculado agora. Carimbar a data lógica de uma execução reprocessada afirmaria algo que a fonte não sustenta. A data é capturada na `extrair` e viaja por XCom até a carga, para não depender de as duas rodarem no mesmo dia. Motivo completo na §6 do PRD.
- **`dim_faixa.na_biblioteca` é `NOT NULL DEFAULT FALSE` e reflete a última coleta.** A carga **zera a coluna** e remarca só o que veio do Spotify — é o que faz faixa removida da biblioteca voltar a `FALSE`. Esse zerar só é seguro porque a biblioteca é coletada inteira (paginada); com coleta truncada, ele apagaria a marca do que ficou de fora.
- **O `commit()` único no fim de `carregar()` é estrutural.** É a atomicidade dele que garante a recuperação: falha no meio → nada entra → a marca d'água não avança → a janela volta inteira. A marca d'água é `max(scrobble_uts)`, **nível máximo**, não "contíguo até aqui" — ela não protege contra buraco no meio da janela. Se converter a carga para `execute_values` (nota da §9.1), **manter um commit só**; commit por lote reintroduz estado parcial e a garantia cai em silêncio.
- **`scripts/backfill.py` não é comando casual:** ~305 chamadas à API do Last.fm e dezenas de minutos.
- **Não reescrever histórico do git.** Já houve um episódio de `filter-branch` que deixou um contribuidor fantasma no painel do GitHub — irreversível pelo lado do repo.
- Nunca commitar `.env` nem o `.cache` do spotipy (token OAuth).

## Segredos

`LASTFM_API_KEY` e (fase 2) as credenciais OAuth do Spotify vivem só no `.env`, que está no `.gitignore`. `.env.example` é o molde versionado. Nunca commite valores reais; se uma chave vazar no histórico, oriente a gerar uma nova.