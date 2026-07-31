# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que este repositório é (leia primeiro)

Este NÃO é um projeto de produção. É o **primeiro projeto de engenharia de dados de uma iniciante**, construído como aprendizado e portfólio, missão a missão. O trabalho é guiado por dois documentos que são a fonte da verdade:

- `docs/PRD_Pipeline_Audicoes.md` — escopo completo, fontes/APIs, modelo de dados, DAGs, riscos. Da **v0.3** em diante é um documento *as-built*: descreve o que o código **faz**, não o que se pretendia fazer, e a **§9.1** lista as dívidas conhecidas — o que ele ainda não faz. Ao consertar uma dívida, atualizar o verbete dela.
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
- **`raw` é imutável e é a fonte única** — vale para o Last.fm: o `backfill.py` grava uma página por objeto, e a DAG diária grava uma pasta por execução (`lastfm/incremental/<ts_nodash>/page_NNNN.json`). A camada `processed` é exceção deliberada: é derivada, regenerável, e a marca d'água garante o reprocessamento. **Ainda não vale para o Spotify**, que usa chaves fixas — dívida **D2** (§9.1 do PRD).
- **Idempotência e carga incremental** são as duas decisões que definem a corretude do núcleo, e **as duas estão implementadas** no Last.fm. Idempotência: filtrar `nowplaying` (vem sem `date`) e `UNIQUE (scrobble_uts, faixa_id)` com `ON CONFLICT DO NOTHING`. Incrementalidade: a task `descobrir_marca_dagua` lê `max(scrobble_uts)` da `fato_audicoes` e passa por XCom para a `extrair`, que usa como `from` (inclusivo, daí o `+1`) e **pagina** por `@attr.totalPages`. A marca d'água vem do warehouse de propósito — é derivada do dado, então nunca dessincroniza; não trocar por Airflow Variable ou arquivo de controle.
- **Casos de borda da extração incremental** já tratados, não reintroduzir: janela vazia → `AirflowSkipException` **antes** de gravar no MinIO (senão sobrescreve com vazio); `max()` de tabela vazia → `None`, falha explícita pedindo o backfill; um único resultado → o Last.fm devolve `track` como objeto, não lista.
- **Modelo estrela/constelação:** duas fatos (`fato_audicoes` do Last.fm, `fato_top_spotify`) compartilhando `dim_artista`, `dim_faixa`, `dim_tempo`. `spotify_*_id` e `mbid` são atributos de apoio e podem vir vazios.
- **A chave de negócio é o nome, comparado sem distinção de caixa.** Garantido por índice: `unique (lower(nome))` em `dim_artista`, `unique (lower(nome), artista_id)` em `dim_faixa`. **Os quatro upserts das duas DAGs usam esses índices no `ON CONFLICT`** — trocar por `ON CONFLICT (nome)` reintroduz linha fantasma (`Zayn` e `ZAYN` viram dois artistas, o enriquecimento cai no que tem zero execuções) e passa a estourar violação de índice. O `DO UPDATE` nunca altera a coluna `nome`: a primeira grafia vista é a que fica.
- **Não normalizar sufixos de versão** (`- 2014 Remaster`, `- Live At …`). São ~184 faixas, e há títulos legítimos com hífen que uma regra ingênua corromperia. Decisão registrada no PRD §5.
- **`localhost` vs nome de serviço:** scripts rodados no host falam com o MinIO em `http://localhost:9000`; código que roda dentro de container (Airflow) usa `http://minio:9000`. Confundir isso é um erro clássico.

## Stack e anti-padrões

Python **3.14.6** (venv local) · Airflow **2.10.5** · PostgreSQL **13** · MinIO. Bibliotecas: `requests`, `boto3`, `pandas`, `pyarrow`, `psycopg2-binary`, `spotipy`, `python-dotenv` (sem pin de versão — ver §9.1 do PRD).

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

**Todo `.py` fora de `dags/` e `arquivo/` roda no host**, com `localhost`. As versões moldadas para container (com `minio:9000`) foram para `arquivo/` — se um script em `lastfm/` ou `spotify/` voltar a usar nome de serviço, é bug, não decisão.

Estrutura das pastas: `dags/` (as **duas** DAGs — `pipeline_audicoes` e `pipeline_spotify`; o compose monta essa pasta, e é o que roda), `lastfm/` e `spotify/` (scripts de host por fonte), `db/` (schema.sql), `scripts/` (backfill), `arquivo/` (código aposentado mantido como registro — as primeiras missões e as versões de etapa que a DAG deixou para trás). Scripts sempre rodados a partir da **raiz** do projeto (ex.: `python spotify/extrair_spotify.py`), pra os caminhos relativos e o `.cache` do spotipy resolverem certo.

`db/schema.sql` usa `CREATE TABLE IF NOT EXISTS` e é seguro de rodar de novo — mas por isso mesmo **editar o arquivo não altera tabelas que já existem**. Mudança de schema num warehouse já povoado (61 mil linhas) exige `ALTER TABLE`; manter os dois coerentes.

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
- **O `commit()` único no fim de `carregar()` é estrutural.** É a atomicidade dele que garante a recuperação: falha no meio → nada entra → a marca d'água não avança → a janela volta inteira. A marca d'água é `max(scrobble_uts)`, **nível máximo**, não "contíguo até aqui" — ela não protege contra buraco no meio da janela. Se converter a carga para `execute_values` (nota da §9.1), **manter um commit só**; commit por lote reintroduz estado parcial e a garantia cai em silêncio.
- **`scripts/backfill.py` não é comando casual:** ~305 chamadas à API do Last.fm e dezenas de minutos.
- **Não reescrever histórico do git.** Já houve um episódio de `filter-branch` que deixou um contribuidor fantasma no painel do GitHub — irreversível pelo lado do repo.
- Nunca commitar `.env` nem o `.cache` do spotipy (token OAuth).

## Segredos

`LASTFM_API_KEY` e (fase 2) as credenciais OAuth do Spotify vivem só no `.env`, que está no `.gitignore`. `.env.example` é o molde versionado. Nunca commite valores reais; se uma chave vazar no histórico, oriente a gerar uma nova.