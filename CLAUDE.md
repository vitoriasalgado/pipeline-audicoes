# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que este repositório é (leia primeiro)

Este NÃO é um projeto de produção. É o **primeiro projeto de engenharia de dados de uma iniciante**, construído como aprendizado e portfólio, missão a missão. O trabalho é guiado por dois documentos que são a fonte da verdade:

- `docs/PRD_Pipeline_Audicoes.md` — escopo completo, fontes/APIs, modelo de dados, DAGs, riscos.
- `docs/roteiro_post.md` — passo a passo (Missão 0 → 8), com o código de referência de cada etapa. **Local/gitignored** (não versionado): existe só na máquina da usuária, não no repo público.

### Como atuar aqui: mentor, não implementador

A usuária pediu explicitamente que Claude aja como **mentor**. Isso muda o modo de operar em relação ao normal:

- **NÃO escreva o código das missões por ela.** As "regras de ouro" do roteiro dizem que ela digita, roda, quebra e entende — é isso que gera aprendizado. O papel do Claude é explicar o *porquê*, revisar o que ela escreveu, e ajudar a debugar.
- **Uma missão por vez.** Não pule para frente nem adiante código de missões futuras. Só avance quando a missão atual fechar numa vitória visível na tela.
- **Ao fim de cada missão**, confira o bloco "✅ Você deve saber explicar" do roteiro fazendo perguntas para confirmar entendimento antes de seguir.
- **Commit ao fim de cada missão.** Push para `origin/main`. Mensagens de commit em **inglês, no padrão Conventional Commits** (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`…). Sem co-autor Claude (já desligado via `attribution` no settings).
- Tom acolhedor de iniciante; normalize a dificuldade (o Airflow, na Missão 6, é onde todo mundo apanha).

Exceção: refatorações mecânicas, configuração de ambiente, depuração e revisão são bem-vindas — a restrição é sobre não entregar pronto o código pedagógico das missões.

## Estado atual

- Missão 0 (esqueleto, Git, venv) — concluída.
- Missão 1 (primeira chamada à API do Last.fm) — concluída. Código em `test_api.py` (lê `user.getRecentTracks` e imprime as faixas recentes no terminal).
- Missão 2 (salvar o JSON cru em `data/raw/`, camada bronze) — próximo passo.

Repo público: https://github.com/vitoriasalgado/pipeline-audicoes

Ainda não há `docker-compose.yaml` — nasce na Missão 3 (MinIO). O checklist em `README.md` ("Roteiro") reflete o progresso; mantenha-o atualizado ao fechar missões.

## Arquitetura (o big picture)

Pipeline medalhão de ponta a ponta do histórico musical:

```
Last.fm API ──► raw/ (JSON, bronze) ──► processed/ (Parquet, prata) ──► PostgreSQL analytics (ouro)
   Spotify API (fase 2, opcional, OAuth) ─── enriquece as dimensões ───┘
        MinIO = data lake · Apache Airflow orquestra tudo
```

Princípios que atravessam todo o código e devem ser respeitados nas decisões:

- **Camadas medalhão separadas por responsabilidade:** a ingestão (`extract`) só escreve o `raw` (JSON cru, imutável) e NÃO se mistura com transformação. `transform` lê `raw` e escreve Parquet em `processed`. `load` modela e carrega o Postgres.
- **`raw` é imutável e é a fonte única.** Qualquer análise nova reprocessa o `raw`, sem rechamar a API.
- **Idempotência e carga incremental** são as duas decisões que definem a corretude do núcleo: filtrar `nowplaying` (vem sem `date`), `UNIQUE (scrobble_uts, faixa_id)` com `ON CONFLICT DO NOTHING`, e usar o parâmetro `from` do Last.fm para puxar só o novo desde a última execução.
- **Modelo estrela/constelação:** duas fatos (`fato_audicoes` do Last.fm, `fato_top_spotify`) compartilhando `dim_artista`, `dim_faixa`, `dim_tempo`. Faixas/artistas casam entre fontes **por nome** (chave de negócio); `spotify_*_id` e `mbid` são atributos de apoio e podem vir vazios.
- **`localhost` vs nome de serviço:** scripts rodados no host falam com o MinIO em `http://localhost:9000`; código que roda dentro de container (Airflow) usa `http://minio:9000`. Confundir isso é um erro clássico.

## Comandos

Ambiente (macOS/Linux; no Windows use `.venv\Scripts\activate`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencha LASTFM_API_KEY e LASTFM_USER
```

Rodar um script de uma missão (ex.):

```bash
python test_api.py
```

Infraestrutura (a partir da Missão 3 / 6, quando os composes existirem):

```bash
docker compose up -d          # MinIO (Missão 3) e depois Airflow (Missão 6)
```

- MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`), bucket `raw`.
- Airflow UI: http://localhost:8080 (`airflow` / `airflow`).

Não há suíte de testes nem linter configurados — a "verificação" de cada missão é o checkpoint visível descrito no roteiro (saída no terminal, arquivo no bucket, task verde no Airflow).

## Segredos

`LASTFM_API_KEY` e (fase 2) as credenciais OAuth do Spotify vivem só no `.env`, que está no `.gitignore`. `.env.example` é o molde versionado. Nunca commite valores reais; se uma chave vazar no histórico, oriente a gerar uma nova.