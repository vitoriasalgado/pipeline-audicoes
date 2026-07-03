# Do zero ao seu primeiro post (versão profissional)
### Pipeline de Audições — Last.fm → MinIO, orquestrado por Airflow

Este roteiro te leva do **nada rodando** até o post original que combinamos:
ingestão do seu histórico real do Last.fm caindo num data lake (MinIO),
**orquestrada pelo Apache Airflow**. É mais trabalho que a versão simples, e é de
propósito — esse é o post que mostra competência de verdade.

**O alvo (o que você vai poder dizer com honestidade):**
> "Montei a camada de ingestão da minha pipeline: meu histórico real do Last.fm
> caindo num data lake (MinIO), orquestrado por Airflow. Transformação e carga
> são os próximos passos."

---

## Regras de ouro (vale mais que qualquer código)

1. **Você escreve o código, não cola pronto.** Pode usar os trechos daqui como
   referência, mas digite, rode, quebre e entenda. É a parte "quebrar e entender"
   que vira competência — e que te salva numa entrevista.
2. **Uma missão por vez, cada uma termina numa vitória que você vê na tela.** Não
   pule pra frente. Se a Missão 3 não fechou, não comece a 4.
3. **Entenda antes de avançar.** No fim de cada missão tem um "✅ Você deve saber
   explicar". Se não souber, releia antes de seguir.
4. **Commit cedo e sempre.** Cada missão que fecha = um commit no GitHub.
5. **Ignore o Spotify por enquanto.** Você tem Premium, mas Spotify é OAuth +
   complexidade, e é opcional ("parte 2"). O primeiro post é **100% Last.fm**.
6. **Sem pressa.** Pra iniciante, isso é coisa de algumas semanas, não de um fim
   de semana. O Airflow (Missão 6) é onde você mais vai apanhar — é normal.

---

## Antes de começar

- **Sistema:** os comandos assumem macOS ou Linux. No Windows, troque
  `source .venv/bin/activate` por `.venv\Scripts\activate` e rode os comandos no
  PowerShell.
- **Docker Desktop** vai ser necessário a partir da Missão 3. Já pode instalar:
  https://www.docker.com/products/docker-desktop/
- Tenha à mão: sua conta do Last.fm e o Python instalado (`python3 --version`).

---

## Missão 0 — Esqueleto do projeto, Git e venv

**Objetivo:** começar com o hábito profissional — projeto versionado e ambiente isolado.

```bash
mkdir pipeline-audicoes && cd pipeline-audicoes
git init
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
```

Crie um arquivo `.gitignore` com este conteúdo (impede commitar segredos e lixo):

```
.venv/
.env
__pycache__/
*.pyc
data/
logs/
```

Crie um `requirements.txt`:

```
requests
python-dotenv
boto3
```

Instale:

```bash
pip install -r requirements.txt
```

Crie um `README.md` curtinho (você melhora depois) com 2 linhas: o nome do
projeto e a frase do problema ("entender como meu gosto musical mudou ao longo
dos anos").

Agora suba pro GitHub: crie um repositório vazio em github.com e rode:

```bash
git add .
git commit -m "estrutura inicial do projeto"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/pipeline-audicoes.git
git push -u origin main
```

- [ ] **Checkpoint:** repositório no GitHub, com `.gitignore`, `requirements.txt` e `README.md`.

✅ **Você deve saber explicar:** o que é um ambiente virtual (venv) e por que o
`.env` está no `.gitignore`.

---

## Missão 1 — Conversar com a API do Last.fm

**Objetivo:** primeira vitória de verdade — suas músicas reais aparecendo no terminal.

1. Pegue sua API key (grátis): https://www.last.fm/api/account/create
   (preencha o formulário; você recebe uma `API key`).

2. Crie um arquivo `.env` (que **não** vai pro Git) com:

```
LASTFM_API_KEY=cole_sua_chave_aqui
LASTFM_USER=seu_usuario_do_lastfm
```

3. Crie `teste_api.py`:

```python
import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["LASTFM_API_KEY"]
USER = os.environ["LASTFM_USER"]

url = "https://ws.audioscrobbler.com/2.0/"
params = {
    "method": "user.getRecentTracks",
    "user": USER,
    "api_key": API_KEY,
    "format": "json",
    "limit": 10,
}

resp = requests.get(url, params=params, timeout=30)
resp.raise_for_status()
data = resp.json()

tracks = data["recenttracks"]["track"]
for t in tracks:
    artista = t["artist"]["#text"]
    musica = t["name"]
    print(f"{artista} — {musica}")
```

4. Rode:

```bash
python teste_api.py
```

- [ ] **Checkpoint:** suas últimas 10 músicas reais impressas no terminal.

✅ **Você deve saber explicar:** o que são os `params` da requisição, e por que o
nome do artista vem em `t["artist"]["#text"]` (é uma esquisitice do formato JSON
do Last.fm).

> **Faça um commit:** `git add teste_api.py requirements.txt` → `git commit -m "primeira chamada à API do Last.fm"` → `git push`

---

## Missão 2 — Guardar o dado cru (bronze, ainda local)

**Objetivo:** salvar a resposta da API como um arquivo JSON, exatamente como veio.
Isso é a "camada bronze" — dado cru e imutável.

Crie `extrair.py`:

```python
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["LASTFM_API_KEY"]
USER = os.environ["LASTFM_USER"]

url = "https://ws.audioscrobbler.com/2.0/"
params = {
    "method": "user.getRecentTracks",
    "user": USER,
    "api_key": API_KEY,
    "format": "json",
    "limit": 200,   # máximo por página
    "page": 1,
}

resp = requests.get(url, params=params, timeout=30)
resp.raise_for_status()
data = resp.json()

os.makedirs("data/raw", exist_ok=True)
caminho = "data/raw/recent.json"
with open(caminho, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

total = data["recenttracks"]["@attr"]["total"]
print(f"salvo em {caminho} — você tem {total} scrobbles no total")
```

Rode `python extrair.py` e **abra o arquivo** `data/raw/recent.json` no seu editor.
Olhe a estrutura. Repare no campo `total` — é o número real de músicas que você já
ouviu (guarde esse número, ele vai pro post).

- [ ] **Checkpoint:** um arquivo `data/raw/recent.json` com dado real, e você viu seu número total de scrobbles.

✅ **Você deve saber explicar:** por que guardamos o dado cru sem limpar nada aqui
(princípio: a camada bronze é imutável; a limpeza vem depois).

---

## Missão 3 — Subir o data lake (Docker + MinIO)

**Objetivo:** ter o MinIO rodando localmente e o console aberto no navegador.

Crie `docker-compose.yml` na raiz do projeto:

```yaml
services:
  minio:
    image: quay.io/minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"   # API (onde o código fala)
      - "9001:9001"   # Console (onde você olha)
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data

volumes:
  minio_data:
```

Suba:

```bash
docker compose up -d
```

Abra **http://localhost:9001** no navegador. Login: `minioadmin` / `minioadmin`.
No console, vá em **Buckets → Create Bucket** e crie um bucket chamado `raw`.

- [ ] **Checkpoint:** console do MinIO aberto, bucket `raw` criado (vazio por enquanto).

✅ **Você deve saber explicar:** o que é object storage (bucket / objeto / key) e
por que o MinIO é útil — ele fala a **mesma API do S3 da AWS**, então o código que
você escreve aqui funciona na nuvem real só trocando o endereço.

---

## Missão 4 — Colocar o dado dentro do lake (ingestão local)

**Objetivo:** enviar seu JSON para dentro do bucket `raw` via Python. Aqui nasce a
"camada de ingestão".

Crie `subir_para_minio.py`:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

s3.upload_file("data/raw/recent.json", "raw", "lastfm/recent.json")
print("enviado para o bucket 'raw' como lastfm/recent.json")
```

Rode `python subir_para_minio.py`. Volte ao console do MinIO, atualize, entre no
bucket `raw` → pasta `lastfm/` → seu `recent.json` está lá.

- [ ] **Checkpoint:** seu histórico real do Last.fm, em JSON, **dentro do data lake**, visível no console.

✅ **Você deve saber explicar:** o caminho completo que o dado fez — origem (API)
→ raw (lake) — e por que o `boto3` falando com MinIO é o mesmo código que falaria
com a AWS S3.

> Esse já seria o post da versão "simples". Mas você escolheu a versão profissional
> — então falta o Airflow orquestrar isso. Bora.

---

## Missão 5 — Preparar a extração para virar uma tarefa

**Objetivo:** juntar "extrair" + "subir pro MinIO" numa única função, que o Airflow
vai chamar. Pequena refatoração.

A ideia: em vez de dois scripts soltos, ter **uma função** `extrair_para_minio()`
que faz tudo. Você vai colá-la dentro da DAG na próxima missão. Por ora, só entenda
que ela é a fusão do que você já fez:

```python
import json, os, requests, boto3

def extrair_para_minio():
    api_key = os.environ["LASTFM_API_KEY"]
    user = os.environ["LASTFM_USER"]

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {"method": "user.getRecentTracks", "user": user,
              "api_key": api_key, "format": "json", "limit": 200}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",   # atenção: 'minio', não 'localhost'
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket="raw", Key="lastfm/recent.json", Body=corpo)
    print("ingestão concluída: lastfm/recent.json no bucket raw")
```

⚠️ **A mudança crítica:** o endereço virou `http://minio:9000` (e não
`localhost:9000`). Motivo: o código vai rodar **dentro** de um container do Airflow,
e lá os serviços se enxergam pelo **nome** (`minio`), não por `localhost`.

✅ **Você deve saber explicar:** por que `localhost` funcionava no seu PC mas vira
`minio` dentro do container.

---

## Missão 6 — Subir o Airflow ⚠️ (a parte difícil)

**Objetivo:** ter o Airflow rodando, com a interface web no ar.

> **Leia isto antes:** essa é, de longe, a missão mais chata. É normal apanhar aqui
> — todo mundo apanha. Vá com paciência, e se algo não subir, a referência oficial
> (link no fim da missão) é a fonte da verdade. Não desanime: passou daqui, o resto
> é tranquilo.

O Airflow precisa de várias peças (webserver, scheduler, um Postgres só pra ele).
A forma confiável é usar o **docker-compose oficial do Airflow** como base.

**1. Baixe o compose oficial** (fixando uma versão estável) na raiz do projeto:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```

**2. Faça 3 ajustes nesse `docker-compose.yaml`:**

**(a)** Dentro de `x-airflow-common:` → `environment:`, adicione suas variáveis e os
pacotes que sua DAG usa:

```yaml
    _PIP_ADDITIONAL_REQUIREMENTS: "requests boto3"
    LASTFM_API_KEY: ${LASTFM_API_KEY}
    LASTFM_USER: ${LASTFM_USER}
```

**(b)** No final, dentro de `services:`, adicione o MinIO como mais um serviço (pra
ele viver na mesma rede do Airflow):

```yaml
  minio:
    image: quay.io/minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
```

**(c)** Na seção `volumes:` do final do arquivo, acrescente `minio_data:`:

```yaml
volumes:
  postgres-db-volume:
  minio_data:
```

**3. Garanta as variáveis no ambiente.** Seu `.env` já tem `LASTFM_API_KEY` e
`LASTFM_USER` (Missão 1) — o compose vai lê-las. No Linux, adicione também:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

**4. Inicialize e suba:**

```bash
docker compose up airflow-init      # roda UMA vez (cria banco + usuário admin)
docker compose up -d
```

Abra **http://localhost:8080**. Login: `airflow` / `airflow`.

- [ ] **Checkpoint:** interface do Airflow no ar em localhost:8080, e o console do MinIO ainda funcionando em localhost:9001.

✅ **Você deve saber explicar:** o que o Airflow faz (ele **coordena**, não guarda
dado) e por que ele precisa de um Postgres próprio (o "banco de metadados").

📚 **Referência oficial (se travar):**
https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html

---

## Missão 7 — A DAG: ingestão orquestrada 🎯 (o marco do post)

**Objetivo:** transformar sua função numa DAG que o Airflow executa, e vê-la ficar
verde na interface.

Crie a pasta `dags/` e dentro dela o arquivo `dags/pipeline_audicoes.py`:

```python
from datetime import datetime
import json, os, requests, boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


def extrair_para_minio():
    api_key = os.environ["LASTFM_API_KEY"]
    user = os.environ["LASTFM_USER"]

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {"method": "user.getRecentTracks", "user": user,
              "api_key": api_key, "format": "json", "limit": 200}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    corpo = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket="raw", Key="lastfm/recent.json", Body=corpo)
    print("ingestão concluída")


with DAG(
    dag_id="pipeline_audicoes",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lastfm"],
) as dag:
    extrair = PythonOperator(
        task_id="extrair",
        python_callable=extrair_para_minio,
    )
```

Espere ~1 minuto (o Airflow varre a pasta `dags/` sozinho). Atualize a interface em
localhost:8080 — a DAG `pipeline_audicoes` deve aparecer na lista.

1. Ative a DAG (o botãozinho à esquerda do nome).
2. Dispare uma execução (o ▶️ "Trigger DAG" à direita).
3. Acompanhe: a task `extrair` deve ficar **verde**.
4. Confirme no console do MinIO (localhost:9001) que o `lastfm/recent.json` foi
   gravado (apague o antigo antes, pra ter certeza de que foi a DAG que escreveu).

- [ ] **Checkpoint:** a task `extrair` **verde** na interface do Airflow + o JSON no MinIO, gravado pela DAG.

> **Se a task ficar vermelha:** clique nela → "Logs". O erro está lá. Os clássicos:
> esqueceu o bucket `raw` (crie no console), faltou `requests`/`boto3` (veja o ajuste
> (a) da Missão 6), ou usou `localhost` em vez de `minio` no endpoint. Ler log de
> task que falhou é metade do trabalho de engenharia de dados — e, aliás, vira um
> ótimo segundo post ("o bug que me fez entender X").

✅ **Você deve saber explicar:** o que é uma DAG (tarefas + dependências, sem ciclos)
e o que significa a sua ter ficado verde.

> **Commit:** `git add dags/ docker-compose.yaml` → `git commit -m "DAG de ingestão Last.fm -> MinIO orquestrada pelo Airflow"` → `git push`

---

## Missão 8 — Deixar apresentável e publicar

**Objetivo:** repositório limpo + prints + post no ar.

**1. Capriche no README** (é a primeira coisa que um recrutador abre). Inclua:
- a frase do problema;
- o diagrama da arquitetura (você já tem o `etl` gerado — pode usá-lo aqui);
- como rodar (os comandos `docker compose`);
- o que **está pronto** (ingestão) e o que **vem depois** (transform, load).

**2. Confirme que NÃO há segredos no Git.** Rode `git log -p | grep -i lastfm_api`
— se sua chave aparecer, **gere uma nova** no Last.fm (a antiga vazou). O `.env`
tem que estar no `.gitignore` (Missão 0).

**3. Tire 2 prints:**
- a interface do Airflow com a task `extrair` **verde** (o print mais forte);
- o console do MinIO com o `recent.json` dentro do bucket `raw`.

**4. Publique o post** (rascunho abaixo).

---

## O post (versão profissional — é só colar)

> Antes de postar: troque o número de scrobbles pelo seu real (campo `total` que
> você viu na Missão 2). LinkedIn não renderiza markdown — o texto abaixo é puro,
> só com quebras de linha. A primeira linha é a que mais importa (é ela que aparece
> antes do "ver mais").

```
Sempre quis entender como meu gosto musical mudou ao longo dos anos. Em vez de só me perguntar isso, escolhi transformar a dúvida no meu primeiro projeto de engenharia de dados.

Toda música que eu ouço fica registrada no Last.fm — são quase [SEU NÚMERO] faixas acumuladas que eu nunca tinha de fato analisado.

Acabei de colocar de pé a primeira peça da pipeline: a camada de ingestão, já orquestrada.

O que roda hoje:
• um job em Python puxa meu histórico direto da API do Last.fm
• o dado cru (JSON) é gravado num data lake — subi o MinIO com Docker, que fala a mesma API do S3 da AWS
• quem dispara, agenda e monitora tudo isso é o Apache Airflow: a tarefa de ingestão roda e fica verde na interface, com retry automático se falhar

A arquitetura segue o modelo medalhão (bronze → prata → ouro): primeiro o dado cru no lake; depois vêm a limpeza em Parquet e a carga num data warehouse em PostgreSQL com esquema estrela.

Importante ser honesto: por enquanto é a camada de ingestão. A transformação e a carga são os próximos passos — a pipeline ainda não roda verde de ponta a ponta. Mas ver meus dados reais caindo no lugar certo, no horário agendado, com código que eu mesmo escrevi e o Airflow coordenando, foi um marco e tanto pra quem está começando.

Próximo capítulo: a esteira completa rodando e a primeira consulta respondendo ("qual foi meu artista mais ouvido em cada mês").

Projeto documentado no GitHub (link nos comentários). Toda crítica de quem é da área é muito bem-vinda.

#EngenhariaDeDados #DataEngineering #Python #ApacheAirflow #Docker
```

**Mecânica do post:**
- O link do GitHub vai no **primeiro comentário** (link no corpo tende a reduzir o alcance).
- Anexe os 2 prints (Airflow verde + bucket do MinIO). Post com imagem alcança mais.
- Bom horário: meio de semana de manhã (terça a quinta). Responda os comentários nas primeiras horas — ajuda o alcance.
- Tom: "iniciante, primeiro projeto" é o enquadramento certo. Ninguém espera produção de quem monta portfólio — e honestidade engaja mais.

---

## O que vem depois (seus próximos posts)

Você não precisa parar aqui — mas também não precisa correr. Os próximos marcos,
casados com as fases do seu guia, viram os próximos posts:

1. **Transformação** (`transform`): ler o JSON cru, limpar com pandas, salvar em
   Parquet no MinIO (camada prata). — Fase 4 do guia.
2. **Carga** (`load`): modelar o esquema estrela e carregar no PostgreSQL analítico
   (camada ouro). — Fase 4.
3. **A DAG verde de ponta a ponta** + a primeira query respondendo. **Esse é o post
   mais forte de todos** — se você fizer só mais um na vida, é esse.
4. (Opcional) Spotify como "parte 2", e tasks de qualidade de dados (Fase 5).

Quando chegar em cada um, é só me chamar que a gente detalha — do mesmo jeito.

**Comece hoje pela Missão 0.** Uma vitória de cada vez.
