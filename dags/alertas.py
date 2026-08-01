"""Aviso de falha das DAGs, por webhook.

O Airflow chama `avisar_falha` como `on_failure_callback` — ou seja, só depois
de esgotar os retries. Falha que a segunda tentativa resolve não gera aviso.

Precisa de ALERTA_WEBHOOK_URL no .env. Sem a variável, não avisa e não quebra:
o alerta é observabilidade, não pode derrubar a pipeline.

O corpo `{"content": ...}` é o formato do Discord. No Slack, trocar por `{"text": ...}`.
"""

import os
import requests


def avisar_falha(context):
    url = os.environ.get("ALERTA_WEBHOOK_URL")
    if not url:
        print("ALERTA_WEBHOOK_URL nao configurada — sem aviso", flush=True)
        return

    ti = context["task_instance"]
    texto = (
        f"❌ **{ti.dag_id}** / `{ti.task_id}` falhou\n"
        f"execucao: {context['run_id']}\n"
        f"erro: {context.get('exception')}"
    )

    try:
        resposta = requests.post(url, json={"content": texto}, timeout=10)
        resposta.raise_for_status()
        print("aviso de falha enviado", flush=True)
    except Exception as erro:
        # o aviso falhar não pode mascarar a falha original
        print(f"nao consegui avisar: {erro}", flush=True)
