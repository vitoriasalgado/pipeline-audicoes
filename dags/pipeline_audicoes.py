from datetime import datetime
import json, os, requests, boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

def extrair_para_minio():
    api_key = os.environ['LASTFM_API_KEY']
    username = os.environ['LASTFM_USER']

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getRecentTracks",
        "user": username,
        "api_key": api_key,
        "format": "json",
        "limit": 200
    }
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
    print("Ingestão concluída: lastfm/recent.json no bucket raw")

with DAG(
    dag_id="pipeline_audicoes",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lastfm"],
) as dag:
    extrair_task = PythonOperator(
        task_id="extrair",
        python_callable=extrair_para_minio
    )