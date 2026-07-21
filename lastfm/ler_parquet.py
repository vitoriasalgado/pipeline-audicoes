import boto3, json, io
import pandas as pd

s3 = boto3.client(
    's3',
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

resposta = s3.get_object(Bucket="processed", Key="lastfm/recent.parquet")
dados = resposta["Body"].read()

df_check = pd.read_parquet(io.BytesIO(dados))
print(df_check.dtypes)
print(df_check.head())