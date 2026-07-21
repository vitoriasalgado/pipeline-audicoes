import boto3, json, io
import pandas as pd
 

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

resposta = s3.get_object(Bucket="raw", Key="spotify/top_tracks_medium_term.json")
conteudo = resposta["Body"].read().decode("utf-8")
data = json.loads(conteudo)

print(len(data["items"]))
