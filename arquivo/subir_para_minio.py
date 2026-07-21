import boto3

s3 = boto3.client(
    's3',
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

s3.upload_file("data/raw/recent.json", "raw", "lastfm/recent.json")
print("Arquivo enviado para bucket 'raw' como lastfm/recent.json")