import boto3, json, io
import pandas as pd
 
def transformar():

    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    resposta = s3.get_object(Bucket="raw", Key="lastfm/recent.json")
    conteudo = resposta["Body"].read().decode("utf-8")
    data = json.loads(conteudo)
    lista_de_faixas = data["recenttracks"]["track"]
    df = pd.json_normalize(lista_de_faixas)
    df = df[["name", "artist.#text", "album.#text", "date.uts", "mbid", "artist.mbid"]]
    df.rename(columns={
        "name": "faixa",
        "artist.#text": "artista",
        "album.#text": "album",
        "date.uts": "scrobbles_uts",
        "mbid": "faixa_mbid",
        "artist.mbid": "artista_mbid"
    }, inplace=True)
    print(df["scrobbles_uts"].isna().sum())

    df["scrobble_uts"] = pd.to_numeric(df["scrobbles_uts"], errors="coerce")
    df["data_hora"] = pd.to_datetime(df["scrobble_uts"], unit="s", errors="coerce")
    df = df.dropna(subset=["scrobble_uts"])   # descarta o nowplaying (vem sem date)
    df = df.drop_duplicates(subset=["scrobble_uts", "faixa"])
    df = df.drop(columns=["scrobbles_uts"])

    print(df.dtypes)
    print(df.columns)
    print(df.head())
    print(len(df))

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(
        Bucket="processed",
        Key="lastfm/recent.parquet",
        Body=buffer.getvalue()
    )
    print("Parquet gravado em processed/lastfm/recent.parquet")