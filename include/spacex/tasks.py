from airflow.hooks.base import BaseHook
from pandas import DataFrame


def _get_datalake_client():

    from minio import Minio

    # For development purpose
    # Since I use localhost minio, so non secure transfer is easier
    # It is not best practice to do this on production
    # The logic here max change to persist data to Cloud Storage later

    datalake = BaseHook.get_connection("datalake")
    client = Minio(
        endpoint=f"{datalake.host}:{datalake.port}",
        access_key=datalake.login,
        secret_key=datalake.password,
        secure=False,  # True for HTTPS
    )

    return client


def _make_bucket(bucket_name: str) -> str:
    client = _get_datalake_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    return bucket_name


def query_api(route, payload) -> object:
    import requests

    api = BaseHook.get_connection("spacex_api")
    url = f"{api.host}/v4/{route}/query"

    response = requests.request(
        "POST",
        url,
        headers={"Content-Type": "application/json"},
        data=payload,
    )

    return response.json()


def store_json(json_str: str, bucket_name: str, object_key: str) -> str:

    from io import BytesIO

    client = _get_datalake_client()

    bucket_name = _make_bucket(bucket_name)

    write_object = client.put_object(
        bucket_name=bucket_name,
        object_name=object_key,
        data=BytesIO(json_str.encode("utf-8")),
        length=len(json_str),
    )

    return f"s3://{write_object.bucket_name}/{write_object.object_name}"


def store_csv(
    df: DataFrame,
    bucket_name: str,
    object_key: str,
    storage_options: object,
) -> str:

    bucket_name = _make_bucket(bucket_name)

    store_path = f"s3://{bucket_name}/{object_key}"

    df.to_csv(
        path_or_buf=store_path,
        index=False,
        storage_options=storage_options,
    )

    return store_path
