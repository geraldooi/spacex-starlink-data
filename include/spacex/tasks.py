import logging

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from pandas import DataFrame


task_logger = logging.getLogger("airflow.task")


def _get_datalake_client():

    from minio import Minio

    # For development purpose
    # Since I use localhost minio, so non secure transfer is easier
    # It is not best practice to do this on production
    # The logic here max change to persist data to Cloud Storage later

    try:
        datalake = BaseHook.get_connection("datalake")
        client = Minio(
            endpoint=f"{datalake.host}:{datalake.port}",
            access_key=datalake.login,
            secret_key=datalake.password,
            secure=False,  # True for HTTPS
        )

        return client

    except ImportError as e:
        task_logger.error(f"Failed to import minio. Please check requirements.txt")
        task_logger.error(f"Error: {e}")
        raise AirflowFailException(f"Minio not found. Task cannot proceed.")

    except Exception as e:
        task_logger.error(
            f"Unexpected issue happend trying to get minio client.", exc_info=True
        )
        raise AirflowFailException("Unexpected error when getting minio client.")


def _make_bucket(bucket_name: str) -> str:
    client = _get_datalake_client()

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            task_logger.info(f"Minio bucket '{bucket_name}' created.")
        else:
            task_logger.info(f"Minio bucket '{bucket_name}' exists.")

        return bucket_name

    except Exception:
        raise AirflowException(
            f"An unexpected error while trying to check for bucket '{bucket_name}'."
        )


def query_api(route, payload) -> object:
    import requests

    api = BaseHook.get_connection("spacex_api")
    url = f"{api.host}/v4/{route}/query"

    try:
        response = requests.request(
            "POST",
            url,
            headers={"Content-Type": "application/json"},
            data=payload,
        )

        # Raise an HTTPError for bad responses (4xx or 5xx status codes)
        response.raise_for_status()

        return response.json()

    except requests.exceptions.HTTPError as e:
        # Catch exception raise by response.raise_for_status()
        # Especially 4xx or 5xx
        status_code = e.response.status_code
        task_logger.error(f"HTTP Error for {url} (Status Code: {status_code}): {e}")
        task_logger.error(f"Response content: {e.response.text}")
        raise

    except requests.exceptions.ConnectionError as e:
        # Catch network related error
        task_logger.error(f"Connection error for {url}: {e}")
        task_logger.error(
            "Could not connect to the API. Please check your network connection."
        )
        raise

    except requests.exceptions.Timeout as e:
        # Catch when the request timeout
        task_logger.error(f"Timeout for {url}: {e}")
        task_logger.error(
            "API request time out. The server seem taking too long to response."
        )
        raise

    except ValueError as e:
        # Catch the case where response.json() fail to parse
        task_logger.error(f"Error parsing JSON response from {url}: {e}")
        task_logger.error(f"Raw response content: {response.text}")
        raise

    except Exception as e:
        # Catch other exception not listed above
        task_logger.error(
            f"An unexpected error occurred when query {url}: {e}",
            exc_info=True,
        )
        raise AirflowFailException(f"Unexpected error when query API: {url}")


def store_json(json_str: str, bucket_name: str, object_key: str) -> str:

    from io import BytesIO

    client = _get_datalake_client()

    bucket_name = _make_bucket(bucket_name)

    try:
        write_object = client.put_object(
            bucket_name=bucket_name,
            object_name=object_key,
            data=BytesIO(json_str.encode("utf-8")),
            length=len(json_str),
        )

        return f"s3://{write_object.bucket_name}/{write_object.object_name}"

    except Exception:
        raise AirflowFailException(
            "An unexpected error happened while trying to save json data to datalake."
        )


def store_csv(
    df: DataFrame,
    bucket_name: str,
    object_key: str,
    storage_options: object,
) -> str:

    bucket_name = _make_bucket(bucket_name)

    store_path = f"s3://{bucket_name}/{object_key}"

    try:
        df.to_csv(
            path_or_buf=store_path,
            index=False,
            storage_options=storage_options,
        )

        return store_path

    except Exception:
        raise AirflowFailException(
            "An unexpected error happened while trying to save csv file to datalake."
        )
