from airflow.hooks.base import BaseHook

_datalake = BaseHook.get_connection("datalake")

storage_options = {
    "key": _datalake.login,
    "secret": _datalake.password,
    "client_kwargs": {
        "endpoint_url": f"http://{_datalake.host}:{_datalake.port}",
    },
}
