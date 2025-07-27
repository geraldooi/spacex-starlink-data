from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

_warehouse = BaseHook.get_connection("warehouse")

engine = create_engine(
    f"postgresql://{_warehouse.login}:{_warehouse.password}@{_warehouse.host}:5432/{_warehouse.schema}"
)
