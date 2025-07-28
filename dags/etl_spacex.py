from airflow.decorators import dag, task
from cosmos import DbtDag, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime, DateTime

from include.spacex.tasks import query_api, store_json, store_csv
from include.utils.datalake import storage_options
from include.utils.warehouse import engine
from include.config import (
    DBT_PROFILE_CONFIG,
    DBT_PROJECT_CONFIG,
    DBT_EXECUTION_CONFIG,
)


@dag(
    start_date=datetime(2019, 1, 1, tz="Asia/Kuala_Lumpur"),
    schedule="@daily",
    catchup=False,
    tags=["starlink"],
)
def etl_spacex():

    @task
    def get_data(
        route: str,
        logical_date: DateTime,
        date_field: str = None,
    ) -> list[object]:

        import json

        # Expect user to call this function by:
        # get_data("starlink", date_field="spaceTrack.CREATION_DATE")
        query_obj = {}
        if date_field:
            query_obj = {
                date_field: {
                    "$gte": logical_date.subtract(days=1).format("YYYY-MM-DD"),
                    "$lt": logical_date.format("YYYY-MM-DD"),
                }
            }

        current_page = 1
        has_next_page = True
        result = []
        while has_next_page:
            query_payload = json.dumps(
                {
                    "query": query_obj,
                    "options": {
                        "limit": 1000,  # To reduce ddos the api. default: 10
                        "page": current_page,
                    },
                }
            )

            response_json_data = query_api(route=route, payload=query_payload)
            result += response_json_data["docs"]

            has_next_page = response_json_data["hasNextPage"]
            current_page = response_json_data["nextPage"]

        return result

    @task
    def store_data(data_name: str, obj_list: list[object], ds_nodash: str) -> str:

        import json

        data_path = store_json(
            json_str=json.dumps(obj_list),
            bucket_name="spacex",
            object_key=f"{data_name}/{data_name}_{ds_nodash}.json",
        )

        return data_path

    @task
    def format_data(data_name: str, data_path: str) -> str:

        import pandas as pd

        raw_df = pd.read_json(
            data_path,
            storage_options=storage_options,
        )
        df = pd.json_normalize(raw_df.to_dict(orient="records"))

        new_file_name = data_path.split("/")[-1].split(".")[0]
        new_path = store_csv(
            df=df,
            bucket_name="spacex-csv",
            object_key=f"{data_name}/{new_file_name}.csv",
            storage_options=storage_options,
        )

        return new_path

    @task
    def load_data_to_warehouse(data_name: str, data_path: str):

        import pandas as pd
        from sqlalchemy import inspect, text

        table_name = f"{data_name}_bronze"

        df = pd.read_csv(
            data_path,
            storage_options=storage_options,
        )

        with engine.connect() as connection:
            inspector = inspect(connection)

            if inspector.has_table(table_name, schema="public"):
                print(f"Table public.{table_name} exists. Truncating table ...")
                truncate_sql = text(
                    f'TRUNCATE TABLE "public"."{table_name}" RESTART IDENTITY CASCADE;'
                )
                connection.execute(truncate_sql)
                print(f"Table public.{table_name} truncated successfully.")

            else:
                print(f"Table public.{table_name} does not exists. Creating table ...")
                df.head(0).to_sql(
                    name=table_name, con=connection, if_exists="replace", index=False
                )
                print(f"Table public.{table_name} created successfully.")

        # Append new data
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        profile_config=DBT_PROFILE_CONFIG,
        project_config=DBT_PROJECT_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
    )

    job_list = ["starlink", "launches"]
    for job in job_list:
        json_data = get_data.override(task_id=f"get_{job}")(route=job)
        json_path = store_data.override(task_id=f"store_{job}")(
            data_name=job,
            obj_list=json_data,
        )
        csv_path = format_data.override(task_id=f"format_{job}")(
            data_name=job,
            data_path=json_path,
        )
        loaded = load_data_to_warehouse.override(task_id=f"load_{job}")(
            data_name=job,
            data_path=csv_path,
        )
        loaded >> transformation


etl_spacex()
