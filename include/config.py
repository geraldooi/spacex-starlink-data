from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

import os

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]


DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="spacex_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="warehouse",
        profile_args={
            "schema": "public",
        },
    ),
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=f"{AIRFLOW_HOME}/dags/dbt",
    models_relative_path="models",
    seeds_relative_path="seeds",
    snapshots_relative_path="snapshots",
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=f"{AIRFLOW_HOME}/dbt_venv/bin/dbt",
)
