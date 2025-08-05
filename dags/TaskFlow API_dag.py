from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
import os

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    dag_id="my_first_dag",
)
def dbt_cosmos_dag():
    dbt_tasks = DbtTaskGroup(
        project_config=ProjectConfig(os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'dbt')),
        profile_config=ProfileConfig(
            profile_name="dbt_study",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_dbt",
                profile_args={"schema": "intermediate"},
            ),
        ),
        render_config=RenderConfig(select=["stg_flights__aircrafts"]),
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )
    return dbt_tasks

dag = dbt_cosmos_dag()