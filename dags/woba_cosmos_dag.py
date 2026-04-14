from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping # Adaptando temporariamente
import os
from datetime import datetime

airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db", 
        profile_args={"schema": "public"},
    ),
)

woba_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        (Path(airflow_home) / "dags" / "dbt" / "woba_bookings_dbt").absolute().as_posix(),
    ),
    profile_config=profile_config,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="woba_bookings_dbt_dag",
    default_args={"retries": 2},
)

