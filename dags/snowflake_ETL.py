import os

from datetime import datetime
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

FILEPATH = Variable.get("directory")


def decide_branch():
    dwh_hook = SnowflakeHook(
        snowflake_conn_id="snowflake_connection").get_conn()
    if dwh_hook.cursor().execute("SELECT COUNT(*) FROM PLANES").fetchone()[0] == 0:
        return "initial_load.transform_load_airports"
    else:
        return "incremental_load_passengers"


@dag(
    dag_id="snowflake_ETL",
    schedule="@daily",
    description="""Extract csv file load to stage,then load it to raw table. 
                From raw table load to the snowflake DWH""",
    start_date=datetime(2024, 1, 1),
    tags=["python", "snowflake"],
    template_searchpath=FILEPATH,
    catchup=False,
)
def snowflake_ETL() -> None:

    create_stream = SnowflakeOperator(
        task_id="create_stream",
        sql="create_stream.sql",
        snowflake_conn_id="snowflake_connection",
    )

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=decide_branch,
    )
    create_tables = SnowflakeOperator(
        task_id="create_tables",
        sql="create_tables.sql",
        snowflake_conn_id="snowflake_connection",
    )

    create_procedures = SnowflakeOperator(
        task_id="create_procedures",
        sql="create_procedures.sql",
        snowflake_conn_id="snowflake_connection",
    )

    load_raw_data = SnowflakeOperator(
        task_id="load_raw_data",
        sql="load_raw_data.sql",
        snowflake_conn_id="snowflake_connection",
    )

    with TaskGroup(group_id="initial_load") as initial_load:
        transform_load_airports = SnowflakeOperator(
            task_id="transform_load_airports",
            sql="transform_load_airports.sql",
            snowflake_conn_id="snowflake_connection",
        )

        transform_load_continents = SnowflakeOperator(
            task_id="transform_load_continents",
            sql="transform_load_continents.sql",
            snowflake_conn_id="snowflake_connection",
        )

        transform_load_countries = SnowflakeOperator(
            task_id="transform_load_countries",
            sql="transform_load_countries.sql",
            snowflake_conn_id="snowflake_connection",
        )

        transform_load_passengers = SnowflakeOperator(
            task_id="transform_load_passengers",
            sql="transform_load_passengers.sql",
            snowflake_conn_id="snowflake_connection",
        )

        transform_load_planes = SnowflakeOperator(
            task_id="transform_load_planes",
            sql="transform_load_planes.sql",
            snowflake_conn_id="snowflake_connection",
        )

        (
            transform_load_airports
            >> transform_load_continents
            >> transform_load_countries
            >> transform_load_passengers
            >> transform_load_planes
        )

    incremental_load_passengers = SnowflakeOperator(
        task_id="incremental_load_passengers",
        sql="incremental_load_passengers.sql",
        snowflake_conn_id="snowflake_connection",
    )

    create_secure_view = SnowflakeOperator(
        task_id="create_secure_view",
        trigger_rule=TriggerRule.ALL_DONE,
        sql="create_secure_view.sql",
        snowflake_conn_id="snowflake_connection",
    )

    create_marts = SnowflakeOperator(
        task_id="create_marts",
        sql="create_data_marts.sql",
        snowflake_conn_id="snowflake_connection",
    )

    load_to_data_marts = SnowflakeOperator(
        task_id="load_to_data_marts",
        sql="load_to_marts.sql",
        snowflake_conn_id="snowflake_connection",
    )

    (
        create_tables
        >> create_procedures
        >> load_raw_data
        >> create_stream
        >> branching
        >> [initial_load, incremental_load_passengers]
        >> create_secure_view
        >> create_marts
        >> load_to_data_marts
    )


snowflake_ETL()
