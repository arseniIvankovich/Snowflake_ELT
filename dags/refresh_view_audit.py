import os

from datetime import datetime, timedelta
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.decorators import dag
from airflow.models import Variable

FILEPATH = Variable.get("directory")


@dag(
    dag_id="refresh_view_audit",
    schedule_interval=timedelta(minutes=10),
    description="""Refresh audit view""",
    start_date=datetime(2024, 1, 1),
    tags=["snowflake"],
    template_searchpath=FILEPATH,
    catchup=False,
)
def refresh_view_audit() -> None:

    create_audit_view = SnowflakeOperator(
        task_id="create_audit_view",
        sql="create_audit_view.sql",
        snowflake_conn_id="snowflake_connection",
    )

    create_audit_view


refresh_view_audit()
