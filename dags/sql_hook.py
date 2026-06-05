from airflow.hooks.base import BaseHook
import pyodbc
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# def test_sqlserver_connection():
#     conn = BaseHook.get_connection("sql_server_conn")

#     conn_str = (
#         f"DRIVER={{ODBC Driver 18 for SQL Server}};"
#         f"SERVER={conn.host},{conn.port or 1433};"
#         f"DATABASE={conn.schema};"
#         f"UID={conn.login};"
#         f"PWD={conn.password};"
#         "Encrypt=yes;"
#     )

#     with pyodbc.connect(conn_str) as c:
#         cursor = c.cursor()
#         cursor.execute("SELECT 1")
#         print(cursor.fetchone())

# with DAG(
#     dag_id="azure_aks_python_etl",
#     start_date=datetime(2026, 1, 1),
#     schedule="@once",
#     catchup=False,
# ) as dag:
#     test_sqlserver_connection_task = KubernetesPodOperator(
#         task_id="test_sqlserver_connection",
#         name="test-sqlserver-connection",
#         namespace="default",
#         image="python:3.9-slim",
#         cmds=["python", "-c"],
#     )

from airflow.hooks.base import BaseHook
import pyodbc
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'Aditya',
    'start_date': datetime(2024, 2, 12)
}

@dag(
        default_args=default_args, 
        schedule="@once", 
        description="Simple SQL Test", 
        catchup=False, 
        tags=['DB1-DEV']
)
def sql_processing():

    # Task Definition
    start = EmptyOperator(task_id='start')

    @task
    def first_task():
        print("And so, it begins!")

    @task
    def read_data():
        conn = BaseHook.get_connection("sql_server_conn")
        conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn.host},{conn.port or 1433};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
        "Encrypt=no;"
        )

        with pyodbc.connect(conn_str) as c:
            cursor = c.cursor()
            cursor.execute("SELECT 1")
            print(cursor.fetchone())

            end = EmptyOperator(task_id='end')
        return 0

    # Orchestration
    first = first_task()
    downloaded = read_data()
    start >> first >> downloaded

execution = sql_processing()