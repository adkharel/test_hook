from airflow.hooks.base import BaseHook
import pyodbc
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

def test_sqlserver_connection():
    conn = BaseHook.get_connection("sql_server_conn")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn.host},{conn.port or 1433};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
        "Encrypt=yes;"
    )

    with pyodbc.connect(conn_str) as c:
        cursor = c.cursor()
        cursor.execute("SELECT 1")
        print(cursor.fetchone())

with DAG(
    dag_id="azure_aks_python_etl",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    test_sqlserver_connection_task = KubernetesPodOperator(
        task_id="test_sqlserver_connection",
        name="test-sqlserver-connection",
        namespace="default",
        image="python:3.9-slim",
        cmds=["python", "-c"],
    )