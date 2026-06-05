from airflow.hooks.base import BaseHook
import pyodbc

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