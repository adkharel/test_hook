from airflow.sdk import dag, task  
from airflow.sdk.bases.hook import BaseHook
import pyodbc
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


default_args = {
    'owner': 'Aditya',
    'start_date': datetime(2024, 2, 12)
}

@dag(
        dag_id = "sql_processing",
        default_args=default_args, 
        schedule="@once", 
        description="Simple SQL Test", 
        catchup=False, 
        tags=['DB1-DEV']
)
def first_dag():

    # Task Definition
    start = EmptyOperator(task_id='start')

    @task
    def first_task():
        print("And so, it begins!")

    @task
    def read_data():
        conn = BaseHook.get_connection("sql_server_conn")
        print(conn.schema)
        conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn.host},{conn.port or 1433};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
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

# if __name__ == "__main__":
#     print("Testing the DAG...")
#     execution = first_dag()
#     execution.test()