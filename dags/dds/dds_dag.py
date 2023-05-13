from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import datetime

dag = DAG(
    dag_id='load_dds',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 5, 9),
    catchup=False,
    default_args={
        'postgres_conn_id': 'PG_WAREHOUSE_CONNECTION',
    }
)

with dag:
    load_users = PostgresOperator(
        task_id='load_users',
        sql='./sql/dm_users.sql',
    )