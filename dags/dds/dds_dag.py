from airflow import DAG
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

    load_restaurants = PostgresOperator(
        task_id='load_restaurants',
        sql='./sql/dm_restaurants.sql',
    )

    load_timestamps = PostgresOperator(
        task_id='load_timestamps',
        sql='./sql/dm_timestamps.sql',
    )

    load_products = PostgresOperator(
        task_id='load_products',
        sql='./sql/dm_products.sql',
    )

    load_orders = PostgresOperator(
        task_id='load_orders',
        sql='./sql/dm_orders.sql',
    )

    load_sales = PostgresOperator(
        task_id='load_sales',
        sql='./sql/fct_product_sales.sql',
    )

    [load_users, load_timestamps, load_restaurants] >> load_orders
    load_restaurants >> load_products
    [load_orders, load_products] >> load_sales
