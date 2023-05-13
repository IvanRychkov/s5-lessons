from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.staging_utils import (
    get_postgres_connections,
    get_mongo_connection,
    get_mongo_writer,
    get_postgres_dwh_connection,
    get_max_value,
)
from lib.dict_util import json2str
from typing import Callable

dag = DAG(
    dag_id='staging_dag',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 5, 1),
)


def pg_load_ranks():
    source_conn, target_conn = get_postgres_connections()
    # Тянем данные
    with source_conn.connection() as src:
        data = src.execute('select * from ranks').fetchall()
    # Записываем
    with target_conn.connection() as tg:
        tg.execute('delete from stg.bonussystem_ranks')
        for row in data:
            tg.execute('insert into stg.bonussystem_ranks values (%s, %s, %s, %s)', row)


def pg_load_users():
    source_conn, target_conn = get_postgres_connections()
    # Тянем данные
    with source_conn.connection() as src:
        data = src.execute('select * from users').fetchall()
    # Записываем
    with target_conn.connection() as tg:
        tg.execute('delete from stg.bonussystem_users')
        for row in data:
            tg.execute('insert into stg.bonussystem_users values (%s, %s)', row)


def pg_load_events():
    source_conn, target_conn = get_postgres_connections()
    with source_conn.connection() as src, target_conn.connection() as tg:
        # Получаем последний существующий idв нашей базе.
        max_id: tuple = tg.execute(
            "select (workflow_settings ->> 'max_id')::int from stg.srv_wf_settings where workflow_key = 'load_events'").fetchone()
        print('max id is', max_id[0])

        # Делаем запрос, начиная с последнего id
        src_cursor = src.execute('select * from outbox where id > %s', max_id)

        # Пишем по батчам
        batch_n = 0
        while data := src_cursor.fetchmany(1000):
            # Записываем
            for row in data:
                tg.execute('insert into stg.bonussystem_events values (%s, %s, %s, %s)', row)
            batch_n += 1

        # Обновляем последний id
        new_max_id = tg.execute('select max(id) from stg.bonussystem_events where id >= %s', max_id).fetchone()
        j = json2str({'max_id': new_max_id[0]})
        print(j)
        tg.execute(
            '''update stg.srv_wf_settings set workflow_settings = %s where workflow_key = 'load_events' ''',
            (j,)
        )


with dag:
    pg_load_ranks_task = PythonOperator(
        task_id='pg_load_ranks',
        python_callable=pg_load_ranks,
    )

    pg_load_users_task = PythonOperator(
        task_id='pg_load_users',
        python_callable=pg_load_users,
    )

    pg_load_events_task = PythonOperator(
        task_id='pg_load_events',
        python_callable=pg_load_events,
    )

    def get_mdb_loader(collection_name: str, filter_by: str, target_table_name: str) -> Callable[[], None]:
        """Создаёт функцию для перекачки данных из MongoDB в Postgres"""
        def loader_func():
            mongo = get_mongo_connection()
            pg = get_postgres_dwh_connection()
            collection = mongo[collection_name]

            workflow_key = 'ordersystem_load_' + collection_name

            with pg.connection() as pg_conn:
                last_loaded_ts = pg_conn.execute(f"""
                    select workflow_settings ->> 'last_{filter_by}'
                    from stg.srv_wf_settings
                    where workflow_key = '{workflow_key}'
                """).fetchone()
                print(f'last_{filter_by}:', last_loaded_ts)
                # Настраиваем фильтр для монго
                mongo_filter = {filter_by: {'$gt': last_loaded_ts[0]}} if last_loaded_ts else None
    
                # Пишем
                write = get_mongo_writer(target_table_name, pg_conn)
                obj_count = 0
                for obj_count, u in enumerate(collection.find(mongo_filter), start=1):
                    write(str(u['_id']), u[filter_by], json2str(u))
                # Обновляем дату
                j = json2str({'last_' + filter_by: get_max_value(pg_conn, target_table_name, filter_by)})
                print(j)
                pg_conn.execute(
                    '''insert into stg.srv_wf_settings (workflow_key, workflow_settings)
                       values(%s, %s)
                       on conflict (workflow_key) do update
                       set workflow_settings = excluded.workflow_settings''',
                    (workflow_key, j,)
                )
            print(obj_count, 'objects loaded')
        return loader_func


    mdb_load_users_task = PythonOperator(
        task_id='mdb_load_users',
        python_callable=get_mdb_loader('users', 'update_ts', 'stg.ordersystem_users'),
    )
    
    mdb_load_orders_task = PythonOperator(
        task_id='mdb_load_orders',
        python_callable=get_mdb_loader('orders', 'update_ts', 'stg.ordersystem_orders'),
    )
