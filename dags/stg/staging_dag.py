from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.staging_utils import get_postgres_connections
from lib.dict_util import json2str

dag = DAG(
    dag_id='staging_dag',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 5, 1),
)


def load_ranks():
    source_conn, target_conn = get_postgres_connections()
    # Тянем данные
    with source_conn.connection() as src:
        data = src.execute('select * from ranks').fetchall()
    # Записываем
    with target_conn.connection() as tg:
        tg.execute('delete from stg.bonussystem_ranks')
        for row in data:
            tg.execute('insert into stg.bonussystem_ranks values (%s, %s, %s, %s)', row)
            

def load_users():
    source_conn, target_conn = get_postgres_connections()
    # Тянем данные
    with source_conn.connection() as src:
        data = src.execute('select * from users').fetchall()
    # Записываем
    with target_conn.connection() as tg:
        tg.execute('delete from stg.bonussystem_users')
        for row in data:
            tg.execute('insert into stg.bonussystem_users values (%s, %s)', row)
    
    
def load_events():
    source_conn, target_conn = get_postgres_connections()
    with source_conn.connection() as src, target_conn.connection() as tg:
        # Получаем последний существующий idв нашей базе.
        max_id: tuple = tg.execute("select (workflow_settings ->> 'max_id')::int from stg.srv_wf_settings where workflow_key = 'load_events'").fetchone()
        print('max id is', max_id[0])
        
        # Делаем запрос, начиная с последнего id
        src_cursor = src.execute('select * from outbox where id > %s', max_id)
        
        # Пишем по батчам
        batch_n = 0
        while data := src_cursor.fetchmany(1000):
            # Записываем
            for row in data:
                tg.execute('insert into stg.bonussystem_events values (%s, %s, %s, %s)', row)
            #     tg.execute('insert into stg.bonussystem_users values (%s, %s)', row)
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
    load_ranks_task = PythonOperator(
        task_id='load_ranks',
        python_callable=load_ranks,
    )
    
    load_users_task = PythonOperator(
        task_id='load_users',
        python_callable=load_users,
    )
    
    load_events_task = PythonOperator(
        task_id='load_events',
        python_callable=load_events,
    )
