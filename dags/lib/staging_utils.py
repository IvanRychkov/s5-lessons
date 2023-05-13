from pg_connect import ConnectionBuilder
from mongo_connect import MongoConnect
from airflow.models import Variable
from dict_util import json2str
from psycopg import Connection
from datetime import datetime
from typing import Any, Callable, Tuple
from pymongo.collection import Collection


def get_postgres_dwh_connection() -> Connection:
    return ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')


def get_postgres_connections() -> Tuple[Connection, Connection]:
    return ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION'), get_postgres_dwh_connection()


def get_mongo_connection() -> Collection:
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    return MongoConnect(
        Variable.get("MONGO_DB_CERTIFICATE_PATH"),
        Variable.get("MONGO_DB_USER"),
        Variable.get("MONGO_DB_PASSWORD"), Variable.get("MONGO_DB_HOST"), Variable.get("MONGO_DB_REPLICA_SET"),
        db,
        db
    ).client()


def get_mongo_writer(target_table: str, pg_conn: Connection) -> Callable[[str, datetime, Any], None]:
    """Пишет данные в формате MDB-объектов."""
    def write_object(id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                        INSERT INTO {target_table} (object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

    return write_object


def get_max_value(pg_conn: Connection, table_name: str, col: str):
    result = pg_conn.execute(f'select max({col}) from {table_name}').fetchone()
    return result[0] if result else None
