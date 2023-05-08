from pg_connect import ConnectionBuilder


def get_postgres_connections():
    source_conn = ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    target_conn = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')
    return source_conn, target_conn
