import psycopg2
from contextlib import contextmanager
from psycopg2 import pool, extensions
from langchain_community.utilities import SQLDatabase
from config.settings import ENV_CONFIG, DB_URI

DB_CONFIG = {
    'dbname':   ENV_CONFIG['db_name'],
    'user':     ENV_CONFIG['db_user'],
    'password': ENV_CONFIG['db_password'],
    'host':     ENV_CONFIG['db_host'],
    'port':     int(ENV_CONFIG['db_port']),
}

_pool: "pool.SimpleConnectionPool | None" = None


def _get_pool() -> pool.SimpleConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
    return _pool


def _is_conn_alive(conn) -> bool:
    """Return True if the connection is still usable."""
    try:
        return conn is not None and conn.closed == 0
    except Exception:
        return False


@contextmanager
def get_db():
    """Yield a pooled connection with auto commit/rollback.

    Broken connections (closed by the server, network drop, etc.) are
    discarded rather than returned to the pool, preventing the
    'connection already closed' cascade error on the next checkout.
    """
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        # Only rollback if the connection is still alive.
        if _is_conn_alive(conn):
            try:
                conn.rollback()
            except Exception:
                pass  # ignore rollback errors on a broken connection
        raise
    finally:
        if _is_conn_alive(conn):
            p.putconn(conn)      # healthy — return to pool
        else:
            # Discard the broken connection so the pool replaces it.
            try:
                p.putconn(conn, close=True)
            except Exception:
                pass


def get_connection() -> extensions.connection:
    """Open and return a plain (non-pooled) connection.

    Kept for backward compatibility. Prefer get_db() for new code.
    """
    return psycopg2.connect(**DB_CONFIG)


def get_sql_database() -> SQLDatabase:
    return SQLDatabase.from_uri(
        DB_URI,
        include_tables=[
            'accounts', 'transactions', 'splits',
            'categories', 'currencies', 'institutions',
            'historical_fx', 'historical_prices', 'holdings',
            'investments', 'payees', 'securities'
        ],
        sample_rows_in_table_info=30
    )
