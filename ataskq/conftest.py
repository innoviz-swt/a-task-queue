from pytest import fixture
import os
import pytest

if os.getenv("_PYTEST_RAISE", "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value


def truncate_query(table_name):
    return f"""DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') THEN
      TRUNCATE TABLE {table_name} CASCADE;
   END IF;
END $$;
"""


def drop_pg_tables(conn):
    assert "pg" in conn
    import psycopg2
    from ataskq.handler.postgresql import from_connection_str

    connection = from_connection_str(conn)
    db_conn = psycopg2.connect(
        host=connection.host, database=connection.database, user=connection.user, password=connection.password
    )
    c = db_conn.cursor()
    c.execute(truncate_query("tasks"))
    c.execute(truncate_query("state_kwargs"))
    c.execute(truncate_query("jobs"))
    # c.execute('DROP TABLE IF EXISTS tasks;')
    # c.execute('DROP TABLE IF EXISTS state_kwargs;')
    # c.execute('DROP TABLE IF EXISTS jobs;')
    db_conn.commit()
    db_conn.close()


@fixture
def conn(tmp_path):
    conn = os.getenv("ATASKQ_CONNECTION", "sqlite://{tmp_path}/ataskq.db.sqlite3")
    if "sqlite" in conn:
        conn = conn.format(tmp_path=tmp_path)
    elif "pg" in conn:
        # connect and clear all db tables
        drop_pg_tables(conn)
    elif "http" in conn:
        server_conn = os.getenv("ATASKQ_SERVER_CONNECTION", "pg://postgres:postgres@localhost/postgres")
        assert "pg" in server_conn, "rest api test must be with posgres server"
        drop_pg_tables(server_conn)
    else:
        raise Exception(f"Unkown connection format '{conn}'")

    return conn
