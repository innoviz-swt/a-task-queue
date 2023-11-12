from pytest import fixture
import os


@fixture
def conn(tmp_path):
    conn = os.getenv('ATASKQ_TEST_CONNECTION', 'sqlite://{tmp_path}/ataskq.db')
    # conn = 'postgresql://postgres:cvalgo.devops@localhost:5432/ataskq'
    if 'sqlite' in conn:
        conn = conn.format(tmp_path=tmp_path)
    elif 'postgresql' in conn:
        # connect and clear all db tables
        import psycopg2
        from ataskq.db_handler.postgresql import from_connection_str
        c = from_connection_str(conn)
        ps_conn = psycopg2.connect(
            host=c.host,
            database=c.database,
            user=c.user,
            password=c.password)
        c = ps_conn.cursor()
        c.execute('DROP TABLE IF EXISTS schema_version')
        c.execute('DROP TABLE IF EXISTS tasks')
        c.execute('DROP TABLE IF EXISTS state_kwargs')
        c.execute('DROP TABLE IF EXISTS jobs')
        ps_conn.commit()
        ps_conn.close()

    return conn
