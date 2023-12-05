from abc import abstractmethod
from .logger import Logger


class Handler(Logger):
    def __init__(self, job_id=None, logger: Logger = None):
        super().__init__(logger)

        self._job_id = job_id

    @property
    def job_id(self):
        return self._job_id

    @abstractmethod
    def create_job(self, c, name='', description=''):
        pass


def from_connection_str(conn=None, **kwargs) -> Handler:
    if conn is None:
        conn = ''

    sep = '://'
    sep_index = conn.find(sep)
    if sep_index == -1:
        raise RuntimeError(f'connection must be of format <db type>://<connection string>')
    handler_type = conn[:sep_index]

    # validate connectino
    if not handler_type:
        raise RuntimeError(f'missing db type, connection must be of format <db type>://<connection string>')

    connection_str = conn[sep_index + len(sep):]
    if not connection_str:
        raise RuntimeError(f'missing connection string, connection must be of format <db type>://<connection string>')

    # get db type handler
    if handler_type == 'sqlite':
        from .db_handler.sqlite3 import SQLite3DBHandler
        handler = SQLite3DBHandler(conn, **kwargs)
    elif handler_type == 'postgresql':
        from .db_handler.postgresql import PostgresqlDBHandler
        handler = PostgresqlDBHandler(conn, **kwargs)
    elif handler_type == 'http' or handler_type == 'https':
        from .rest_handler import RESTHandler
        handler = RESTHandler(conn, **kwargs)
    else:
        raise Exception(f"unsupported db type '{handler_type}', db type must be one of ['sqlite', 'postgresql']")

    return handler
