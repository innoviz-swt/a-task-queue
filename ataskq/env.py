import os

ATASKQ_CONNECTION = os.getenv('ATASKQ_CONNECTION', "sqlite://ataskq.db.sqlite3")
ATASKQ_SERVER_CONNECTION = os.getenv('ATASKQ_SERVER_CONNECTION', "sqlite://ataskq.db.sqlite3")
