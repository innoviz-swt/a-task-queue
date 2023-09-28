from pathlib import Path
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    if Path("./sql/test.db.sqlite3").exists():
        Path("./sql/test.db.sqlite3").unlink()        
    create_connection(r"./sql/test.db.sqlite3")
