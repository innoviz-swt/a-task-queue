from pathlib import Path

import context
from ataskq.handler.db_handler import set_connection_log

if __name__ == "__main__":
    db_path = "./sqlite.local.db"
    Path(db_path).unlink()

    # def myprint(*args, **kwargs):
    #     print(*args, **kwargs, end=";\n")

    # set_connection_log(myprint)

    t(f"sqlite://{db_path}")
