from multiprocessing import Lock
import os
import time
from datetime import datetime


def write_to_file(filepath, text, sleep=None):
    if sleep is not None:
        time.sleep(sleep)
    text = text.replace(r"@{pid}", f"{os.getpid()}").replace(r"@{now}", f"{datetime.now()}")
    with open(filepath, "a") as f:
        f.write(text)


__lock__ = Lock()


def write_to_file_mp_lock(filepath, text, sleep=None):
    if sleep is not None:
        time.sleep(sleep)
    text = text.replace(r"@{pid}", f"{os.getpid()}").replace(r"@{now}", f"{datetime.now()}")
    with __lock__:
        with open(filepath, "a") as f:
            f.write(text)
