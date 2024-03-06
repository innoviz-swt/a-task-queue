from multiprocessing import Lock


def write_to_file(filepath, text):
    with open(filepath, "a") as f:
        f.write(text)


__lock__ = Lock()


def write_to_file_mp_lock(filepath, text):
    with __lock__:
        with open(filepath, "a") as f:
            f.write(text)
