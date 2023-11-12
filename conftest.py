from pytest import fixture

@fixture
def conn(tmp_path):
    return f'sqlite://{tmp_path}/ataskq.db'