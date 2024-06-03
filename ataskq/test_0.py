import pytest


@pytest.mark.parametrize("conn_type", ["sqlite", "pg", "http"])
def test_conn_type_check(conn_type, config):
    conn = config["connection"]
    if f"{conn_type}://" not in conn:
        pytest.skip()
