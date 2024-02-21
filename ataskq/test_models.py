import pytest

from .models import __MODELS__
from .handler import Handler, from_connection_str
from .register import register_ihandlers


@pytest.fixture
def handler(conn) -> Handler:
    handler = from_connection_str(conn)
    register_ihandlers('test_handler', handler)


@pytest.fixture
def jhandler(handler) -> Handler:
    return handler.create_job()


def create(model_cls, **kwargs):
    # todo: better handle not Null fields (take from schema in future)
    annotations = model_cls.__annotations__.keys()
    if 'entrypoint' in annotations and 'entrypoint' not in kwargs:
        kwargs['entrypoint'] = 'dummy entry point'
    if 'job_id' in annotations and 'job_id' not in kwargs:
        kwargs['job_id'] = 0

    # test that deleting a job deletes all its tasks
    m = model_cls(**kwargs).create()

    return m


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_create(handler, model_cls):
    m = create(model_cls, name="test name")

    count = len(model_cls.get_all())
    assert count == 1

    assert m.name == "test name"


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_get_all(handler, model_cls):
    m1 = create(model_cls, name="test 1")
    m2 = create(model_cls, name="test 2")
    m3 = create(model_cls, name="test 3")
    data = [m1, m2, m3]
    m_all = model_cls.get_all()
    assert len(m_all) == 3

    for i in range(len(data)):
        m = data[i]
        m_rec = m_all[i]
        assert isinstance(m_rec, model_cls), f"index: '{i}'"
        assert m.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
        assert m.__dict__ == m_rec.__dict__, f"index: '{i}'"
        assert getattr(m, m.id_key()) == i + 1, f"index: '{i}'"
        assert getattr(m_rec, m_rec.id_key()) == i + 1, f"index: '{i}'"


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_get(handler, model_cls):
    m1 = create(model_cls, name="test 1")
    m2 = create(model_cls, name="test 2")
    m3 = create(model_cls, name="test 3")
    data = [m1, m2, m3]
    m_all = [model_cls.get(i + 1) for i in range(len(data))]

    for i in range(len(data)):
        m = data[i]
        m_rec = m_all[i]
        assert isinstance(m_rec, model_cls), f"index: '{i}'"
        assert m.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
        assert m.__dict__ == m_rec.__dict__, f"index: '{i}'"
        assert getattr(m, m.id_key()) == i + 1, f"index: '{i}'"
        assert getattr(m_rec, m_rec.id_key()) == i + 1, f"index: '{i}'"


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_delete(handler, model_cls):
    m = create(model_cls)

    count = len(model_cls.get_all())
    assert count == 1

    m.delete()

    count = len(model_cls.get_all())
    assert count == 0
