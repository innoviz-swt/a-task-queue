import pytest

from .models import Model, __MODELS__, Job
from .handler import Handler, from_config, register_handler, unregister_handler


@pytest.fixture(scope="function")
def handler(config):
    handler = from_config(config)
    register_handler("test_handler", handler)
    yield handler
    unregister_handler("test_handler")


@pytest.fixture
def jhandler(handler) -> Handler:
    return handler.create_job()


def init(model_cls, **kwargs):
    # todo: better handle not Null fields (take from schema in future)
    annotations = model_cls.__annotations__.keys()
    if "entrypoint" in annotations and "entrypoint" not in kwargs:
        kwargs["entrypoint"] = "dummy entry point"
    if "job_id" in annotations and "job_id" not in kwargs and not issubclass(model_cls, Job):
        job = create(Job)
        kwargs["job_id"] = job.job_id

    ret = model_cls(**kwargs)

    return ret


def create(model_cls, **kwargs):
    m = init(model_cls, **kwargs)
    m = m.create()

    return m


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_create(handler, model_cls):
    m = create(model_cls, name="test name")

    count = len(model_cls.get_all())
    assert count == 1

    assert m.name == "test name"


def assert_model(m_src, m_rec, model_cls, first_id, i):
    assert isinstance(m_rec, model_cls), f"index: '{i}'"
    assert m_src.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
    assert m_src.__dict__ == m_rec.__dict__, f"index: '{i}'"
    assert getattr(m_src, m_src.id_key()) == i + first_id, f"index: '{i}'"
    assert getattr(m_rec, m_rec.id_key()) == i + first_id, f"index: '{i}'"


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_get_all(handler, model_cls):
    m1 = create(model_cls, name="test 1")
    m2 = create(model_cls, name="test 2")
    m3 = create(model_cls, name="test 3")
    data = [m1, m2, m3]
    m_all = model_cls.get_all()
    assert len(m_all) == 3

    first_id = getattr(m1, m1.id_key())
    for i in range(len(data)):
        m_src = data[i]
        m_rec = m_all[i]
        assert_model(m_src, m_rec, model_cls, first_id, i)


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_get_all_where(handler, model_cls: Model):
    m1 = create(model_cls, name="test 1")
    m2 = create(model_cls, name="test 2")
    m3 = create(model_cls, name="test 3")

    m_all = model_cls.get_all(_where="name='test 1'")
    assert len(m_all) == 1

    m_rec = m_all[0]
    first_id = getattr(m1, m1.id_key())
    assert_model(m1, m_rec, model_cls, first_id, 0)


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_get(handler, model_cls):
    m1 = create(model_cls, name="test 1")
    m2 = create(model_cls, name="test 2")
    m3 = create(model_cls, name="test 3")
    data = [m1, m2, m3]

    first_id = getattr(m1, m1.id_key())
    m_all = [model_cls.get(i + first_id) for i in range(len(data))]
    for i in range(len(data)):
        m = data[i]
        m_rec = m_all[i]
        assert isinstance(m_rec, model_cls), f"index: '{i}'"
        assert m.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
        assert m.__dict__ == m_rec.__dict__, f"index: '{i}'"
        assert getattr(m, m.id_key()) == i + first_id, f"index: '{i}'"
        assert getattr(m_rec, m_rec.id_key()) == i + first_id, f"index: '{i}'"


@pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=__MODELS__.keys())
def test_delete(handler, model_cls):
    m = create(model_cls)

    count = len(model_cls.get_all())
    assert count == 1

    m.delete()

    count = len(model_cls.get_all())
    assert count == 0


MODEL_CHILD = [
    (model_cls, child_cls, parent_key)
    for model_cls in __MODELS__.values()
    for child_cls, parent_key in model_cls.children().items()
]


@pytest.mark.parametrize("model_cls, child_cls, parent_key", MODEL_CHILD)
def test_add_get_children(handler, model_cls: Model, child_cls, parent_key):
    m1 = create(model_cls)
    children = [init(child_cls, **{parent_key: None}) for i in range(3)]
    m1.add_children(child_cls, children)

    m2 = create(model_cls)
    children = [init(child_cls, **{parent_key: None}) for i in range(4)]
    m2.add_children(child_cls, children)

    assert getattr(m1, model_cls.id_key()) != getattr(m2, model_cls.id_key())

    rec_children = m1.get_children(child_cls)
    assert len(rec_children) == 3
    assert all([getattr(c, parent_key) == getattr(m1, model_cls.id_key()) for c in rec_children])

    rec_children = m2.get_children(child_cls)
    assert len(rec_children) == 4
    assert all([getattr(c, parent_key) == getattr(m2, model_cls.id_key()) for c in rec_children])
