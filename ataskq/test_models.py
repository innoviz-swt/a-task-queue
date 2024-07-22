import pytest
from copy import copy

from .model import EState
from .models import Model, Job, Task, Object
from .handler import Handler, from_config, register_handler, unregister_handler


def create_job_id(h: Handler):
    job = Job()
    h.add(job)
    return job.job_id


TEST_DATA = [
    {
        "klass": Object,
        "attr": {"serializer": "ser"},
    },
    {
        "klass": Job,
        "attr": {"name": "obj-name"},
    },
    {
        "klass": Task,
        "attr": {"name": "obj-name", "job_id": create_job_id},
    },
]
TEST_IDS = [m["klass"].table_key() for m in TEST_DATA]


@pytest.fixture(scope="function")
def handler(config):
    handler = from_config(config)
    register_handler("test_handler", handler)
    yield handler
    unregister_handler("test_handler")


def apply_lamdas(h: Handler, test_attr):
    ret = copy(test_attr)
    for k, v in test_attr.items():
        if callable(v):
            ret[k] = v(h)

    return ret


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_create(handler, test_data):
    model_cls: Type[Model] = test_data["klass"]
    attr = apply_lamdas(handler, test_data["attr"])
    m = model_cls(**attr)
    assert m._state.value == EState.New
    handler.add(m)
    assert m._state.value == EState.Modified

    count = len(handler.get_all(model_cls))
    assert count == 1

    for k, v in attr.items():
        assert getattr(m, k) == v, f"{m}.{k}: key value mismatch"


def assert_model(m_src: Model, m_rec: Model, model_cls, first_id, i):
    assert id(m_src) != id(m_rec), f"index: '{i}'"
    assert isinstance(m_rec, model_cls), f"index: '{i}'"
    assert m_src.to_dict() == m_rec.to_dict(), f"index: '{i}'"
    assert getattr(m_src, m_src.id_key()) == i + first_id, f"index: '{i}'"
    assert getattr(m_rec, m_rec.id_key()) == i + first_id, f"index: '{i}'"


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_get_all(handler, test_data):
    model_cls: Type[Model] = test_data["klass"]
    attr = apply_lamdas(handler, test_data["attr"])
    k = list(test_data["attr"].keys())[0]
    m1 = model_cls(**{**attr, k: "test 1"})
    m2 = model_cls(**{**attr, k: "test 2"})
    m3 = model_cls(**{**attr, k: "test 3"})

    data = [m1, m2, m3]
    handler.add(data)

    m_all = handler.get_all(model_cls)
    assert len(m_all) == 3

    first_id = getattr(m1, m1.id_key())
    for i in range(len(data)):
        m_src = data[i]
        m_rec = m_all[i]
        assert_model(m_src, m_rec, model_cls, first_id, i)
        assert m_rec._state.value == EState.Fetched


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_get_all_where(handler, test_data):
    model_cls: Type[Model] = test_data["klass"]
    attr = apply_lamdas(handler, test_data["attr"])
    k = list(test_data["attr"].keys())[0]
    m1 = model_cls(**{**attr, k: "test 1"})
    m2 = model_cls(**{**attr, k: "test 2"})
    m3 = model_cls(**{**attr, k: "test 3"})
    data = [m1, m2, m3]
    handler.add(data)

    m_all = handler.get_all(model_cls, where=f"{k}='test 1'")
    assert len(m_all) == 1

    m_rec = m_all[0]
    first_id = getattr(m1, m1.id_key())
    assert_model(m1, m_rec, model_cls, first_id, 0)
    assert m_rec._state.value == EState.Fetched


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_get(handler, test_data):
    model_cls: Type[Model] = test_data["klass"]
    attr = apply_lamdas(handler, test_data["attr"])
    k = list(test_data["attr"].keys())[0]
    m1 = model_cls(**{**attr, k: "test 1"})
    m2 = model_cls(**{**attr, k: "test 2"})
    m3 = model_cls(**{**attr, k: "test 3"})
    data = [m1, m2, m3]
    handler.add(data)

    first_id = getattr(m1, m1.id_key())
    m_all = [handler.get(model_cls, i + first_id) for i in range(len(data))]
    for i in range(len(data)):
        m_src = data[i]
        m_rec = m_all[i]
        assert_model(m_src, m_rec, model_cls, first_id, i)
        assert m_rec._state.value == EState.Fetched


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_delete(handler, test_data):
    model_cls: Type[Model] = test_data["klass"]
    attr = apply_lamdas(handler, test_data["attr"])
    model = model_cls(**attr)
    handler.add(model)

    count = len(handler.get_all(model_cls))
    assert count == 1

    handler.delete(model)
    assert model._state.value == EState.Deleted

    count = len(handler.get_all(model_cls))
    assert count == 0


def test_job_delete_cascade(config):
    # test that deleting a job deletes all its tasks
    handler = from_config(config)
    job1 = Job(
        name="job1",
        tasks=[
            Task(entrypoint=""),
            Task(entrypoint=""),
            Task(entrypoint=""),
        ],
    )
    job2 = Job(
        name="job2",
        tasks=[
            Task(entrypoint=""),
            Task(entrypoint=""),
        ],
    )
    handler.add([job1, job2])
    assert handler.count_all(Job) == 2
    assert handler.count_all(Task) == 5

    handler.delete(job2)
    assert handler.count_all(Job) == 1
    assert handler.count_all(Task) == 3

    handler.delete(job1)
    assert handler.count_all(Job) == 0
    assert handler.count_all(Task) == 0
