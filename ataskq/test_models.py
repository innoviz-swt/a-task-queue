import pytest

from typing import Dict

from .models import Model, Job, Task, Object
from .handler import Handler, from_config, register_handler, unregister_handler

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
        "attr": {"name": "obj-name", "job_id": lambda: Job.create()},
    },
]
TEST_IDS = [m["klass"].table_key() for m in TEST_DATA]


@pytest.fixture(scope="function")
def handler(config):
    handler = from_config(config)
    register_handler("test_handler", handler)
    yield handler
    unregister_handler("test_handler")


@pytest.fixture
def jhandler(handler) -> Handler:
    return handler.create_job()


def apply_lamdas(test_attr):
    for k, v in test_attr.items():
        if callable(v):
            test_attr[k] = v()

    return test_attr


@pytest.mark.parametrize("test_data", TEST_DATA, ids=TEST_IDS)
def test_create(handler, test_data):
    model_cls: Model = test_data["klass"]
    attr = apply_lamdas(test_data["attr"])
    m = model_cls(**attr).screate()

    count = len(model_cls.get_all())
    assert count == 1

    for k, v in attr.items():
        assert getattr(m, k) == v, f"{m}.{k}: key value mismatch"


# def assert_model(m_src, m_rec, model_cls, first_id, i):
#     assert isinstance(m_rec, model_cls), f"index: '{i}'"
#     assert m_src.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
#     assert m_src.__dict__ == m_rec.__dict__, f"index: '{i}'"
#     assert getattr(m_src, m_src.id_key()) == i + first_id, f"index: '{i}'"
#     assert getattr(m_rec, m_rec.id_key()) == i + first_id, f"index: '{i}'"


# @pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=TEST_IDS)
# def test_get_all(handler, model_cls):
#     m1 = create(model_cls, name="test 1")
#     m2 = create(model_cls, name="test 2")
#     m3 = create(model_cls, name="test 3")
#     data = [m1, m2, m3]
#     m_all = model_cls.get_all()
#     assert len(m_all) == 3

#     first_id = getattr(m1, m1.id_key())
#     for i in range(len(data)):
#         m_src = data[i]
#         m_rec = m_all[i]
#         assert_model(m_src, m_rec, model_cls, first_id, i)


# @pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=TEST_IDS)
# def test_get_all_where(handler, model_cls: Model):
#     m1 = create(model_cls, name="test 1")
#     m2 = create(model_cls, name="test 2")
#     m3 = create(model_cls, name="test 3")

#     m_all = model_cls.get_all(_where="name='test 1'")
#     assert len(m_all) == 1

#     m_rec = m_all[0]
#     first_id = getattr(m1, m1.id_key())
#     assert_model(m1, m_rec, model_cls, first_id, 0)


# @pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=TEST_IDS)
# def test_get(handler, model_cls):
#     m1 = create(model_cls, name="test 1")
#     m2 = create(model_cls, name="test 2")
#     m3 = create(model_cls, name="test 3")
#     data = [m1, m2, m3]

#     first_id = getattr(m1, m1.id_key())
#     m_all = [model_cls.get(i + first_id) for i in range(len(data))]
#     for i in range(len(data)):
#         m = data[i]
#         m_rec = m_all[i]
#         assert isinstance(m_rec, model_cls), f"index: '{i}'"
#         assert m.name == m_rec.name == f"test {i+1}", f"index: '{i}'"
#         assert m.__dict__ == m_rec.__dict__, f"index: '{i}'"
#         assert getattr(m, m.id_key()) == i + first_id, f"index: '{i}'"
#         assert getattr(m_rec, m_rec.id_key()) == i + first_id, f"index: '{i}'"


# @pytest.mark.parametrize("model_cls", __MODELS__.values(), ids=TEST_IDS)
# def test_delete(handler, model_cls):
#     m = create(model_cls)

#     count = len(model_cls.get_all())
#     assert count == 1

#     m.delete()

#     count = len(model_cls.get_all())
#     assert count == 0
