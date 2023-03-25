import pytest
import tempfile
import shutil
from pathlib import Path

from ataskq.task_runner import TaskRunner

@pytest.fixture(scope="session")
def temp_folder(request):
    folder_path = tempfile.mkdtemp(dir=Path(__file__).parent.parent / 'tmp')

    def remove_folder():
        shutil.rmtree(folder_path)

    request.addfinalizer(remove_folder)

    return Path(folder_path)


def test_create_job(temp_folder: Path):
    runner = TaskRunner(job_path=temp_folder).create_job(overwrite=True)
    assert isinstance(runner, TaskRunner)

    assert temp_folder.exists()
    assert (temp_folder / '.ataskqjob').exists()

    assert (temp_folder / 'tasks.db').exists()
    assert (temp_folder / 'tasks.db').is_file()

    assert (temp_folder / 'keyvalue.db').exists()
    assert (temp_folder / 'keyvalue.db').is_dir()


def test_run_task_by_level(temp_folder):
    assert True
