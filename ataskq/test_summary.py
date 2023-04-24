from pathlib import Path

from ataskq.task_runner import TaskRunner, Task, targs


def test_table(tmp_path):
    # very general sanity test
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    table = runner.html_table().split('\n')
    assert '<table>' in table[0]
    assert '</table>' in table[-1]


def test_html(tmp_path: Path):
    # very general sanity test
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    html = runner.html()
    assert '<body>' in html
    assert '</body>' in html
    
    html = html.split('\n')
    assert '<html>' in html[0]
    assert '</html>' in html[-1]


def test_html_file_str_dump(tmp_path: Path):
    # very general sanity test
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    file=tmp_path / 'test.html'
    html = runner.html(file=file)
    
    assert file.exists()
    assert html == file.read_text()
    

def test_html_file_io_dump(tmp_path: Path):
    # very general sanity test
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    file=tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = runner.html(file=f)
        
    assert file.exists()
    assert html == file.read_text()