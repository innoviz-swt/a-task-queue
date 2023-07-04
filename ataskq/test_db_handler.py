from pathlib import Path

from .db_handler import DBHandler, EQueryType


def test_table(tmp_path):
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    table = db_handler.html_table().split('\n')
    assert '<table>' in table[0]
    assert '</table>' in table[-1]


def test_html(tmp_path: Path):
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS)
    assert '<body>' in html
    assert '</body>' in html
    
    html = html.split('\n')
    assert '<html>' in html[0]
    assert '</html>' in html[-1]


def test_html_file_str_dump(tmp_path: Path):
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    file=tmp_path / 'test.html'
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=file)
    
    assert file.exists()
    assert html == file.read_text()
    

def test_html_file_io_dump(tmp_path: Path):
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    file=tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=f)
        
    assert file.exists()
    assert html == file.read_text()