#!/usr/bin/env python
# pre requisites: 
# ```
# sudo apt-get install -y libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev
# ````
from setuptools import setup
from ataskq import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='ataskq',
    version=__version__,
    description='An in process task queue for distributed computing systems.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Mark Kolodkin',
    author_email='markk@innoviz-tech.com',
    url='https://github.com/innoviz-swt/a-task-queue/',
    packages=['ataskq'],
    package_data={
        'ataskq': ['templates/*'],
    },
    keywords=['python', 'task', 'queue', 'distributed systems', 'distributed computing'],
)