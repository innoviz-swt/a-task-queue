#!/usr/bin/env python
# pre requisites: 
# ```
# sudo apt-get install -y libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev
# ````
from setuptools import setup
from ataskq import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

__version__ = '1.0'

setup(
    name='ataskq',
    version='1.0',
    description='An in process task queue for distributed computing systems.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Mark Kolodkin',
    author_email='markk@innoviz-tech.com',
    url='https://github.com/innoviz-swt/a-task-queue/',
    packages=['ataskq'],
    keywords=['python', 'task', 'queue', 'distributed systems', 'distributed computing'],
    install_requires=[
        'plyvel>=1.5.0',
    ],
)