#! /bin/bash
set -e
SCRIPT_DIR=$( dirname $(readlink -f $0) )
cd $SCRIPT_DIR/..

pip install -r requirements.txt
pre-commit install

echo Done setup