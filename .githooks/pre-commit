#!/bin/bash
version=$(python -c "from ataskq import __schema_version__; print(__schema_version__, end='')")
echo "dump version 'schema/schema_v${version}.sql'"
sqlite3 ataskq.db.sqlite3 ".schema --indent" > schema/schema_v${version}.sql
