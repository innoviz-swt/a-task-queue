#!/bin/bash
set -e
cp -r $(dirname $0) $(dirname $0)/../contrib
cp $(dirname $0)/../.env $(dirname $0)/../contrib
