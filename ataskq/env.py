import os

ATASKQ_CONFIG = os.getenv("ATASKQ_CONFIG")
# for dev purposes add additional config for server defaulted to normal config
ATASKQ_SERVER_CONFIG = os.getenv("ATASKQ_SERVER_CONFIG", ATASKQ_CONFIG)
