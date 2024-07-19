import os

CONFIG = os.getenv("ATASKQ_CONFIG")
DEFAULT_HANDLER = os.getenv("sqlite")
# for dev purposes add additional config for server defaulted to normal config
SERVER_CONFIG = os.getenv("ATASKQ_SERVER_CONFIG", CONFIG)
