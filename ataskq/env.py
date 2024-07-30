import os

CONFIG = os.getenv("ATASKQ_CONFIG")
# for dev purposes add additional config for server defaulted to normal config
SERVER_CONFIG = os.getenv("ATASKQ_SERVER_CONFIG", CONFIG)
SERVER_LIFESPAN = bool(int(os.getenv("ATASKQ_SERVER_LIFESPAN", False)))
