import logging


def init_logger(level=logging.INFO):
    logger = logging.getLogger('ataskq')

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger
