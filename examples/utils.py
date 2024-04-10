def get_logger():
    # init logger
    log_level = logging.INFO
    logger = logging.getLogger("ataskq")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    logger.addHandler(handler)
    handler.setLevel(log_level)
    logger.setLevel(log_level)

    return logger
