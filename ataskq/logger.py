import logging

class Logger:
    def __init__(self, logger: logging.Logger or None):
        self._logger = logger or logging.getLogger('ataskq')
    
    def exception(self, *args, **kwwargs):
        self._logger.exception(*args, **kwwargs)

    def critical(self, *args, **kwwargs):
        self._logger.critical(*args, **kwwargs)

    def error(self, *args, **kwwargs):
        self._logger.error(*args, **kwwargs)

    def warning(self, *args, **kwwargs):
        self._logger.warning(*args, **kwwargs)

    def warn(self, *args, **kwwargs):
        self._logger.warn(*args, **kwwargs)

    def info(self, *args, **kwwargs):
        self._logger.info(*args, **kwwargs)

    def debug(self, *args, **kwwargs):
        self._logger.debug(*args, **kwwargs)